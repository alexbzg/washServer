#!/usr/bin/python
#coding=utf-8
from twisted.internet import reactor, protocol, task
from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from twisted.conch.telnet import StatefulTelnetProtocol
from twisted.python import log
from twisted.spread import pb
from twisted.web import static, server
import sys, decimal, re, datetime, os, logging
#log.startLogging(sys.stdout)
from twisted.conch.telnet import TelnetTransport, StatefulTelnetProtocol

from lxml import etree
from collections import deque

from common import appRoot, readConf
from washDB import db, cursor2dicts
import simplejson as json
from datetime import date, timedelta

args = {}
args['t'] = '-t' in sys.argv
postfix = '_t' if args['t'] else ''
conf = readConf( 'site' + postfix + '.conf' )
pidFile = open( appRoot + '/washServer' + postfix + '.pid', 'w' )
pidFile.write( str( os.getpid() ) )
pidFile.close()
observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig( level = logging.DEBUG if args['t'] else logging.ERROR,
        format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
log.msg( 'starting in test mode' )

locations, devices, controllers, prices, operations, \
    clientConnections, clientButtons, operators = \
    [], {}, {}, {}, {}, {}, {}, []


def formatDT( date, format ):
    return date.strftime( format ) if date else None

def epoch( date ):
    if date:
        td = date - datetime.datetime(1970,1,1)
        return td.seconds + td.days * 24 * 3600
    else:
        return None


def jsonEncodeExtra( obj ):
    if isinstance( obj, decimal.Decimal ):
        return str( obj )
    if isinstance( obj, datetime.datetime ):
        return obj.isoformat()
    if hasattr( obj, '__dict__' ):
        return None
    log.err( repr( obj ) + " is not JSON serializable" )
    return None


class Car:
    reLP = re.compile( '^(.{5,}\D)(\d+)$', re.U )

    @classmethod
    def fromQuery( cls, query ):
        queryMatch = Car.reLP.match( query )
        ln, region = '', ''
        if queryMatch:
            ln = queryMatch.group( 1 )
            region = queryMatch.group( 2 )
        else:
            ln = query
            region = None
        return cls( { 'license_no': ln, 'region': region } )

    def __init__( self, params ):
        dbData = db.getObject( 'cars', params )
        self.id = dbData[ 'id' ]
        self.licenseNo = dbData[ 'license_no' ]
        self.region = dbData[ 'region' ]
        self.balance = 0
        self.notpayed = None
        self.prev = 0
       
        if dbData[ 'client_id' ]:
            self.client = getWashClient( dbData[ 'client_id' ] )
        else:
            self.client = None
            balance_sql = "select sum( pay_sum ) - sum( total ) \
                    as balance,\
                    sum( 1 ) as  count \
                from operations \
                where car_id = %s and pay is not null \
                    and client_id is null \
                and not service_mode"
            balance = db.getValue( balance_sql, 
                ( self.id, ) )
            if balance:
                self.balance = balance
            if self.balance < -50:
                notpayed_sql = """
                    select id, total,
                    to_char( tstamp_start, 'DD.MM.YYYY HH24:MI' ) 
                    as start
                    from operations where car_id = %s 
                    and total - pay_sum > 50 and not service_mode 
                    and pay is not null;
                """
                self.notpayed = cursor2dicts( 
                    db.execute( notpayed_sql, ( self.id, ) ),
                    True )
                if not self.notpayed:
                    notpayed = None
            else:
                self.notpayed = None

    def toDict( self ):
        return { 'id': self.id,
                'licenseNo': self.licenseNo,
                'region': self.region,
                'balance': self.balance,
                'notpayed': self.notpayed,
                'client': self.client }
   

       

class PbConnection( pb.Referenceable ):
    def remote_data( self, data ):

        d = json.loads( data )

        if d.has_key( 'signal' ):
            if d[ 'signal'][ 'type' ] == 'stop':
                devices[ d[ 'signal'][ 'device' ] 
                        ].stopping = True
            devices[ d[ 'signal'][ 'device' ] ].signal( \
                    d[ 'signal'][ 'type' ] )

        elif d.has_key( 'carQuery' ):
            self.update( { 'cars': \
                    Car.fromQuery( d[ 'carQuery' ] ).toDict(), 
                    'create': 1 } )

        elif d.has_key( 'setCarData' ):
            cd = d[ 'setCarData' ]
            opr = operations[ cd[ 'operation' ] ]
            if cd[ 'car' ]:
                opr.setCar( Car( cd[ 'car' ] ) )
            elif cd[ 'serviceMode' ]:
                opr.setServiceMode()
            elif cd[ 'noLP' ]:
                opr.setNoLP()
            if cd[ 'client' ]:
                opr.setClient( cd[ 'client' ][ 'id' ] )

        elif d.has_key( 'autoOperation' ):
            od = d[ 'autoOperation']
            db.getObject( 'operations', 
                { 'location_id': self.locationId,
                'device_id': od['device'],
                'car_id': od['carData']['car']['id'] \
                    if od['carData']['car'] else None,
                'client_id': od['carData']['car']['client']['id'] \
                    if od['carData']['car'] and \
                    od['carData']['car']['client'] else None,
                'service_mode': od['carData']['serviceMode'],
                'no_lp': od['carData']['noLP'],
                'total': od['total'],
                'pay_sum': od['pay'] }, 
                True )


        elif d.has_key( 'closeOperation' ):
            op = operations[ d['closeOperation']['id'] ]
            total = op.getTotal()
            op.close( total, 
                total if d['closeOperation']['pay'] else 0 )
            if ( d['closeOperation']['pay'] and op.car and \
                    op.car.notpayed ):
                for np in op.car.notpayed.values():
                    db.updateObject( 'operations',
                        { 'id': np['id'], 'pay_sum': np['total'] } )

        elif d.has_key( 'getLPHints' ):
            self.sendLPHints( d[ 'getLPHints' ][ 'pattern' ] )

        elif d.has_key( 'getShiftData' ):
            self.sendShiftData()

        elif d.has_key( 'newShift' ):
            db.getObject( 'shifts', 
                { 'location_id': self.locationId,
                    'operator_id': d['newShift']['operator'] },
                True )
            self.sendShiftData()

        elif d.has_key( 'setDetectsPresence' ):
            devices[ d[ 'setDetectsPresence'][ 'device' ] 
                ].setDetectsPresence(
                    d[ 'setDetectsPresence' ][ 'value' ] )

        elif d.has_key( 'linkOperation' ):
            operations[ d['linkOperation']['id'] ].setLink(
                d['linkOperation']['value'] )


    def sendShiftData( self ):
        sql = """
            select tstamp_start, operator_id, 
                totals[1] as qty, totals[2] as total, 
                totals[2] - totals[3] as notpayed
            from
            ( select tstamp_start, operator_id, 
                ( select ARRAY[ sum( 1 ),                               
                        sum( round( total, 0 ) ),                  
                        sum( pay_sum )]
                    from operations                                 
                    where location_id = %(location)s
                      and not service_mode          
                      and client_id is null                             
                      and pay > shifts.tstamp_start ) as totals
              from shifts
              where location_id = %(location)s and tstamp_end is null   
              order by tstamp_start desc                       
              limit 1 ) as s
            """
        self.update( { 'shiftData': \
            cursor2dicts( db.execute( sql, 
                { 'location': self.locationId } ),
                False ) } )


    def sendLPHints( self, pattern ):
        sql = """select license_no || region as lp from cars 
                    where license_no ~ %s 
                    order by 
                        ( select sum(1) from operations 
                        where car_id = cars.id and 
                        pay > now() - interval '2 months' )
                    limit 3"""
        self.update( { 'lpHints' : \
            cursor2dicts( db.execute( sql, (pattern, ) ) ) } )


    def update( self, params ):
        try:
            self.clientSide.callRemote( "update", 
                json.dumps( params, default = jsonEncodeExtra ) )
        except (pb.PBConnectionLost, pb.DeadReferenceError), e:
            log.msg( type( e ).__name__ )
            log.err( "the client disconnected or crashed" )
            clientConnections[ self.locationId ].remove( self )
            return
        if args['t']:
            f = open( conf.get( 'common', 'siteRoot' ) + \
                '/debug/' + str( self.count ) + \
                '.json', 'w' )
            f.write( json.dumps( params, default = jsonEncodeExtra ) )
            f.close()
            self.count += 1


class PbServer( pb.Root ):
    def remote_connect( self, clientSide, locationId ):
        pbc = PbConnection()
        pbc.clientSide = clientSide
        pbc.locationId = locationId
        pbc.count = 0
        if not clientConnections.has_key( locationId ):
            clientConnections[ locationId ] = []
        clientConnections[ locationId ].append( pbc )
        pbc.update( 
            { 'devices': 
                dict( ( id, devices[ id ].toDict() ) 
                for id in devices.keys() 
                if devices[ id ].locationId == locationId ),
            'operations': 
                dict( ( id, operations[ id ].toDict() ) 
                for id in operations.keys()
                if operations[ id ].locationId == locationId ),
            'clientButtons':
                clientButtons[ locationId ] \
                if clientButtons.has_key( locationId ) else None,
            'operators': operators,
            'create': True } )
        return pbc


class Device:

    class Event:

        def __init__( self, controller, node ):
            self.node = node
            self.controller = controller
            if node.get( 'type' ) == 'signal':
                line = int( node.get( 'line' ) )
                self.stateMatch = ( node.get( 'signal' ) == '1' )
                self.timer = None
                controller.setCallback( line, self.trigger )
                controller.setLineMode( line, node.get( 'mode' ) )
                for signalAction in self.node.xpath( 
                        'action[ @type = "signal" ]' ):
                    self.controller.setLineMode( 
                            int( signalAction.get( 'line' ) ), 'out' )

        def cancelTimer( self ):
            if self.timer:
                self.timer.cancel()
                self.timer = None

        def triggerAction( self ):
            log.msg( 'trigger on line ' + self.node.get( 'line' ) + \
                    ' signal ' + self.node.get( 'signal' ) + \
                    ' continues ' + str( self.node.get( 'continues' ) ) )
            for signalAction in self.node.xpath( 
                    'action[ @type = "signal" ]' ):
                self.controller.setLineState( 
                        int( signalAction.get( 'line' ) ),
                        ( signalAction.get( 'signal' ) == '1' ) )
            self.timer = None

        def trigger( self, state ):
            if state == self.stateMatch:
                if self.node.get( 'continues' ):
                    self.timer = reactor.callLater(
                            int( self.node.get( 'continues' ) ),
                            self.triggerAction )
                else:
                    self.triggerAction()
            else:
                self.cancelTimer()


    def signal( self, type ):
        for service in self.services.values():
            if service.default:
                service.signal( type )
        
    def controllerConnectionChanged( self, val ):
        self.updateClient( controllersConnection = \
                self.controllersConnection )

    def setDetectsPresence( self, val ):
        if val != self.detectsPresence:
            self.detectsPresence = val
            db.updateObject( 'devices', 
                    { 'id': self.id, 'detects_presence': val } )
            if self.detectsPresence and self.presence:
                self.presence = False
                self.setPresence( True )

    def setPresence( self, val ):
        if self.presence != val:
            if self.detectsPresence:
                self.presence = val
                if self.presence:
                    log.msg( self.name + " presence on" )
                    if not self.active:
                        self.presenceTimer = reactor.callLater(
                            conf.getfloat( 'control', 
                                'presenceInterval' ),
                            lambda: self.onPresenceTimer() )
                else:
                    if self.active:
                        self.stopping = True
                        self.signal( 'stop' )
                    if self.presenceTimer:
                        self.presenceTimer.cancel()
                        self.presenceTimer = None
                    log.msg( self.name + " presence off" )
            else:
                self.presence = val

    def onPresenceTimer( self ):
        self.signal( 'start' )
        self.presenceTimer = None

    def updateClient( self, **kwargs ):
        newKwargs = { 'devices': { self.id: kwargs } }
        updateClient( self.locationId, newKwargs )

    def start( self ):
        if not self.active:
            self.active = True
            self.updateClient( active = True )

    def stop( self ):
        if self.active:
            self.stoppping = False
            self.active = False
            self.pause = False
            if self.operation:
                self.operation.stop()
                self.operation = None
                for service in self.services.values():
                    service.operationDetail = None
            self.updateClient( active = False )
            self.UARTsend( 0 )
            
    def UARTsend( self, val ):
        if self.UARTvalue != val:
            for controller in self.controllers:
                controller.UARTsend( val )
            self.UARTvalue = val


    def charge( self ):
        if self.active:
            for service in self.services.values():
                service.charge()

    def toDict( self ):
        return { 'id': self.id, 'name': self.name,
                'detectsPresence': self.detectsPresence,
                'active': self.active,
                'controllersConnection': self.controllersConnection,
                'paramsXML': self.paramsXML,
                'auto': self.auto,
                'parentId': self.parentId,
                'services': dict( ( id, self.services[ id ].toDict() )
                    for id in self.services.keys() ) }

    def __init__( self, params, servicesParams ):
        self.id = params['id']
        devices[ self.id ] = self
        self.stopping = False
        self.pause = False
        self.active = False
        self.operation = None
        self.UARTvalue = 0
        self.controllers = []
        self.type = params[ 'type_id' ]
        self.name = params[ 'name' ]
        self.tariff = params[ 'tariff' ]
        self.locationId = params[ 'location_id' ]
        self.detectsPresence = params[ 'detects_presence' ]
        self.parentId = params['parent_id']
        self.parent = None
        self.presence = False
        self.events = []
        self.presenceTimer = None
        self.auto = self.type == 2
        self.paramsXML = params['params_xml'] if \
            params.has_key( 'params_xml' ) else None
        if servicesParams:
            self.services = dict( zip( servicesParams.keys(), 
                [ Service( serviceParams, self ) for serviceParams in 
                    servicesParams.values() ] ) )
        else: 
            self.services = {}
        if params.has_key('controllers_xml') and \
            params['controllers_xml']:
            controllersDom = \
                    etree.fromstring( params['controllers_xml'] )
            for controllerNode in \
                controllersDom.xpath( '/controllers/controller' ):
                name = controllerNode.get( 'name' )
                if name in controllers:
                    controller = controllers[ name ]
                else:
                    controller = Controller( name )
                controller.devices.append( self )
                if controller not in self.controllers:
                    self.controllers.append( controller )
                templateDom = etree.fromstring( db.getValue( '''
                    select template from device_control_templates
                    where name = %s''', 
                    ( controllerNode.get( 'template' ), ) ) )
                for controlNode in templateDom.xpath( 
                        'control[ @action = "presence" ]' ):
                    line = int( controlNode.get( 'no' ) )
                    controller.setCallback( line, 
                            self.setPresence )
                    controller.setLineMode( line, 'in' )
                for eventNode in templateDom.xpath( 'event' ):
                    self.events.append( 
                            self.Event( controller, eventNode ) )
                for serviceNode in templateDom.xpath( 'service' ):
                    service = self.services[ 
                            int( serviceNode.get( 'id' ) ) ]
                    for controlNode in serviceNode.xpath( 
                        'control[ @mode = "pulse" or @signal = "yes" ]' ):
                        line = int( controlNode.get( 'no' ) )
                        service.outLines[ controlNode.get( 'action' ) 
                                ].append( {'controller': controller,
                                'line': line } )
                        controller.setLineMode( line, 
                                controlNode.get( 'mode' ) )

                    for controlNode in serviceNode.xpath(
                            'control[ @action = "status" ]' ):
                        line = int( controlNode.get( 'no' ) )
                        service.statusLines.append(
                                { 'controller': controller,
                                    'line': line,
                                    'negative': 
                                        ( controlNode.get( 'negative' ) 
                                            == '1' ) } )
                        controller.setCallback( line, 
                                service.checkStatusLines )
                        controller.setLineMode( line, 
                                controlNode.get( 'mode' ) )



    @property
    def controllersConnection( self ):
        if [ c for c in self.controllers if not c.connected ]:
            if [ c for c in self.controllers if c.connected ]:
                return None
            else:
                return False
        else:
            return True


class Service:
    def __init__( self, params, device ):
        self.device = device
        self.active = False
        self.operationDetail = None
        self.default = params[ 'is_default' ]
        self.id = params[ 'id' ]
        self.name = params[ 'name' ]
        self.tarification = params[ 'tarification' ]
        self.outLines = { 'start': [], 'stop': [], 'status': [] }
        self.statusLines = []

    def controllersConnected( self ):
        if [ l for l in self.statusLines \
                if not l[ 'controller' ].connected ]:
            return False
        else:
            return True

    def toDict( self ):
        return { 'id': self.id, 'name': self.name, 
                'active': self.active }

    def checkStatusLines( self, state = False ):
        active = True
        for line in self.statusLines:
            if line[ 'controller' ].getLineState( line[ 'line' ] ) ==\
                line[ 'negative' ]:
                active = False
                break
        if self.active and not active:
            self.stop()
        if not self.active and active:
            self.start()

    def updateClient( self, **kwargs ):
        newKwargs = { 'devices': { self.device.id: 
            { 'services': { self.id: kwargs } } } }
        updateClient( self.device.locationId, newKwargs )

    def start( self ):
        if not self.active:
            self.active = True
            log.msg( self.device.name + " " + self.name + " start" )
            if self.default:
                if self.device.pause:
                    self.device.pause = False
                elif not self.device.active:
                    self.device.start()
            elif self.device.presence and not self.device.active:
                self.device.signal( "start" )
            self.updateClient( active=True )


    def stop( self ):
        if self.active:
            self.active = False
            log.msg( self.device.name + " " + self.name + " stop" )
            if self.default:
                if self.device.stopping:
                    self.device.stop()
                else:
                    self.device.pause = True
            self.updateClient( active = False )

    def charge( self ):
        if self.active and self.device.active \
                and not self.device.pause \
                and self.controllersConnected():
            if not self.device.operation:
                Operation( self.device )
            if not self.operationDetail:
                OperationDetail( self, self.device.operation )
            self.operationDetail.charge()

    def signal( self, type ):
        for line in self.outLines[ type ]:
            line[ 'controller' ].pulseLine( line[ 'line' ] )
        for line in self.outLines[ 'status' ]:
            line[ 'controller' ].setLineState( line[ 'line' ], 
                    ( type == 'start' ) )

class Operation:
    def __init__( self, device = None, id = None ):
        self.details = {}
        self.car = None
        self.noLP = False
        self.serviceMode = False
        self.client = None
        self.device = None
        self.UARTtotal = 0
        self.parent = None
        self.children = {}
        if id:
            dbData = db.getObject( 'operations', { 'id': id } )
            self.id = id
            self.tstamp_start = dbData[ 'tstamp_start' ]
            self.tstamp_stop = dbData[ 'tstamp_end' ]
            self.locationId = dbData[ 'location_id' ]
            if dbData[ 'device_id' ]:
                self.device = devices[ dbData[ 'device_id' ] ]
            if dbData[ 'current_device_id' ] and \
                ( devices[ dbData[ 'current_device_id' ] ].active or \
                not devices[ dbData[ 'current_device_id' ] \
                    ].controllersConnection ):
                self.device = devices[ dbData[ 'current_device_id' ] ]
                self.device.operation = self
            else:
                self.stop()
            if dbData['car_id']:
                self.setCar( Car( { 'id': dbData['car_id'] } ) )
            if dbData[ 'no_lp' ]:
                self.setNoLP()
            if dbData[ 'service_mode' ]:
                self.setServiceMode()
            if dbData[ 'client_id' ]:
                self.client = getWashClient( dbData[ 'client_id' ] )
            if dbData[ 'parent_id' ]:
                self.setParent( 
                        Operation( None, dbData[ 'parent_id' ] ) )
            detailData = cursor2dicts( db.execute ( '''
                select * from operation_detail where operation_id = 
                ''' + str( id ) ), True )
            if detailData:
                for row in detailData.values():
                    detail = OperationDetail( 
                            devices[ row[ 'device_id' ] 
                        ].services[ row[ 'service_id' ] ], self )
                    detail.update( row[ 'qty' ] )
        else:
            self.device = device
            device.operation = self
            self.locationId = device.locationId
            dbData = db.getObject( 'operations',
                    { 'location_id': device.locationId,
                        'device_id': device.id,
                        'current_device_id': device.id },
                    True )
            self.tstamp_start = dbData[ 'tstamp_start' ]
            self.tstamp_stop = None
            self.id = dbData[ 'id' ]
        operations[ self.id ] = self
        updateClient( self.locationId,
                { 'operations': { self.id: self.toDict() },
                    'create': True } )

    def setParent( self, parent ):
        if parent == self.parent:
            return
        if parent:
            self.parent = parent
            parent.children[ self.id ] = self
            if self.parent.car != self.car:
                if self.parent.car:
                    self.setCar( self.parent.car )
                else:
                    self.parent.setCar( self.car )
            if self.parent.client != self.client:
                if self.parent.client:
                    self.setClient( self.parent.client )
                else:
                    self.parent.setClient( self.client )
            if self.parent.serviceMode != self.serviceMode:
                if self.parent.serviceMode:
                    self.setServiceMode()
                else:
                    self.parent.setServiceMode()
            if self.parent.noLP != self.noLP:
                if self.parent.noLP:
                    self.setNoLP()
                else:
                    self.parent.setNoLP()
        elif self.parent:
            del self.parent.children[ self.id ]
        self.recalc()

    def setLink( self, value ):
        if value and self.device.operation == self and \
            self.device.parent and self.device.parent.operation:
            self.setParent( self.device.parent.operation )
        elif not value:
            self.setParent( None )

        

    def getTotal( self ):
        r = 0
        for deviceId in self.details.keys():
            for serviceId in self.details[ deviceId ].keys():
                r += self.details[ deviceId ][ serviceId ].total
        return r

    def recalc( self ):
        for deviceId in self.details.keys():
            for serviceId in self.details[ deviceId ].keys():
                self.details[ deviceId ][ serviceId ].recalc()
   

    def toDict( self ):
        return { 'id': self.id,
                'start': formatDT( self.tstamp_start, '%H:%M' ),
                'startEpoch': epoch( self.tstamp_start ),
                'stop': formatDT( self.tstamp_stop, '%H:%M' ),
                'stopEpoch': epoch( self.tstamp_stop ),
                'device': self.device.id if self.device else None,
                'noLP': self.noLP,
                'serviceMode': self.serviceMode,
                'car': self.car.toDict() if self.car else None,
                'client': self.client,
                'details': 
                    dict( ( deviceId, dict( ( serviceId,
                        self.details[ deviceId 
                            ][ serviceId ].toDict() ) 
                        for serviceId in 
                            self.details[ deviceId ].keys() ) )
                        for deviceId in self.details.keys() ) 
                    }

    def stop( self ):
        if ( not self.tstamp_stop ):
            self.tstamp_stop = ( 
                self.update( current_device_id = None ) 
                    )[ 'tstamp_end' ] 
        self.updateClient( 
                stop = formatDT( self.tstamp_stop, '%H:%M' ),
                stopEpoch = epoch( self.tstamp_stop ) )

    def update( self, **params ):
        params[ 'id' ] = self.id
        return db.updateObject( 'operations', params )

    def updateClient( self, **kwargs ):
        newKwargs = { 'operations': { self.id: kwargs } }
        updateClient( self.locationId, newKwargs )

    def close( self, total, paySum ):
        self.update( total = total, pay_sum = paySum )
        del operations[ self.id ]
        self.updateClient( closed = True )

    def setCar( self, car ):
        self.car = car
        if car != None:
            self.serviceMode = False
            self.noLP = False
            if ( car.balance < -50 ) and self.device and \
                    self.device.operation == self:
                self.device.stopping = True
                self.device.signal( 'stop' )
        self.updateCarData()

    def setNoLP( self ):
        self.noLP = True
        self.car = None
        self.serviceMode = False
        self.updateCarData()

    def setServiceMode( self ):
        self.serviceMode = True
        self.noLP = False
        self.car = None
        self.updateCarData()

    def setClient( self, id ):
        if id:
            self.client = getWashClient( id )
            self.serviceMode = False
            self.noLP = False
        else:
            self.client = None
        self.updateCarData()

    def updateCarData( self ):
        self.update( car_id = self.car.id if self.car else None,
                client_id = self.client['id'] if self.client else None,
                service_mode = self.serviceMode,
                no_lp = self.noLP )
        if self.parent:
            self.copyCarData( self.parent )
        for child in self.children.values():
            self.copyCarData( child )

    def copyCarData( self, dst ):
        if self.car != dst.car:
            dst.setCar( self.car )
        if self.client != dst.client:
            dst.setClient( self.client )
        if self.serviceMode and not dst.serviceMode:
            dst.setServiceMode()
        if self.noLP and not dst.noLP:
            dst.setNoLP()

class OperationDetail:
    def __init__( self, service, operation ):
        self.service = service
        self.device = service.device
        self.operation = operation
        if not self.operation.details.has_key( service.device.id ):
            self.operation.details[ service.device.id ] = {}
        self.operation.details[ service.device.id ][ service.id ] = self
        if service.device.operation == operation:
            service.operationDetail = self
        dbdata = db.getObject( 'operation_detail', 
                { 'device_id': service.device.id,
                    'service_id': service.id,
                    'operation_id': self.operation.id } )
        self.id = dbdata[ 'id' ]
        self.qty = 0
        self.total = 0
        self.charged = 0
        self.prices = prices[ service.device.tariff ][ service.id ]

    def toDict( self ):
        return { 'id': self.id,
                'device': self.device.id,
                'service': self.service.id,
                'total': self.total,
                'qty': self.qty }

    def charge( self ):
        self.update( self.qty + 1 )

    def recalc( self ):
        self.update( self.qty )

    def update( self, qty, updateDB = True ):
        self.qty = qty
        if ( self.charged < self.qty ) or \
            ( self.charged > self.qty + self.service.tarification ):
            self.charged = ( ( self.qty // self.service.tarification ) \
                    + 1 ) * self.service.tarification
            if ( self.service.device != self.operation.device and \
                self.service.default ) or \
                ( self.operation.parent and self.service.default ):
                self.total = 0
            else:
                self.total = 0
                prevPrice = 0
                prevQty = 0
                for ( qty, price ) in self.prices:
                    if qty < self.qty:
                        self.total = self.total + ( qty - prevQty ) * \
                                price / 60
                        prevQty = qty
                        prevPrice = price
                    else:
                        break
                self.total = self.total + \
                        ( self.qty - prevQty ) * prevPrice / 60
            if updateDB:
                db.updateObject( 'operation_detail',
                        { 'id': self.id, 'qty': self.qty, 
                            'total': self.total } )
        if self.operation.device.operation == self.operation:
            updateClient( self.operation.locationId, 
                { 'operations': 
                    { self.operation.id: 
                        { 'details': 
                            { self.device.id: 
                                { self.service.id: { 'qty': self.qty, 
                                    'total': self.total } } } } } } )
            newUtotal = int( round( self.operation.getTotal() ) )
            while newUtotal > 1000:
                newUtotal -= 1000
            self.operation.device.UARTsend( newUtotal )



        

class ControllerProtocol( StatefulTelnetProtocol, object ):
    timeoutInterval = conf.getfloat( 'control', 'controllerTimeout' )
    pingInterval = conf.getfloat( 'control', 'pingInterval' )


    def ping( self ):
        self.queueCmd( '' )
        self.pingTimer = None

    def lineReceived(self, data):
        data = data.replace( '#', '' ).replace( '\r', '' )
        log.msg( self.factory.host + ' ' + data )
        if self.pingTimer:
            self.pingTimer.cancel()
            self.pingTimer = None;
        self.pingTimer = reactor.callLater( self.pingInterval,
                lambda: self.ping() )
        if data.startswith( 'SLINF' ) or data.startswith( 'FLAGS' ) or \
                data.startswith( 'JConfig' ):
            return
        if data.startswith( 'EVT' ) and data != 'EVT,OK':
            discard, line, state = data.rsplit( ',', 2 )
            if self.factory.linesStates:
                self.factory.saveLineState( int( line ), state )
        else:
            if self.currentCmd:
                if self.timeout:
                    self.timeout.cancel()
                    self.timeout = None
                if data == 'ERR':
                    log.err( self.factory.host + \
                        ' error in response to ' + self.currentCmd[0] )
                cmd, cb = self.currentCmd
                self.currentCmd = None
                if data != 'OK' and data != 'ERR':
                    discard, data = data.rsplit( ',', 1 )
                if len( self.cmdQueue ) > 0:
                    self.currentCmd = self.cmdQueue.popleft()
                    self.sendCmd( self.currentCmd[0] )
                if cb:
                    cb( data )

    def connectionMade( self ):
        log.err( self.factory.host + " connection made" )
        self.factory.setConnected( True )
        self.currentCmd = None
        self.cmdQueue = deque( [] )
        self.timeout = None
        self.pingTimer = None
        self.queueCmd( '' )
        self.queueCmd( "PSW,SET,Jerome", 
                lambda x: self.factory.UARTconnect() )
        self.queueCmd( "EVT,ON" )
        self.queueCmd( "IO,GET,ALL", self.factory.saveLinesDirs )
        self.queueCmd( "RID,ALL", self.factory.saveLinesStates )

    def connectionLost( self, reason ):
        log.err( self.factory.host + " connection lost" )
        self.factory.setConnected( False )
        #self.factory = None

    def sendCmd( self, cmd ):
        log.msg( self.factory.host + ' ' + cmd )
        fullCmd = cmd
        if cmd != '':
            fullCmd = "," + fullCmd
        fullCmd = "$KE" + fullCmd + "\r\n"
        self.transport.write( fullCmd )
        self.timeout = reactor.callLater( self.timeoutInterval, 
                lambda: self.callTimeout() )

    def callTimeout( self ):
        log.err( self.factory.host + " timeout" )
        self.transport.loseConnection()

    def queueCmd( self, cmd, cb = None ):
        if self.currentCmd:
            self.cmdQueue.append( ( cmd, cb ) )
        else:
            self.currentCmd = ( cmd, cb )
            self.sendCmd( cmd )

class UARTProtocol( StatefulTelnetProtocol ):
    def connectionMade( self ):
        self.factory.controller.UARTsend( 0 )
        log.err( self.factory.controller.host + ' UART connection made' )


class UARTConnection( ClientFactory ):
    maxDelay = 15

    def __init__( self, controller ):
        self.controller = controller

    def buildProtocol( self, addr ):
        self.protocol = UARTProtocol()
        self.controller.UARTconnection = self.protocol
        self.protocol.factory = self
        return self.protocol
  
    def clientConnectionLost(self, connector, reason):
        log.err( self.controller.host + \
                ' UART connection lost or failed ' + \
                reason.getErrorMessage() )
        self.controller.UARTconnectionLost()

    def clientConnectionFailed(self, connector, reason):
        self.clientConnectionLost( connector, reason )

class Controller( ReconnectingClientFactory ):
    maxDelay = 15
    UARTinterval = conf.getfloat( 'control', 'UARTinterval' )
   

    def toggleLine( self, line, cb = None ):
        self.setLineState( line, not self.getLineState( line ), cb )

    def setLineState( self, line, state, cb = None ):
        nState = '1' if state else '0'
        self.protocol.queueCmd( 'WR,' + str( line ) + ',' + nState, \
                lambda data: self.setLineStateCB( line, nState, data, 
                    cb ) )

    def setLineDir( self, line, dir, cb = None ):
        self.protocol.queueCmd( 'IO,SET,' + str( line ) + ',' + \
                str( dir ), 
                lambda data: self.setLineDirCB( line, dir, data, cb ) )

    def setLineDirCB( self, line, dir, data, cb = None ):
        if data == 'OK':
            self.linesDirs[ line ] = dir
        if cb:
            cb( data )


    def setLineMode( self, line, mode ):
        self.linesModes[ line ] = mode
        self.checkLineMode( line )

    def checkLineMode( self, line ):
        if self.linesModes.has_key( line ) and \
            len( self.linesDirs ) > line:
            if self.linesModes[ line ] == 'in':
                if self.linesDirs[ line ] == '0':
                    self.setLineDir( line, 1 )
            else:
                if self.linesDirs[ line ] == '1':
                    self.setLineDir( line, 0 )
                if self.linesModes[  line ] == 'pulse' \
                        and len( self.linesStates ) > line \
                        and self.getLineState( line ):
                    self.setLineState( line, False )

    def setLineStateCB( self, line, state, data, cb = None ):
        if data == 'OK':
            self.saveLineState( line, state )
        if cb:
            cb( data )

    def pulseLineCB( self, line, data, cb = None ):
        if data == 'OK':
            reactor.callLater( 0.3, 
                    lambda: self.toggleLine( line, cb ) )
        elif cb:
            cb( data )
            
    def pulseLine( self, line, cb = None ):
        self.toggleLine( line, \
            lambda data: self.pulseLineCB( line, data, cb ) )

    def setCallback( self, line, callback ):
        lineNo = int( line )
        if not self.callbacks.has_key( lineNo ):
            self.callbacks[ lineNo ] = []
        self.callbacks[ lineNo ].append( callback )

    def getLineState( self, line ):
        if len( self.linesStates ) > int( line ):
            return self.linesStates[ int( line ) ] 
        else:
            return None

    def saveLinesDirs( self, data ):
        self.linesDirs = [ None ] + list( data )
        for line in self.linesModes.keys():
            self.checkLineMode( line )

    def saveLinesStates( self, data ):
        self.linesStates = [ None ] + [ x == '1' for x in list( data ) ]
        for line, mode in self.linesModes.iteritems():
            if mode == 'pulse':
                self.checkLineMode( line )
        for line, callbacks in self.callbacks.items():
            for callback in callbacks:
                callback( self.linesStates[ line ] )


    def saveLineState( self, line, state ):
        if self.linesStates[ line ] != ( state == '1' ):
            self.linesStates[ line ] = ( state == '1' )
            log.msg( "line " + str( line ) + ": " + str( state ) )
            if self.callbacks.get( line ):
                for callback in self.callbacks[ line ]:
                    callback( self.linesStates[ line ] )

    def __init__( self, name ):
        self.linesDirs = []
        self.linesModes = {}
        self.linesStates = []
        self.callbacks = {}
        self.devices = []
        self.pingTimer = None
        self.UARTconnection = None
        self.UARTcache = []
        self.UARTtimer = None
        self.name = name
        self.__connected = False
        paramsXml = db.getValue( """
            select params_xml from controllers where name = %s""",
            ( name, ) )
        paramsDom = etree.fromstring( paramsXml )
        self.host = paramsDom.get( 'host' )
        self.UART = paramsDom.get( 'UART' )
        controllers[ name ] = self
        reactor.connectTCP( self.host, 2424, self )

    def UARTconnect( self ):
        if self.UART:
            reactor.connectTCP( self.host, 2525, 
                    UARTConnection( self ) )
            self.setUARTtimer()

    def UARTsend( self, val ):
        if self.UARTconnection:
            data = [40, 41, 42]
            co = 0
            while val > 1000:
                val -= 1000
            while val:
                data[ co ] = ( ( val % 10 ) << 2 ) | co
                co += 1
                val /= 10

            self.UARTcache = data

            for d in data:
                self.UARTconnection.transport.write( chr( d ) )
            if self.UARTtimer and self.UARTtimer.active():
                self.UARTtimer.cancel()
            self.setUARTtimer()

    def setUARTtimer( self ):
        if self.UARTconnection:
            self.UARTtimer = reactor.callLater( self.UARTinterval,
                lambda: self.UARTrepeat() )


    def UARTrepeat( self ):
        if self.UARTconnection:
            for d in self.UARTcache:
                self.UARTconnection.transport.write( chr( d ) )
            self.setUARTtimer()

    def UARTconnectionLost( self ):
        if self.UARTconnection:
            self.UARTconnection.factory.stopFactory()
        if self.UARTtimer and self.UARTtimer.active():
            self.UARTtimer.cancel()
        if self.connected and self.UART:
            reactor.callLater( 5, self.UARTconnect )


       

    def getConnected( self ):
        return self.__connected

    def setConnected( self, val ):
        log.err( "Controller " + self.host + \
                ' connected: ' + str( val ) )
        self.__connected = val
        for device in self.devices:
            device.controllerConnectionChanged( val )
        if not val and self.UARTconnection:
            self.UARTconnection.factory.stopFactory()
            log.err( "Controller " + self.host + ' UART disconnected' )
            self.UARTconnection = None

    connected = property( getConnected, setConnected )

    def buildProtocol( self, addr ):
        self.protocol = ControllerProtocol()
        self.protocol.factory = self
        return self.protocol

    def startedConnecting(self, connector):
        log.msg( 'Started to connect ' + 
                connector.getDestination().host )

def charge():
    for device in devices.values():
        device.charge()

def getWashClient( id ):
    return db.getObject( 'clients', { 'id': id } )

def updateClient( locationId, kwargs ):
    if clientConnections.has_key( locationId ):
        for cc in clientConnections[ locationId ]:
            cc.update( kwargs )


def main():
    locationsStr = conf.get( 'control', 'locations' )
    deviceTypesStr = conf.get( 'control', 'deviceTypes' )
    deviceTypes = [ int( s ) for s in deviceTypesStr.split( ',' ) ]
    locations = [ int( s ) for s in locationsStr.split( ',' ) ]

    devicesParams = cursor2dicts( 
        db.execute( '''
            select * 
            from devices 
            where location_id in ( %s ) and type_id in ( %s )''' % 
            ( locationsStr, deviceTypesStr ) ), True )

    if not devicesParams:
        log.err( "No devices on this location!" )
        return

    pricesData = cursor2dicts(
        db.execute( 
        "select * from prices order by tariff, service_id, count" ), 
        False )
    for row in pricesData:
        if not prices.has_key( row[ 'tariff' ] ):
            prices[ row[ 'tariff' ] ] = {}
        if not prices[ row[ 'tariff' ] ].has_key( row[ 'service_id' ] ):
            prices[ row[ 'tariff' ] ][ row[ 'service_id' ] ] = []
        prices[ row[ 'tariff' ] ][ row[ 'service_id' ] ].append(
                ( row[ 'count' ], row[ 'price' ] ) )

    servicesParams = {}
    for deviceType in deviceTypes:
        servicesParams[ deviceType ] = cursor2dicts(
            db.execute( '''
                select * 
                from services 
                where type_id = %s
                ''', ( deviceType, ) ), True )

    for deviceParams in devicesParams.values():
        devices[ deviceParams['id'] ] = Device( deviceParams, 
                servicesParams[ deviceParams[ 'type_id' ] ] )    
    for device in devices.values():
        if device.parentId:
            device.parent = devices[ device.parentId ]

    global clientButtons
    clientButtons = dict( [ ( l, cursor2dicts( 
        db.execute( '''
            select client_id as id, image, 
                ( select name from clients 
                    where clients.id = client_id ) as name
                from client_buttons 
                where location_id = %s''',
                ( l, ) ), False ) ) for l in locations ] )

    global operators 
    operators = cursor2dicts( db.execute( '''
        select id, name from operators where active''' ),
        False )

    prevOperations = cursor2dicts(
            db.execute( '''
                select id
                from operations
                where pay is null and location_id in ( %s ) ''' %
                locationsStr ), True )

    if prevOperations:
        for opId in prevOperations.keys():
            if not opId in operations:
                Operation( None, opId )

    chargeCall = task.LoopingCall( charge )
    chargeCall.start( 1 )

    siteRoot = static.File( conf.get( 'common', 'siteRoot' ) )
    reactor.listenTCP( conf.getint( 'common', 'httpPort' ), 
            server.Site( siteRoot ) )
    reactor.listenTCP( conf.getint( 'common', 'pbPort' ), 
            pb.PBServerFactory( PbServer() ) )
    reactor.run()

if __name__ == '__main__':
    main()

