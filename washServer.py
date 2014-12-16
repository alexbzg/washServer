#!/usr/bin/python
#coding=utf-8
from twisted.internet import reactor, protocol, task
from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from twisted.conch.telnet import StatefulTelnetProtocol
from twisted.python import log
from twisted.spread import pb
from twisted.web import static, server
import sys
log.startLogging(sys.stdout)

from lxml import etree
from collections import deque

from common import siteConf, appRoot
from washDB import db, cursor2dicts
import simplejson as json
import decimal
import datetime
import re
from datetime import date, timedelta

def formatDT( date, format ):
    return date.strftime( format ) if date else None


def jsonEncodeExtra( obj ):
    if isinstance( obj, decimal.Decimal ):
        return str( obj )
    if isinstance( obj, datetime.datetime ):
        return obj.isoformat()
    if hasattr( obj, '__dict__' ):
        return None
    print repr( obj ) + " is not JSON serializable"
    return None

conf = siteConf()

class Car:
    reLP = re.compile( r'/^(.{5,}\D)(\d+)$/' )

    @classmethod
    def fromQuery( cls, query ):
        queryMatch = reLP.match( query )
        ln, region
        if queryMatch:
            ln = queryMatch( 1 )
            region = queryMatch( 2 )
        else:
            ln = query
            region = None
        return cls( { 'license_no': ln, 'region': region } )

    def __init__( self, params ):
        dbData = db.getObject( 'cars', params, create = True )
        self.id = dbData[ 'id' ]
        self.licenseNo = dbData[ 'license_no' ]
        self.region = dbData[ 'region' ]
        self.balance = 0
        self.notpayed = None

        if dbData[ 'client_id' ]:
            self.client = db.getObject( 'clients', 
                    { 'id': dbData[ 'client_id' ] } )
        else:
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
                as tstamp_start
                from operations where car_id = %s 
                and total - pay_sum > 50 and not service_mode 
                and pay is not null;
                """
            self.notpayed = cursor2dicts( 
                    db.execute( sql, ( carId, ) ),
                    True )
            if not slef.notpayed:
                notpayed = None
        else:
            self.notpayed = None

    def toDict( self ):
        return { 'id': self.id,
                'licenseNo': self.licenseNo,
                'region': self.region,
                'balance': self.balance,
                'notpayed': self.notpayed }
   

       

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
            self.update( 
                { 'cars': \
                    { Car.fromQuery( d[ 'carQuery' ] ).toDict() }, 
                    'create': 1 } )
        elif d.has_key( 'setCarData' ):
            if d[ 'setCarData' ].has_key( 'car' ):
                operations[ d[ 'setCarData' ][ 'operation' ] ].setCar( \
                    d[ 'setCarData' ][ 'car' ] )
            elif d[ 'setCarData' ].has_key( 'serviceMode' ):
                operations[ d[ 'setCarData' ][ 'operation' ] \
                        ].setServiceMode()
            elif d[ 'setCarData' ].has_key( 'noLP' ):
                operations[ d[ 'setCarData' ][ 'operation' ] \
                        ].setNoLP()


    def update( self, params ):
        try:
            self.clientSide.callRemote( "update", 
                json.dumps( params, default = jsonEncodeExtra ) )
        except pb.DeadReferenceError:
            print "the client disconnected or crashed"
            del clientConnections[ self.locationId ]
            return
        f = open( conf.get( 'common', 'siteRoot' ) + \
                '/client-files/debug/' + str( self.count ) + \
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
        clientConnections[ locationId ] = pbc
        pbc.update( { 'devices': dict( ( id, devices[ id ].toDict() ) 
            for id in devices.keys() 
            if devices[ id ].locationId == locationId ),
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
            print 'trigger on line ' + self.node.get( 'line' ) + \
                    ' signal ' + self.node.get( 'signal' ) + \
                    ' continues ' + str( self.node.get( 'continues' ) )
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

    def setPresence( self, val ):
        if self.detectsPresence and self.presence != val:
            self.presence = val
            if self.presence:
                print self.name + " presence on"
                if not self.active:
                    self.presenceTimer = reactor.callLater(
                        conf.getfloat( 'control', 'presenceInterval' ),
                        lambda: self.onPresenceTimer() )
            else:
                if self.active:
                    self.stopping = True
                    self.signal( 'stop' )
                if self.presenceTimer:
                    self.presenceTimer.cancel()
                    self.presenceTimer = None
                print self.name + " presence off"

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

    def charge( self ):
        if self.active:
            for service in self.services.values():
                service.charge()

    def toDict( self ):
        return { 'id': self.id, 'name': self.name,
                'detectsPresence': self.detectsPresence,
                'active': self.active,
                'services': dict( ( id, self.services[ id ].toDict() )
                    for id in self.services.keys() ) }

    def __init__( self, params, servicesParams ):
        self.id = params['id']
        devices[ self.id ] = self
        self.stopping = False
        self.pause = False
        self.active = False
        self.operation = None
        self.type = params[ 'type_id' ]
        self.name = params[ 'name' ]
        self.tariff = params[ 'tariff' ]
        self.locationId = params[ 'location_id' ]
        self.detectsPresence = params[ 'detects_presence' ]
        self.presence = False
        self.events = []
        self.presenceTimer = None
        self.services = dict( zip( servicesParams.keys(), 
            [ Service( serviceParams, self ) for serviceParams in 
                servicesParams.values() ] ) )
        controllersDom = etree.fromstring( params['controllers_xml'] )
        for controllerNode in \
            controllersDom.xpath( '/controllers/controller' ):
            name = controllerNode.get( 'name' )
            if name in controllers:
                controller = controllers[ name ]
            else:
                controller = Controller( name )
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
                service = self.services[ int( serviceNode.get( 'id' ) ) ]
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
            print self.device.name + " " + self.name + " start"
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
            print self.device.name + " " + self.name + " stop"
            if self.default:
                if self.device.stopping:
                    self.device.stop()
                else:
                    self.device.pause = True
            self.updateClient( active = False )

    def charge( self ):
        if self.active and self.device.active and not self.device.pause:
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
        if id:
            dbData = db.getObject( 'operations', { 'id': id } )
            self.id = id
            self.tstamp_start = dbData[ 'tstamp_start' ]
            self.tstamp_stop = dbData[ 'tstamp_end' ]
            self.locationId = dbData[ 'location_id' ]
            if dbData[ 'current_device_id' ] and \
                devices[ dbData[ 'current_device_id' ] ].active:
                self.device = devices[ dbData[ 'current_device_id' ] ]
                self.device.operation = self
            else:
                self.device = None
                self.stop()
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

    def toDict( self ):
        return { 'id': self.id,
                'start': formatDT( self.tstamp_start, '%H:%M' ),
                'stop': formatDT( self.tstamp_stop, '%H:%M' ),
                'device': self.device.id if self.device else None,
                'noLP': self.noLP,
                'serviceMode': self.serviceMode,
                'car': self.car.toDict() if self.car else None,
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
            self.tstamp_stop = ( self.update( current_device_id = None ) 
                    )[ 'tstamp_end' ] 
        self.updateClient( 
                stop = formatDT( self.tstamp_stop, '%H:%M' ) )

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
        self.serviceMode = False
        self.noLP = False
        self.car = car
        self.updateCarData()
        if ( car.getBalance() < -50 ) and self.device:
            self.device.signal( 'stop' )
        self.updateClient( carId = car.id, 
                license_no = car.license_no,
                region = car.region, balance = car.balance )

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

    def updateCarData( self ):
        self.update( car_id = self.car.id if self.car else None,
                service_mode = self.serviceMode,
                no_lp = self.noLP )

class OperationDetail:
    def __init__( self, service, operation ):
        self.service = service
        self.device = service.device
        self.operation = operation
        if not self.operation.details.has_key( service.device.id ):
            self.operation.details[ service.device.id ] = {}
        self.operation.details[ service.device.id ][ service.id ] = self
        if operation.device == service.device:
            service.operationDetail = self
        dbdata = db.getObject( 'operation_detail', 
                { 'device_id': service.device.id,
                    'service_id': service.id,
                    'operation_id': self.operation.id },
                True )
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
            if self.service.device != self.operation.device and \
                    self.service.default:
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
        if self.operation.device:
            updateClient( self.operation.locationId, 
                { 'operations': 
                    { self.operation.id: 
                        { 'details': 
                            { self.device.id: 
                                { self.service.id: { 'qty': self.qty, 
                                    'total': self.total } } } } } } )


        

class ControllerProtocol( StatefulTelnetProtocol, object ):
    timeoutInterval = conf.getfloat( 'control', 'controllerTimeout' )
    pingInterval = conf.getfloat( 'control', 'pingInterval' )


    def ping( self ):
        self.queueCmd( '' )
        self.pingTimer = None

    def lineReceived(self, data):
        data = data.replace( '#', '' ).replace( '\r', '' )
        print self.factory.host + ' ' + data
        if self.pingTimer:
            self.pingTimer.cancel()
            self.pingTimer = None;
        self.pingTimer = reactor.callLater( self.pingInterval,
                lambda: self.ping() )
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
                    print self.factory.host + \
                        ' error in response to ' + self.currentCmd[0]
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
        print self.factory.host + " connection made"
        self.currentCmd = None
        self.cmdQueue = deque( [] )
        self.timeout = None
        self.pingTimer = None
        self.queueCmd( '' )
        self.queueCmd( "PSW,SET,Jerome" )
        self.queueCmd( "EVT,ON" )
        self.queueCmd( "IO,GET,ALL", self.factory.saveLinesDirs )
        self.queueCmd( "RID,ALL", self.factory.saveLinesStates )

    def connectionLost( self, reason ):
        print self.factory.host + " connection lost"
        #self.factory = None

    def sendCmd( self, cmd ):
        print self.factory.host + ' ' + cmd   
        fullCmd = cmd
        if cmd != '':
            fullCmd = "," + fullCmd
        fullCmd = "$KE" + fullCmd + "\r\n"
        self.transport.write( fullCmd )
        self.timeout = reactor.callLater( self.timeoutInterval, 
                lambda: self.callTimeout() )

    def callTimeout( self ):
        print self.factory.host + " timeout"
        self.transport.loseConnection()

    def queueCmd( self, cmd, cb = None ):
        if self.currentCmd:
            self.cmdQueue.append( ( cmd, cb ) )
        else:
            self.currentCmd = ( cmd, cb )
            self.sendCmd( cmd )

class Controller( ReconnectingClientFactory ):
    maxDelay = 15

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
                        and ( not self.getLineState( line ) ):
                    self.setLineState( line, False )

    def setLineStateCB( self, line, state, data, cb = None ):
        if data == 'OK':
            self.saveLineState( line, state )
        if cb:
            cb( data )

    def pulseLineCB( self, line, data, cb = None ):
        #print "pulse Line cb " + str( line ) + ': ' + data
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
            print "line " + str( line ) + ": " + str( state )
            if self.callbacks.get( line ):
                for callback in self.callbacks[ line ]:
                    callback( self.linesStates[ line ] )

    def __init__( self, name ):
        self.linesDirs = []
        self.linesModes = {}
        self.linesStates = []
        self.callbacks = {}
        self.pingTimer = None
        self.name = name
        paramsXml = db.getValue( """
            select params_xml from controllers where name = %s""",
            ( name, ) )
        paramsDom = etree.fromstring( paramsXml )
        self.host = paramsDom.get( 'host' )
        controllers[ name ] = self
        reactor.connectTCP( self.host, 2424, self )

    def buildProtocol( self, addr ):
        self.protocol = ControllerProtocol()
        self.protocol.factory = self
        return self.protocol

    def startedConnecting(self, connector):
        print 'Started to connect ' + connector.getDestination().host

locations, devices, controllers, prices, operations, \
    clientConnections = {}, {}, {}, {}, {}, {}

def charge():
    for device in devices.values():
        device.charge()

def updateClient( locationId, kwargs ):
    if clientConnections.has_key( locationId ):
        clientConnections[ locationId ].update( kwargs )

locationsStr = conf.get( 'control', 'locations' )
deviceTypesStr = conf.get( 'control', 'deviceTypes' )
deviceTypes = [ int( s ) for s in deviceTypesStr.split( ',' ) ]

devicesParams = cursor2dicts( 
    db.execute( '''
        select * 
        from devices 
        where location_id in ( %s ) and type_id in ( %s )''' % 
        ( locationsStr, deviceTypesStr ) ), True )

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

prevOperations = cursor2dicts(
        db.execute( '''
            select id
            from operations
            where pay is null and location_id in ( %s ) ''' %
            locationsStr ), True )

if prevOperations:
    for opId in prevOperations.keys():
        Operation( None, opId )

chargeCall = task.LoopingCall( charge )
chargeCall.start( 1 )

siteRoot = static.File( conf.get( 'common', 'siteRoot' ) + \
        '/client-files' )
reactor.listenTCP( 8788, server.Site( siteRoot ) )

reactor.listenTCP( 8789, pb.PBServerFactory( PbServer() ) )
reactor.run()


