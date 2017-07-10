#!/usr/bin/python
#coding=utf-8
from twisted.internet import reactor, protocol, task
from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory, ClientCreator
from twisted.conch.telnet import TelnetTransport, StatefulTelnetProtocol
import logging
from collections import deque

class ControllerProtocol( StatefulTelnetProtocol, object ):
    timeoutInterval = 1
    pingInterval = 60


    def ping( self ):
        self.queueCmd( '' )
        self.pingTimer = None

    def lineReceived(self, data):
        data = data.replace( '#', '' ).replace( '\r', '' )
        logging.info( self.factory.host + ' ' + data )
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
                    logging.error( self.factory.host + \
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
        logging.error( self.factory.host + " connection made" )
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
        logging.error( self.factory.host + " connection lost" )
        self.factory.setConnected( False )
        #self.factory = None

    def sendCmd( self, cmd ):
        logging.info( self.factory.host + ' ' + cmd )
        fullCmd = cmd
        if cmd != '':
            fullCmd = "," + fullCmd
        fullCmd = "$KE" + fullCmd + "\r\n"
        self.transport.write( fullCmd )
        self.timeout = reactor.callLater( self.timeoutInterval, 
                lambda: self.callTimeout() )

    def callTimeout( self ):
        logging.error( self.factory.host + " timeout" )
        self.transport.loseConnection()

    def queueCmd( self, cmd, cb = None ):
        if self.currentCmd:
            self.cmdQueue.append( ( cmd, cb ) )
        else:
            self.currentCmd = ( cmd, cb )
            self.sendCmd( cmd )

class UARTProtocol( StatefulTelnetProtocol ):

    def connectionLost( self, reason ):
        logging.error( self.controller.host + \
                ' UART connection lost or failed ' + \
                reason.getErrorMessage() )
        self.controller.UARTconnectionLost()




class Controller( ReconnectingClientFactory ):
    UARTClientCreator = ClientCreator( reactor, UARTProtocol )
    maxDelay = 15
    UARTinterval = 0
   

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
            logging.info( "line " + str( line ) + ": " + str( state ) )
            if self.callbacks.get( line ):
                for callback in self.callbacks[ line ]:
                    callback( self.linesStates[ line ] )

    def __init__( self, params ):
        self.linesDirs = []
        self.linesModes = {}
        self.linesStates = []
        self.callbacks = {}
        self.setConnectedCallbacks = []
        self.pingTimer = None
        self.UARTconnection = None
        self.UARTcache = []
        self.UARTtimer = None
        self.__connected = False
        if params.has_key( 'name' ):
            self.name = params[ 'name' ]
        self.host = params[ 'host' ]
        self.UART = params[ 'UART' ]
        reactor.connectTCP( self.host, 2424, self )

    def UARTonProtocol( self, p ):
        self.UARTconnection = p
        p.controller = self
        self.setUARTtimer()
        self.UARTsend( 0 )
        logging.error( self.host + ' UART connected' )

    def UARTconnect( self ):
        if self.UART:
            uc = Controller.UARTClientCreator.connectTCP( 
                    self.host, 2525 )
            uc.addCallback( self.UARTonProtocol )

    def UARTsend( self, val ):
        if self.UARTconnection:
            v0 = val
            data = [40, 41, 42]
            co = 0
            while val > 1000:
                val -= 1000
            try:
                while val:
                    data[ co ] = ( ( val % 10 ) << 2 ) | co
                    co += 1
                    val /= 10
            except IndexError, e:
                logging.error( "IndexError: value - " + str(v0) + \
                        " co - " + str( co ) )
                return


            self.UARTcache = data

            for d in data:
                self.UARTconnection.transport.write( chr( d ) )
            if self.UARTtimer and self.UARTtimer.active():
                self.UARTtimer.cancel()
            self.setUARTtimer()

    def setUARTtimer( self ):
        if self.UARTconnection and self.UARTinterval > 0:
            self.UARTtimer = reactor.callLater( self.UARTinterval,
                lambda: self.UARTrepeat() )


    def UARTrepeat( self ):
        if self.UARTconnection:
            for d in self.UARTcache:
                self.UARTconnection.transport.write( chr( d ) )
            self.setUARTtimer()

    def UARTconnectionLost( self ):
        if self.UARTconnection:
            self.UARTconnection.transport.loseConnection()
            self.UARTconnection = None
        if self.UARTtimer and self.UARTtimer.active():
            self.UARTtimer.cancel()
        if self.connected and self.UART:
            reactor.callLater( 5, self.UARTconnect )


       

    def getConnected( self ):
        return self.__connected

    def setConnected( self, val ):
        self.__connected = val
        for entry in self.setConnectedCallbacks:
            entry( val )
        if not val and self.UARTconnection:
            logging.error( "Controller " + self.host + \
                    ' UART disconnected' )
            self.UARTconnectionLost()

    connected = property( getConnected, setConnected )

    def buildProtocol( self, addr ):
        self.protocol = ControllerProtocol()
        self.protocol.factory = self
        return self.protocol

    def startedConnecting(self, connector):
        logging.info( 'Started to connect ' + 
                connector.getDestination().host )

