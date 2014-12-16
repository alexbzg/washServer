#!/usr/bin/python
#coding=utf-8

import psycopg2, sys

from common import siteConf

def cursor2dicts( cur, keys = None ):
    if cur and cur.rowcount:
        colNames = [ col[0] for col in cur.description ]
        if cur.rowcount == 1 and not keys:
            return dict( zip( colNames, cur.fetchone() ) )
        else:
            if ( 'id' in colNames ) and keys:
                idIdx = colNames.index( 'id' )
                return dict( 
                        ( row[ idIdx ], dict( zip( colNames, row ) ) )
                        for row in cur.fetchall() )
            else:
                return [ dict( zip( colNames, row ) ) for
                        row in cur.fetchall() ]
    else:
        return False

class dbConn:

    def __init__( self ):
        connStr = ' '.join( 
                [ k + "='" + v + "'" 
                    for k, v in siteConf().items( 'db' ) ] )
        try:
            conn = psycopg2.connect( connStr )
            conn.set_client_encoding( 'UTF8' )
            self.conn = conn
        except:
            sys.stderr.write( "No db connection!" )
            self = False

    def fetch( self, sql, params = None ):
        cur = self.conn.cursor()
        cur.execute( sql, params )
        if cur.rowcount:
            res = cur.fetchall()
            cur.close()
            return res
        else:
            cur.close()
            return False

    def execute( self, sql, params = None ):
        cur = self.conn.cursor()
        try:
            cur.execute( sql, params )
        except psycopg2.Error, e:
            sys.stderr.write( "Error executing: " + sql + "\n" )
            if params:
                sys.stderr.write( "Params: " )
                print >>sys.stderr, params
            if e.pgerror:
                sys.stderr.write(  e.pgerror )
                self.error = e.pgerror
            sys.stderr.flush()
            self.conn.rollback()
            return False
        return cur

    def getValue( self, sql, params = None ):
        res = self.fetch( sql, params )
        if res:
            return res[0][0]
        else:
            return False

    def commit( self ):
        self.conn.commit()

    def getObject( self, table, params, create = False, 
            never_create = False ):
        sql = ''
        cur = False
        if not create:
            sql = "select * from %s where %s" % (
                    table, 
                    " and ".join( [ k + " = %(" + k + ")s"
                        if params[ k ] != None 
                        else k + " is null"
                        for k in params.keys() ] ) )
            cur = self.execute( sql, params )
            if cur and not cur.rowcount:
                cur.close()
                cur = False
                if never_create:
                    return False
        if create or not cur:
            keys = params.keys()
            sql = "insert into " + table + " ( " + \
                ", ".join( keys ) + ") values ( " + \
                ', '.join( [ "%(" + k + ")s" for k in keys ] ) + \
                " ) returning *"
            cur = self.execute( sql, params )
        if cur:
            objRes = cursor2dicts( cur )
            cur.close()
            self.commit()
            return objRes
        else:
            return False

    def getMasterDetail( self, masterSQL, params, detailSQL, 
            detailFieldName = 'detail' ):
        data = cursor2dicts( self.execute( masterSQL, params ) )
        for row in data:
            row[ detailFieldName ] = cursor2dicts( 
                    self.execute( detailSQL, row ) )
        return data

    def updateObject( self, table, params ):
        paramString = ", ".join( [ k + " = %(" + k + ")s" 
            for k in params.keys() if k != "id" ] )
        if paramString != '':
            sql = "update " + table + " set " + paramString + \
                " where id = %(id)s returning *" 
            cur = self.execute( sql, params )
            if cur:
                objRes = cursor2dicts( cur )
                cur.close()
                self.commit()
                return objRes

    def deleteObject( self, table, id ):
        sql = "delete from " + table + " where id = %s" 
        self.execute( sql, ( id, ) ).close()
        self.commit()

db = dbConn()


    
