#!/usr/bin/env python
"""
_PhEDExNotifierComponent_

ProdAgent Component to notify clients of new transfers

"""
__all__ = []
__revision__ = "$Id: PhEDExNotifierComponent.py,v 1.11 2008/08/15 15:29:44 gowdy Exp $"
__version__ = "$Revision: 1.11 $"

import logging

from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser

from urllib import urlopen
from urllib import quote

class PhEDExNotifierComponent(FeederImpl):
    """
    _PhEDExNotifierComponent_

    """
    
    def __init__( self, nodes, phedexURL = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas", dbsURL = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet" ):
        
        # Add the node specification the URL
        nodeList = self.makelist( nodes )
        for node in nodeList:
            # for the first node need to be a bit different
            if node == nodeList[0]:
                self.nodeURL = phedexURL + "?node=%s" % node
            else:
                self.nodeURL += "&node=%s" % node
        self.dbsURL = dbsURL

        try:
            #optManager  = DbsOptionParser()
            #(opts,args) = optManager.getOpt()
            #opts[ 'url' ] = dbsURL
            self.dbsapi = DbsApi( {'url': dbsURL } )#opts.__dict__)
        except DbsApiException, ex:
            print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
            if ex.getErrorCode() not in (None, ""):
                print "DBS Exception Error Code: ", ex.getErrorCode()


    def __call__( self, filesets ):
        """
        _operator(message, payload)_

        Respond to messages from the message service

        """
        filesetList = self.makelist( filesets )
        for fileset in filesetList:
            entity = fileset.name

            # determine if we have a block or dataset
            if entity.find("#") != -1:
                self.doBlock( entity, fileset )
            else:
                self.doDataset( entity, fileset )


    def doDataset( self, entity, fileset ):

        # need to adjust the URL to get the list of blocks
        datasetURL = self.nodeURL.replace( "fileReplicas", "blockReplicas" )

        connection = urlopen( datasetURL + "&block=%s%s" % (entity,"*") )
        aString = connection.read()
        connection.close()

        if aString[2:8] != "phedex":
            print "PhEDExNotifier: bad string from server follows."
            print "%s" % aString

        phedex = eval( aString.replace( "null", "None" ), {}, {} )
        
        blocks = phedex[ 'phedex' ][ 'block' ]
        if len( blocks ) == 0:
            print "PhEDExNotifier: Found no blocks, expected one or more"
        for block in blocks:
            self.doBlock( block[ 'name' ], fileset )

    def doBlock( self, entity, fileset ):
    
        connection = urlopen( self.nodeURL + "&block=%s" % quote( entity ) )        
        aString = connection.read()
        connection.close()

        if aString[2:8] != "phedex":
            print "PhEDExNotifier: bad string from server follows."
            print "%s" % aString

        phedex = eval( aString.replace( "null", "None" ), {}, {} )
        
        blocks = phedex[ 'phedex' ][ 'block' ]
        if len( blocks ) != 1:
            print "PhEDExNotifier: Found %d blocks, expected 1, will only consider first block" % len( blocks)

        files = blocks[0][ 'file' ]
        for file in files:
            lfn = file[ 'name' ]
            events = self.getEvents( lfn )
            (runs,lumis) = self.getRunLumi( lfn )
            fileToAdd = File( lfn, file[ 'bytes'], events, runs[0], lumis[0] )
            replicas = file[ 'replica' ]
            if len( replicas ) > 0:
                locations = []
                for replica in replicas:
                    locations.append( replica[ 'node' ] )
                fileToAdd.setLocation( locations )
                fileset.addFile( fileToAdd )

    def getEvents( self, lfn ):
        try:
            # Get the file object, I hope there is only one!
            files = self.dbsapi.listFiles( patternLFN = lfn )
            if len ( files ) != 1:
                print "LFN doesn't map to single file in DBS! lfn=%s" % lfn
            return files[0][ 'NumberOfEvents' ]

        except DbsDatabaseError,e:
            print e




    def getRunLumi( self, lfn ):
        try:
            # List all lumi sections of the file
            lumiSections = self.dbsapi.listFileLumis( lfn )
                
        except DbsDatabaseError,e:
            print e

        lumiSecNumbers = []
        runNumbers = []
        # if there is no information set it to -1 for now
        # till WMBSFile takes a list of runs and lumi sections
        if len( lumiSections ) == 0:
            lumiSecNumbers.append( -1 )
            runNumbers.append( -1 )
        else:
            for lumiSection in lumiSections:
                lumiSecNumbers.append( lumiSection[ 'LumiSectionNumber' ] )
                runNumbers.append( lumiSection[ 'RunNumber' ] )

        return (runNumbers, lumiSecNumbers )
