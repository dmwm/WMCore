#!/usr/bin/env python
"""
_PhEDExNotifierComponent_

ProdAgent Component to notify clients of new transfers

"""
__all__ = []
__revision__ = "$Id: PhEDExNotifierComponent.py,v 1.8 2008/07/23 14:49:42 gowdy Exp $"
__version__ = "$Revision: 1.8 $"

import logging

from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

from urllib2 import urlopen
from urllib import quote

class PhEDExNotifierComponent(FeederImpl):
    """
    _PhEDExNotifierComponent_

    """
    
    def __init__( self, nodes, phedexURL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas", dbsURL = "https://cmsweb.cern.ch/dbs_discovery/aSearch?dbsInst=cms_dbs_prod_global&html=0&caseSensitive=on&_idx=0&pagerStep=10&xml=0&details=0&cff=0&method=dd&userInput=", dbsQuery = "find %s where file=%s" ):
        
        # Add the node specification the URL
        nodeList = self.makelist( nodes )
        for node in nodeList:
            # for the first node need to be a bit different
            if node == nodeList[0]:
                self.nodeURL = phedexURL + "?node=%s" % node
            else:
                self.nodeURL += "&node=%s" % node
        self.dbsURL = dbsURL
        self.dbsQuery = dbsQuery

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
            fileToAdd = File( lfn, file[ 'bytes'], events )
            replicas = file[ 'replica' ]
            if len( replicas ) > 0:
                locations = []
                for replica in replicas:
                    locations.append( replica[ 'node' ] )
                fileToAdd.setLocation( locations )
                fileset.addFile( fileToAdd )

    def getEvents( self, lfn ):
        query = self.dbsQuery % ( "file.numevents", lfn )
        return self.doQuery( quote( query ) )

    def doQuery(self, query ):
        connection = urlopen( self.dbsURL + query )
        aString = connection.read()
        connection.close()
        
        # now we need to get the actual result out of the text
        lines = aString.splitlines()
        answer = lines[3].strip()

        return answer
    
