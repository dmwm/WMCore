#!/usr/bin/env python
"""
_PhEDExNotifierComponent_

ProdAgent Component to notify clients of new transfers

"""
__all__ = []
__revision__ = "$Id: PhEDExNotifierComponent.py,v 1.7 2008/07/23 11:32:57 gowdy Exp $"
__version__ = "$Revision: 1.7 $"

import logging

from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

from urllib import urlretrieve
from urllib import urlopen

class PhEDExNotifierComponent(FeederImpl):
    """
    _PhEDExNotifierComponent_

    """
    
    def __init__( self, nodes, baseURL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas" ):
        
        # Add the node specification the URL
        nodeList = self.makelist( nodes )
        for node in nodeList:
            # for the first node need to be a bit different
            if node == nodeList[0]:
                self.nodeURL = baseURL + "?node=%s" % node
            else:
                self.nodeURL += "&node=%s" % node
                
            
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
    
        connection = urlopen( self.nodeURL + "&block=%s" % entity.replace( "#", "%23" ) )        
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
            fileToAdd = File( file[ 'name' ], file[ 'bytes'] )
            replicas = file[ 'replica' ]
            if len( replicas ) > 0:
                locations = []
                for replica in replicas:
                    locations.append( replica[ 'node' ] )
                fileToAdd.setLocation( locations )
                fileset.addFile( fileToAdd )
