#!/usr/bin/env python
"""
_PhEDExNotifierComponent_

ProdAgent Component to notify clients of new transfers

"""
__all__ = []
__revision__ = "$Id: PhEDExNotifierComponent.py,v 1.6 2008/07/23 10:37:18 gowdy Exp $"
__version__ = "$Revision: 1.6 $"

import logging

from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

from urllib import urlretrieve
from urllib import urlopen

class PhEDExNotifierComponent(FeederImpl):
    """
    _PhEDExNotifierComponent_

    """
    
    def __init__( self, nodes, baseURL ):
        
        self.nodes = self.makelist( nodes )
        if baseURL == None:
            self.baseURL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas"
        else
            self.baseURL = baseURL
        
            
        # Get current state from PhEDEx or local file if it exists
        baseFileName = self.args['ComponentDir'] + "/base.json"
        if not os.path.isfile( baseFileName ):
            logging.debug("PhEDExNotifier: downloading base.json file")
            urlretrieve( self.baseURL, baseFileName )
        f = open( baseFileName )
        self.base = eval( f.read(), {}, {} )
        f.close()
        phedex = self.base[ 'phedex' ]


    def __call__(self, fileset):
        """
        _operator(message, payload)_

        Respond to messages from the message service

        """

        # Add the node specification the URL
        for node in self.nodes:
            # for the first node need to be a bit different
            if node == self.nodes[0]:
                nodeURL = self.baseURL + "?node=%s" % node
            else:
                nodeURL += "&node=%s" % node
                
        entity = fileset.name

        # determine if we have a block or dataset
        if entity.find("#") != -1:
            doBlock( entity, fileset, nodeURL )
        else:
            doDataset( entity, fileset, nodeURL )


    def doDataset( entity, fileset, nodeURL ):

        # need to adjust the URL to get the list of blocks
        datasetURL = nodeURL,replace( "fileReplicas", "blockReplicas" )

        connection = urlopen( datasetURL + "&block=%s%s" % (entity,"*") )
        aString = connection.read()
        connection.close()

        if aString[2:8] != "phedex":
            logging.debug( "PhEDExNotifier: bad updated string from server follows." )
            logging.debug( "%s" % aString )

        phedex = eval( aString, {}, {} )
        
        blocks = phedex[ 'phedex' ][ 'block' ]
        if len( blocks ) == 0:
            logging.debug( "PhEDExNotifier: Found no blocks, expected one or more" )
        for block in blocks:
            doBlock( block.name, fileset, nodeURL )

    def doBlock( entity, fileset, nodeURL ):
    
        connection = urlopen( nodeURL + "&block=%s" % entity )
        aString = connection.read()
        connection.close()

        if aString[2:8] != "phedex":
            logging.debug( "PhEDExNotifier: bad updated string from server follows." )
            logging.debug( "%s" % aString )

        phedex = eval( aString, {}, {} )
        
        blocks = phedex[ 'phedex' ][ 'block' ]
        if len( blocks ) != 1:
            logging.debug( "PhEDExNotifier: Found %d blocks, expected 1, will only consider first block" % len( updatedBlocks) )

        files = blocks[0]
        for file in files:
            fileToAdd = File( file.name, file.bytes )
            replicas = file[ 'replica' ]
            if len( replicas ) > 0:
                locations = []
                for replica in replicas:
                    locations.append( replica.node )
                fileToAdd.setlocation( locations )
                fileset.addFile( fileToAdd )
