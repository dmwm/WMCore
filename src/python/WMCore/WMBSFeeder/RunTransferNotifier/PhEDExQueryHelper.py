#!/usr/bin/env python

from urllib import urlopen
from urllib import quote

class PhEDExQueryHelper:
    """
    Contains methods to query PhEDEx for file / site information
    """
    def __init__(self, url):
        """
        Initisalises the query helper
        """
        self.phedexUrl = url
        
    def getPhEDExBlockFiles(self, block):
        """
        Queries PhEDEx to get all files in a block
        """
        connection = urlopen(self.phedexUrl + "&block=%s" % quote(block))        
        aString = connection.read()
        connection.close()

        if aString[2:8] != "phedex":
            print "RunTransferNotifier: bad string from server follows."
            print "%s" % aString

        phedex = eval(aString.replace( "null", "None" ), {}, {})
        
        blocks = phedex['phedex']['block']
        if len(blocks) != 1:
            print "PhEDExNotifier: Found %d blocks, will only use first" % \
                len(blocks)

        return blocks[0]['file']
    
    def getCompleteSites(self, blocks):
        """
        Queries PhEDEx to determine sites where all listed blocks are present
        """
        pass