#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""

__revision__ = "$Id: SiteDB.py,v 1.1 2008/08/12 21:47:19 ewv Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Services.JSONParser import JSONParser

class SiteDBJSON:

    """
    API for dealing with retrieving information from SiteDB
    """

    def __init__(self):
        siteDBURL = "https://cmsweb.cern.ch/sitedb/json/index/"
        self.parser = JSONParser.JSONParser(siteDBURL)


    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name
        """
        
        userinfo = self.parser.getJSON("dnUserName", dn=dn)
        userName = userinfo['user']
        return userName


    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name to list of CEs
        """
        
        ceList = self.cmsNametoList(cmsName, 'CE')
        return ceList


    def cmsNametoSE(self, cmsName):
        """
        Convert CMS name to list of SEs
        """
        
        seList = self.cmsNametoList(cmsName, 'SE')
        return seList


    def cmsNametoList(self, cmsName, kind):
        """
        Convert CMS name to list of CEs or SEs
        """
        
        cmsName = cmsName.replace('*','%')
        cmsName = cmsName.replace('?','_')
        theInfo = self.parser.getJSON("CMSNameto"+kind, name=cmsName)
    
        theList = []
        for index in theInfo:
            try:
                item = theInfo[index]['name']
                if item:
                    theList.append(item)
            except KeyError:
                pass

        return theList



if __name__ == '__main__':

    mySiteDB = SiteDBJSON()

    print "Username for Simon Metson:", \
          mySiteDB.dnUserName(dn="/C=UK/O=eScience/OU=Bristol/L=IS/CN=simon metson")

    print "CMS name for UNL:", \
          mySiteDB.parser.getJSON("CEtoCMSName", name="red.unl.edu")
    print "Tier 1 CEs:", mySiteDB.cmsNametoCE("T1")
    print "Tier 1 SEs:", mySiteDB.cmsNametoSE("T1")

