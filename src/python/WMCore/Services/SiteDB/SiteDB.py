#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""

__revision__ = "$Id: SiteDB.py,v 1.2 2008/09/18 15:30:00 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Services.JSONParser import JSONParser
import urllib

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
        file = 'dnUserName_%s.json' % str(dn.__hash__()) 
        userinfo = self.parser.getJSON("dnUserName", dn=dn, file=file)
        userName = userinfo['user']
        return userName


    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name to list of CEs
        """
        file = 'cmsNametoCE_%s.json' % cmsName
        ceList = self.cmsNametoList(cmsName, 'CE', file=file)
        return ceList


    def cmsNametoSE(self, cmsName):
        """
        Convert CMS name to list of SEs
        """
        file = 'cmsNametoSE_%s.json' % cmsName
        seList = self.cmsNametoList(cmsName, 'SE', file=file)
        return seList


    def cmsNametoList(self, cmsName, kind, file):
        """
        Convert CMS name to list of CEs or SEs
        """
        
        cmsName = cmsName.replace('*','%')
        cmsName = cmsName.replace('?','_')
        theInfo = self.parser.getJSON("CMSNameto"+kind, file=file, name=cmsName)
    
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
          mySiteDB.parser.getJSON("CEtoCMSName", 
                                  file="CEtoCMSName", 
                                  name="red.unl.edu")
          
    print "T1 Site Exec's:", \
          mySiteDB.parser.getJSON("CMSNametoAdmins", 
                                  file="CMSNametoAdmins", 
                                  name="T1",
                                  role="Site Executive")
    print "Tier 1 CEs:", mySiteDB.cmsNametoCE("T1")
    print "Tier 1 SEs:", mySiteDB.cmsNametoSE("T1")
