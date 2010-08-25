#!/usr/bin/env python
"""
_SAM_

Talk to the SAM Service to get site status from the results of SAM tests.
"""

__revision__ = "$Id: SAM.py,v 1.2 2009/07/07 18:56:43 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from BeautifulSoup import BeautifulSoup
from WMCore.Services.AuthorisedService import AuthorisedService

class SAM(AuthorisedService):
    def getCMSSWInstalls(self, ce=None):
        """
        Contact the SAM test page and get the tested, installed software on a CE.
        TODO: improved error handling
        """
        url = "funct=TestResultLatest&nodename=%s&vo=cms&testname=CE-cms-swinst" % ce
        file = 'sam_cmssw_inst_%s.html' % ce
        pageSoup = BeautifulSoup(self.refreshCache(file, url))
        
        swlists = pageSoup.findAll('ol', { "class" : "CMSSW-list"})
        self.logger.debug(swlists)
        
        software = {}
        for tag in swlists:
            children=[]
            for i in tag.contents:
                child = i.string
                if child != '\n' and child != 'error':
                    children.append(child)
                
            children.sort()
            children.reverse()
            software[tag['id']] = children
        return software
        
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='/tmp/sam/samstatus.log',
                    filemode='w')
    
    dict = {}
    dict['endpoint'] = 'https://lcg-sam.cern.ch:8443/sam/sam.py?'
    dict['cachepath'] = '/tmp/sam'
    dict['type'] = 'text/json'
    dict['logger'] = logging.getLogger('SAMtest')
    
    sam = SAM(dict = dict)
    print sam.getCMSSWInstalls(ce='lcgce02.gridpp.rl.ac.uk')
