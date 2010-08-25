#!/usr/bin/env python
"""
_Dashboard_

Talk to the Dashboard Service to get site status.
"""

__revision__ = "$Id: Dashboard.py,v 1.4 2010/01/08 21:38:04 metson Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Services.Service import Service
class Dashboard(Service):
    """
    Dashboard provides a service that gives a site's status. A site can be in 
    the follow states: "MAINTENANCE", "ERROR", "WARNING", "UP"
    """
    def __init__(self, dict={}):
        dict['accept_type'] = 'text/csv'
        Service.__init__(dict)
    
    def getStatus(self, name):
        summaryfile = "db_sam_summary_%s.csv" % (name)
        self['logger'].debug('writing to %s/%s for site %s' % (self['cachepath'], 
                                                            summaryfile, 
                                                            name))
        
        url = '/latestresults?sites=%s' % (name)
        summaryfile = self.refreshCache(summaryfile, url)
        
        status = []
        for l in summaryfile.readlines():
            status.append(l.strip().rsplit(',',1)[1])
        self['logger'].debug(status)
        for i in ["MAINTENANCE", "ERROR", "WARNING", "UP"]:
            if i in status:
                 return {'status':i, 'id':name}
        return {'status':'UNKNOWN', 'id':name}
    
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='/tmp/sam/samstatus.log',
                    filemode='w')
    
    dict = {}
    dict['endpoint'] = 'http://lxarda16.cern.ch/dashboard/request.py'
    dict['cachepath'] = '/tmp/dashboard'
    dict['accept_type'] = 'text/csv'
    dict['logger'] = logging.getLogger('SAMtest')
    
    dashboard = Dashboard(dict = dict)
    print dashboard.getStatus(name='T1_UK_RAL')    