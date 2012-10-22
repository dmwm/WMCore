#!/usr/bin/env python
"""
_Dashboard_

Talk to the Dashboard Service to get site status.
"""




from WMCore.Services.Service import Service
class Dashboard(Service):
    """
    Dashboard provides a service that gives a site's status. A site can be in
    the follow states: "MAINTENANCE", "ERROR", "WARNING", "UP"
    """
    def __init__(self, dict={}):
        dict['accept_type'] = 'text/csv'
        dict['method'] = 'GET'
        Service.__init__(self, dict)

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
                return {'status':i, 'cms_name':name}
        return {'status':'UNKNOWN', 'cms_name':name}
