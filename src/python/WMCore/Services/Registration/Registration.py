#!/usr/bin/env python
'''
A very simple interface to the Registration service. All we need to do here is 
send a JSON encoded dictionary periodically (hourly) to the service. To achieve
this do something like:

reg_info ={
    "url": "https://globaldbs",
    "admin": "joe.bloggs@cern.ch",
    "type": "DBS",
    "name": "Global DBS",
    "timeout": 2
}

reg = Registration({"inputdata": reg_info})

this will create a Registration object with all the relevant registration 
information (_id, admin, type, name, and timeout are the minimal set, you could 
add more such as description, configuration files etc should your app need it). 

Once instatiated you will then want to poll it hourly like so:

reg.refreshCache()

This will push the configuration up to the Registration service
'''
from WMCore.Services.Service import Service 
from WMCore.Services.Requests import BasicAuthJSONRequests
import datetime
import logging
from httplib import HTTPException

class Registration(Service):
    def __init__(self, dict):
        defaultdict = {'endpoint': "https://cmsweb.cern.ch/registration/",
                       'cacheduration': 1,
                       'cachepath': '/tmp'}
        defaultdict.update(dict)
        defaultdict["method"] = 'PUT'
        defaultdict["content_type"] = "application/json"
        defaultdict['requests'] = BasicAuthJSONRequests
        defaultdict['req_cache_path'] = defaultdict['cachepath']
        
        if 'logger' not in defaultdict.keys():
            logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt = '%m-%d %H:%M',
                    filename = defaultdict['cachepath'] + '/regsvc.log',
                    filemode = 'w')
            defaultdict['logger'] = logging.getLogger('RegService')

        Service.__init__(self, defaultdict)
        # Set correct internal state
        cache = 'regsvc'
        url = '/regsvc/%s' % self['inputdata']['url'].__hash__()
        try:
            data = Service.forceRefresh(self, cache, url, 
                                    verb = 'GET', decoder=False).read()
            # Decode the data from json
            data = self['requests'].decode(data)
            # Update internal state to get the revision
            self['inputdata']['_rev'] = data['_rev']
            print data['_rev']

        except HTTPException, he:
            # If the document is not found (404) we can refresh the cache to 
            # create it. Other statuses should be raised for handling higher up
            if he.status == 404:
                self.refreshCache()
            else:
                raise he
                
        
    def refreshCache(self, inputdata = {}):
        # It's possible that the data has changed, for instance a change in admin
        self['inputdata'].update(inputdata)
        # But we want to set the timestamp explicitly
        self['inputdata']['timestamp'] = str(datetime.datetime.now())
        
        cache = 'regsvc'
        url = '/regsvc/%s' % self['inputdata']['url'].__hash__()
        # Talk to the RegSvc, read the data but don't decode the response
        data = Service.refreshCache(self, cache, url, 
                                    verb = 'PUT', decoder=False).read()
        
        # Decode the data from json
        data = self['requests'].decode(data)
        # Update internal state to get the revision
        self['inputdata']['_rev'] = data['rev']

