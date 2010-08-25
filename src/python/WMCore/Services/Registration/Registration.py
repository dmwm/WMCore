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

@author: metson
'''
from WMCore.Services.AuthorisedService import AuthorisedService #Service as 
from WMCore.Services.Requests import JSONRequests
import datetime
import logging

class Registration(JSONRequests, AuthorisedService):
    def __init__(self, dict):
        defaultdict = {'endpoint': "cmsweb.cern.ch/registration/",
                       'cacheduration': 1,
                       'cachepath': '/tmp'}
        defaultdict.update(dict)
        defaultdict["method"] = 'PUT'
        if 'logger' not in defaultdict.keys():
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=defaultdict['cachepath'] + '/regsvc.log',
                    filemode='w')
            defaultdict['logger'] = logging.getLogger('RegService')

        AuthorisedService.__init__(self, defaultdict)
        JSONRequests.__init__(self, defaultdict['endpoint'])
        
    def refreshCache(self):
        self['inputdata']['timestamp'] = str(datetime.datetime.now())
        return AuthorisedService.refreshCache(self, 
                                       'regsvc', 
                                       self['inputdata']['url'].__hash__())