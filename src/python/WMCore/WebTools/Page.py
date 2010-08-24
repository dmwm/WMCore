#!/usr/bin/env python

__revision__ = "$Id: Page.py,v 1.11 2009/02/02 12:21:14 metson Exp $"
__version__ = "$Revision: 1.11 $"

import urllib
import cherrypy
from cherrypy import log as cplog
from cherrypy import request
from Cheetah.Template import Template
from simplejson import JSONEncoder
import logging, os, types
import datetime, time
class Page(object):
    """
    __Page__
    
    Page is a base class that holds a configuration
    """
    def __init__(self, config = {}):
        #Config is a WMCore.Configuration
        self.config = config
    
    def warning(self, msg):
        self.log(msg, logging.WARNING)
        
    def exception(self, msg):
        self.log(msg, logging.ERROR)
        
    def debug(self, msg):
        self.log(msg, logging.DEBUG)
    
    def info(self, msg):
        self.log(msg, logging.INFO)
    
    def log(self, msg, severity):
        cplog(msg, context=self.config.application, 
                severity=severity, traceback=False)
        
class TemplatedPage(Page):
    """
    __TemplatedPage__
    
    TemplatedPage is a class that provides simple Cheetah templating
    """
    def __init__(self, config):
        Page.__init__(self, config)
        
        self.templatedir = ''
        if hasattr(self.config, 'templates'):
            self.templatedir = self.config.templates
        else:
            # Take a guess
            self.templatedir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'Templates')
        self.debug("templates are located in: %s" % self.templatedir)
        
    def templatepage(self, file=None, *args, **kwargs):
        searchList=[]
        if len(args) > 0:
            searchList.append(args)
        if len(kwargs) > 0:
            searchList.append(kwargs)
        templatefile = "%s/%s.tmpl" % (self.templatedir, file)
        if os.path.exists(templatefile):
            template = Template(file=templatefile, searchList=searchList)
            return template.respond()
        else:
            self.warning("%s not found at %s" % (file, self.templatedir))
            return "Template %s not known" % file

class SecuredPage(Page):
    def authenticate(self):
        pass

    def authenticateviahn(self):
        userdn = ""
        return userdn

    def authenticateviacert(self):
        userdn = ""
        try:
            userdn  = cherrypy.request.headers['Cms-Client-S-Dn']
            access  = cherrypy.request.headers['Cms-Auth-Status']
            if  userdn != '(null)' and access == 'OK':
                self.debug("Found user cert")
        except:
            self.debug("No cert found in a browser")
        return userdn

    
def exposexml (func):
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/xml"
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposedasxml (func):
    """
    This will prepend the DAS header to the data and calculate the checksum of 
    the data to set the etag correctly
    
    TODO: pass in the call_time value, can we get this in a smart/neat way?
    TODO: include the request_version in the data hash - a new version should 
    result in an update in a cache
    TODO: "inherit" from the exposejson
    """
    def wrapper (self, *args, **kwds):
        itime = time.time()
        data  = func (self, *args, **kwds)
        ctime = time.time()-itime
        now   = time.mktime(datetime.datetime.utcnow().timetuple())
        url   = request.base + request.path_info + request.query_string
        url   = urllib.quote(url)
        ver   = 123
        call  = func.__name__
        header = """<?xml version='1.0' standalone='yes'?>
<das request_timestamp="%s" 
     request_url="%s" 
     request_version="%s" 
     request_call="%s" 
     call_time="%s">""" % (now, url, ver, call, ctime)
        cherrypy.response.headers['ETag'] = data.__str__().__hash__()
        cherrypy.response.headers['Content-Type'] = "application/xml"
        xmldata = header + data + "</das>"
        return xmldata
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposetext (func):
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposejson (func):
    def wrapper (self, *args, **kwds):
        encoder = JSONEncoder()
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/json"
        try:
            jsondata = encoder.iterencode(data)
            return jsondata
        except:
            Exception("Fail to jsontify obj '%s' type '%s'" % (data, type(data)))
#        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposedasjson (func):
    """
    This will prepend the DAS header to the data and calculate the checksum of 
    the data to set the etag correctly
    
    TODO: pass in the call_time value, can we get this in a smart/neat way?
    TODO: include the request_version in the data hash - a new version should 
    result in an update in a cache
    TODO: "inherit" from the exposejson
    """
    def wrapper (self, *args, **kwds):
        encoder = JSONEncoder()
        data = func (self, *args, **kwds)
        now = time.mktime(datetime.datetime.utcnow().timetuple())
        dasdata = {self.config.application:{
                        'request_timestamp': now,
                        'request_url': request.base + request.path_info + \
                                                    request.query_string,
                        'request_version': 123,
                        'request_call': func.__name__,
                        'call_time': 0,
                        func.__name__: data
                        }
                   }
        cherrypy.response.headers['ETag'] = data.__str__().__hash__()
        cherrypy.response.headers['Content-Type'] = "application/json"
        try:
            jsondata = encoder.iterencode(dasdata)
            return jsondata
        except:
            Exception("Fail to jsontify obj '%s' type '%s'" % (data, type(data)))

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposejs (func):
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/javascript"
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposecss (func):
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/css"
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper
