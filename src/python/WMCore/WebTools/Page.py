#!/usr/bin/env python

__revision__ = "$Id: Page.py,v 1.39 2010/02/01 18:02:30 sryu Exp $"
__version__ = "$Revision: 1.39 $"

import urllib
import cherrypy
from cherrypy import log as cplog
from cherrypy import request
from Cheetah.Template import Template
from Cheetah import Version
try:
    from json import JSONEncoder
except:
    # Prior python 2.6 json comes from simplejson
    from simplejson import JSONEncoder

try:
    # with python 2.5
    import hashlib
except:
    # prior python 2.5
    import md5

import logging, os, types
import time
from datetime import datetime, timedelta
import traceback

from WMCore.DataStructs.WMObject import WMObject
from WMCore.WMFactory import WMFactory

from wsgiref.handlers import format_date_time
from datetime import datetime, timedelta
from time import mktime
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

DEFAULT_EXPIRE = 5*60

class Page(WMObject):
    """
    __Page__

    Page is a base class that holds a configuration
    """
    def warning(self, msg):
        if  msg:
            self.log(msg, logging.WARNING)

    def exception(self, msg):
        if  msg:
            self.log(msg, logging.ERROR)

    def debug(self, msg):
        if  msg:
            self.log(msg, logging.DEBUG)

    def info(self, msg):
        if  msg:
            self.log(msg, logging.INFO)

    def log(self, msg, severity):
        if type(msg) != str:
            msg = str(msg)
        if  msg:
            cplog(msg, context=self.config.application,
                    severity=severity, traceback=False)

class TemplatedPage(Page):
    """
    __TemplatedPage__

    TemplatedPage is a class that provides simple Cheetah templating
    """
    def __init__(self, config = {}):
        Page.__init__(self, config)
        self.templatedir = ''
        if hasattr(self.config, 'templates'):
            self.templatedir = self.config.templates
        else:
            # Take a guess
            self.templatedir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'Templates')
        self.debug("Templates are located in: %s" % self.templatedir)
        self.debug("Using Cheetah version: %s" % Version)

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

def exposeatom (func):
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/atom+xml"
        return self.templatepage('Atom', data = data,
                                 config = self.config,
                                 path = request.path_info)
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposexml (func):
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/xml"
        return self.templatepage('XML', data = data,
                                 config = self.config,
                                 path = request.path_info)
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposedasplist (func):
    """
    Return data in XML plist format, 
    see http://docs.python.org/library/plistlib.html#module-plistlib
    """
    def wrapper (self, *args, **kwds):
        import plistlib
        data_struct = runDas(self, func, *args, **kwds)
        plist_str = plistlib.writePlistToString(data_struct)
        cherrypy.response.headers['ETag'] = data_struct['results'].__str__().__hash__()
        cherrypy.response.headers['Content-Type'] = "application/xml"
        return plist_str
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
    TODO: "inherit" from the exposexml
    """
    def wrapper (self, *args, **kwds):
        das = runDas(self, func, *args, **kwds)
        header = "<?xml version='1.0' standalone='yes'?>"
        keys = das.keys()
        keys.remove('results')
        string = ''
        for key in keys:
            string = '%s %s="%s"' % (string, key, das[key])
        header = "%s\n<das %s>" % (header, string)

        cherrypy.response.headers['Content-Type'] = "application/xml"
        xmldata = header + das['results'].__str__() + "</das>"
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
#            jsondata = encoder.iterencode(data)
            jsondata = encoder.encode(data)
            return jsondata
        except:
            Exception("Fail to jsontify obj '%s' type '%s'" % (data, type(data)))
#        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposejsonthunker (func):
    def wrapper (self, *args, **kwds):
        thunker = JSONThunker()
        encoder = JSONEncoder()
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/json+thunk"
        try:
            data = thunker.thunk(data)
            jsondata = encoder.encode(data)
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
        data = runDas(self, func, *args, **kwds)
        
        cherrypy.response.headers['Content-Type'] = "application/json"
        try:
#            jsondata = encoder.iterencode(data)
            jsondata = encoder.encode(data)
            return jsondata
        except:
            Exception("Failed to json-ify obj '%s' type '%s'" % (data, type(data)))

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

def runDas(self, func, *args, **kwds):
    """
    Run a query and produce a dictionary for DAS formatting
    """
    start_time = time.time()
    expires = kwds.pop('expires', DEFAULT_EXPIRE)
    results    = func(self, *args, **kwds)
    call_time  = time.time() - start_time
    res_expire = make_timestamp(expires)
    if  type(results) is types.ListType:
        if len(results) > 0:
            row = results[0]
        else:
            row = None
    else:
        row = results
    if  type(row) is types.StringType:
        row = '"%s"' % row
    try:
        factory = WMFactory('webtools_factory')
        object  = factory.loadObject(self.config.model.object, self.config)
        res_version = object.version
    except:
        res_version = 'unknown'
#        msg  = traceback.format_exc()
#        msg += '\nThe application %s does not have version member data. '\
#        % self.config.application
#        msg += 'Unable to set the version.'
#        raise Exception(msg)

    try:
        keyhash = hashlib.md5()
    except:
        keyhash = md5.new() # prior python 2.5

    keyhash.update(str(results))
    res_checksum = keyhash.hexdigest()
    dasdata = {'application':'%s.%s' % (self.config.application, func.__name__),
               'request_timestamp': start_time,
               'request_url': request.base + request.path_info + '?' + \
                                            request.query_string,
               'request_method' : request.method,
               'request_params' : request.params,
               'response_version': res_version,
               'response_expires': res_expire,
               'response_checksum': res_checksum,
               'request_call': func.__name__,
               'call_time': call_time,
               'results': results,
              }
    return dasdata

def make_timestamp(seconds=0):
    then = datetime.now() + timedelta(seconds=seconds)
    return mktime(then.timetuple())

def make_rfc_timestamp(seconds=0):
    return format_date_time(make_timestamp(seconds))
