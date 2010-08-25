#!/usr/bin/env python

__revision__ = "$Id: Page.py,v 1.30 2009/06/08 20:19:05 valya Exp $"
__version__ = "$Revision: 1.30 $"

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

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.DBFactory import DBFactory
from WMCore.DataStructs.WMObject import WMObject

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

class DatabasePage(TemplatedPage, DBFormatter):
    def __init__(self, config = {}):
        """
        __DatabasePage__

        A page with a database connection (a WMCore.Database.DBFormatter) held
        in self.dbi. Look at the DBFormatter class for other handy helper
        methods, such as getBinds and formatDict.

        The DBFormatter class was originally intended to be extensively
        sub-classed, such that it's subclasses followed the DAO pattern. For web
        tools we do not generally do this, and you will normally access the
        database interface directly:

        binds = {'id': 123}
        sql = "select * from table where id = :id"
        result = self.dbi.processData(sql, binds)
        return self.formatDict(result)

        Although following the DAO pattern is still possible and encouraged
        where appropriate. However, if you want to use the DAO pattern it may be
        better to *not* expose the DAO classes and have a normal DatabasePage
        exposed that passes the database connection to all the DAO's.
        """
        TemplatedPage.__init__(self, config)
        assert hasattr(self.config, 'database'), "No database configured"
        conn = DBFactory(self, self.config.database).connect()
        DBFormatter.__init__(self, self, conn)

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
                                 request = request)
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
                                 request = request)
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
        data_struct = func(self, *args, **kwds)
#        data_struct = runDas(self, func, *args, **kwds)
        plist_str = plistlib.writePlistToString(data_struct)
#        cherrypy.response.headers['ETag'] = das['results'].__str__().__hash__()
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

        cherrypy.response.headers['ETag'] = das['results'].__str__().__hash__()
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
        cherrypy.response.headers['ETag'] = data.__str__().__hash__()
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
        cherrypy.response.headers['ETag'] = data.__str__().__hash__()
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
    results    = func(self, *args, **kwds)
    call_time  = time.time() - start_time
    if  type(results) is types.ListType:
        row = results[0]
    else:
        row = results
    if  type(row) is types.StringType:
        row = eval(row)
#    if  type(results) is types.StringType:
#        results = eval(results)
#    if  type(row) is not types.DictType:
#        data  = str(results)
#        dtype = type(results)
#        raise Exception("Unsupported data format '%s' data type '%s'" % (data, dtype))
    if  type(row) is types.DictType and row.has_key('expire'):
        res_expire = row['expire']
    else:
        res_expire = 60*5 # 5 minutes
    if  type(row) is types.DictType and row.has_key('version'):
        res_version = row['version']
    else:
        res_version = 'unknown'
    try:
        keyhash = hashlib.md5()
    except:
        # prior python 2.5
        keyhash = md5.new()

    keyhash.update(str(results))
    res_checksum = keyhash.hexdigest()
    dasdata = {'application':'%s.%s' % (self.config.application, func.__name__),
               'request_timestamp': start_time,
               'request_url': request.base + request.path_info + \
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
#    dasdata.update(func(self, *args, **kwds))
    return dasdata

