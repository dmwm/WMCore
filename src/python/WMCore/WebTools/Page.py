#!/usr/bin/env python
"""
Some generic base classes for building web pages with.
"""

from builtins import str, bytes

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from time import mktime
from wsgiref.handlers import format_date_time

import cherrypy
from Cheetah import Version
from Cheetah.Template import Template
from cherrypy import log as cplog
from cherrypy import request

from Utils.PythonVersion import PY3
from Utils.Utilities import encodeUnicodeToBytesConditional
from WMCore.DataStructs.WMObject import WMObject
from WMCore.WMFactory import WMFactory
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

DEFAULT_EXPIRE = 5 * 60


class Page(WMObject):
    """
    __Page__

    Page is a base class that holds a configuration and provides a logger.
    """

    def warning(self, msg=None):
        """
        Log a warning
        """
        self.log(msg, logging.WARNING)

    def exception(self, msg=None):
        """
        Log an exception
        """
        self.log(msg, logging.ERROR)

    def error(self, msg=None):
        """
        Log an error
        """
        self.log(msg, logging.ERROR)

    def debug(self, msg=None):
        """
        Log a debug statement
        """
        self.log(msg, logging.DEBUG)

    def info(self, msg=None):
        """
        Log some info
        """
        self.log(msg, logging.INFO)

    def log(self, msg=None, severity=logging.INFO):
        """
        Do the logging using the CherryPy logger
        """

        if not isinstance(msg, str):
            msg = str(msg)
        if msg:
            cplog.error_log.log(severity, msg)


class TemplatedPage(Page):
    """
    __TemplatedPage__

    TemplatedPage is a class that provides simple Cheetah templating
    """

    def __init__(self, config={}):
        """
        Configure the Page base class then add in the location of the templates. If
        this is not specified in the configuration take a guess based on the location
        of the file.
        """
        Page.__init__(self, config)
        self.templatedir = ''
        if hasattr(self.config, 'templates'):
            self.templatedir = self.config.templates
        else:
            # Take a guess
            self.warning("Configuration doesn't specify template location, guessing %s" % self.templatedir)
            self.templatedir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'Templates')

        self.debug("Templates are located in: %s" % self.templatedir)
        self.debug("Using Cheetah version: %s" % Version)

    def templatepage(self, file=None, *args, **kwargs):
        """
        Apply the cheetah template specified by file to the data in arg and kwargs.
        The templates are compiled in memory and results do not get written to the
        filesystem. The templates do not need to be compiled. Templates are located in
        self.templatedir - this is specified in the configuration section 'templates'.

        You should use cgi.escape to escape data going into the template if you are unsure
        of it's provenance.
        """
        searchList = []
        if len(args) > 0:
            searchList.append(args)
        if len(kwargs) > 0:
            searchList.append(kwargs)
        templatefile = "%s/%s.tmpl" % (self.templatedir, file)
        if os.path.exists(templatefile):
            template = Template(file=templatefile, searchList=searchList)
            return template.respond()
        else:
            self.warning("Template %s not found at %s" % (file, self.templatedir))
            return "Template for page not found"


def _setCherryPyHeaders(data, contentType, expires):
    """
    Convenience function to set headers appropriately
    """
    cherrypy.response.headers['Content-Type'] = contentType
    if data:
        cherrypy.response.headers['Content-Length'] = len(data)
    else:
        cherrypy.response.headers['Content-Length'] = 0
    cherrypy.lib.caching.expires(secs=expires, force=True)
    # TODO: find a better way to generate Etag
    cherrypy.response.headers['ETag'] = data.__str__().__hash__()


def exposeatom(func):
    """
    Convenience decorator function to expose atom XML
    """

    def wrapper(self, data, expires, contentType="application/atom+xml"):
        data = func(self, data)
        _setCherryPyHeaders(data, contentType, expires)
        return self.templatepage('Atom', data=data,
                                 config=self.config,
                                 path=request.path_info)

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposexml(func):
    """
    Convenience decorator function to expose XML
    """

    def wrapper(self, data, expires, contentType="application/xml"):
        data = func(self, data)
        _setCherryPyHeaders(data, contentType, expires)
        return self.templatepage('XML', data=data,
                                 config=self.config,
                                 path=request.path_info)

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposedasplist(func):
    """
    Convenience decorator function to expose plist XML
    see http://docs.python.org/library/plistlib.html#module-plistlib
    """

    def wrapper(self, data, expires, contentType="application/xml"):
        import plistlib
        data_struct = runDas(self, func, data, expires)
        plist_str = plistlib.writePlistToString(data_struct)
        cherrypy.response.headers['ETag'] = data_struct['results'].__str__().__hash__()
        _setCherryPyHeaders(plist_str, contentType, expires)
        return plist_str

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposedasxml(func):
    """
    Convenience decorator function to expose DAS XML

    This will prepend the DAS header to the data and calculate the checksum of
    the data to set the etag correctly

    TODO: pass in the call_time value, can we get this in a smart/neat way?
    TODO: include the request_version in the data hash - a new version should
    result in an update in a cache
    TODO: "inherit" from the exposexml
    """

    def wrapper(self, data, expires, contentType="application/xml"):
        das = runDas(self, func, data, expires)
        header = "<?xml version='1.0' standalone='yes'?>"
        keys = list(das)
        keys.remove('results')
        string = ''
        for key in keys:
            string = '%s %s="%s"' % (string, key, das[key])
        header = "%s\n<das %s>" % (header, string)
        xmldata = header + das['results'].__str__() + "</das>"
        _setCherryPyHeaders(xmldata, contentType, expires)
        return xmldata

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposetext(func):
    """
    Convenience decorator function to expose plain text
    """

    def wrapper(self, data, expires, contentType="text/plain"):
        data = func(self, data)
        data = str(data)
        _setCherryPyHeaders(data, contentType, expires)
        return data

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposejson(func):
    """
    Convenience decorator function to expose json
    """

    def wrapper(self, data, expires, contentType="application/json"):
        data = func(self, data)
        try:
            #            jsondata = encoder.iterencode(data)
            jsondata = json.dumps(data)
            _setCherryPyHeaders(jsondata, contentType, expires)
            return jsondata
        except Exception:
            raise
            # Exception("Fail to jsontify obj '%s' type '%s'" % (data, type(data)))
        #        return data

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposejsonthunker(func):
    """
    Convenience decorator function to expose thunked json
    """

    def wrapper(self, data, expires, contentType="application/json+thunk"):
        data = func(self, data)
        try:
            thunker = JSONThunker()
            data = thunker.thunk(data)
            jsondata = json.dumps(data)
            _setCherryPyHeaders(jsondata, contentType, expires)
            return jsondata
        except Exception:
            raise
            # Exception("Fail to jsontify obj '%s' type '%s'" % (data, type(data)))
        #        return data

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposedasjson(func):
    """
    Convenience decorator function to expose DAS json

    This will prepend the DAS header to the data and calculate the checksum of
    the data to set the etag correctly

    TODO: pass in the call_time value, can we get this in a smart/neat way?
    TODO: include the request_version in the data hash - a new version should
    result in an update in a cache
    TODO: "inherit" from the exposejson
    """

    def wrapper(self, data, expires, contentType="application/json"):
        data = runDas(self, func, data, expires)

        try:
            jsondata = json.dumps(data)
            _setCherryPyHeaders(jsondata, contentType, expires)
            return jsondata
        except Exception:
            raise
            # Exception("Failed to json-ify obj '%s' type '%s'" % (data, type(data)))

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposejs(func):
    """
    Convenience decorator function to expose javascript
    """

    def wrapper(self, data, expires, contentType="application/javascript"):
        data = func(self, data)
        _setCherryPyHeaders(data, contentType, expires)
        return data

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def exposecss(func):
    """
    Convenience decorator function to expose css
    """

    def wrapper(self, data, expires, contentType="text/css"):
        data = func(self, data)
        _setCherryPyHeaders(data, contentType, expires)
        return data

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper


def runDas(self, func, data, expires):
    """
    Run a query and produce a dictionary for DAS formatting
    """
    start_time = time.time()
    results = func(self, data)
    call_time = time.time() - start_time
    res_expire = make_timestamp(expires)
    if isinstance(results, list):
        if len(results) > 0:
            row = results[0]
        else:
            row = None
    else:
        row = results
    if isinstance(row, str):
        row = '"%s"' % row
    try:
        factory = WMFactory('webtools_factory')
        obj = factory.loadObject(self.config.model.object, self.config)
        res_version = obj.version
    except Exception:
        res_version = 'unknown'

    keyhash = hashlib.md5()

    if not isinstance(results, (str, bytes)):
        keyhash.update(encodeUnicodeToBytesConditional(str(results), condition=PY3))
    else:
        keyhash.update(encodeUnicodeToBytesConditional(results, condition=PY3))
    res_checksum = keyhash.hexdigest()
    dasdata = {'application': '%s.%s' % (self.config.application, func.__name__),
               'request_timestamp': start_time,
               'request_url': request.base + request.path_info + '?' + request.query_string,
               'request_method': request.method,
               'request_params': request.params,
               'response_version': res_version,
               'response_expires': res_expire,
               'response_checksum': res_checksum,
               'request_call': func.__name__,
               'call_time': call_time,
               'results': results,
               }
    return dasdata


def make_timestamp(seconds=0):
    """
    Convenience function to make a timestamp
    """
    then = datetime.now() + timedelta(seconds=seconds)
    return mktime(then.timetuple())


def make_rfc_timestamp(seconds=0):
    """
    Convenience function to make an rfc formatted timestamp
    """
    return format_date_time(make_timestamp(seconds))
