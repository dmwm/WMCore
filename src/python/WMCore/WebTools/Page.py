#!/usr/bin/env python

__revision__ = "$Id: Page.py,v 1.2 2009/01/09 16:31:22 valya Exp $"
__version__ = "$Revision: 1.2 $"

import cherrypy
from cherrypy import log
from Cheetah.Template import Template
from simplejson import JSONEncoder
import logging, os, types

class Page(object):
    """
    __Page__
    
    Page is a base class that holds a configuration
    """
    def __init__(self, config):
        self.config = config
        self.app = self.config[0]['root']['application']
        
class TemplatedPage(Page):
    """
    __TemplatedPage__
    
    TemplatedPage is a class that provides simple Cheetah templating
    """
    def __init__(self, config):
        Page.__init__(self, config)
        
        self.templatedir = ''
        if 'templates' in self.config[0].keys():
            self.templatedir = self.config[0]['templates']
        elif 'templates' in self.config[0]['root'].keys():  
            self.templatedir = self.config[0]['root']['templates']
        else:
            # Take a guess
            self.templatedir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'Templates')
            
        log("templates are located in: %s" % self.templatedir, context=self.app, 
            severity=logging.DEBUG, traceback=False)
        
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
            log("%s not found at %s" % (file, self.templatedir), 
                        context=self.app, severity=logging.WARNING)
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
                log("Found user cert", context=self.app, 
                    severity=logging.DEBUG, traceback=False)
        except:
            log("No cert found in a browser", context=self.app, 
                severity=logging.DEBUG, traceback=False)
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
