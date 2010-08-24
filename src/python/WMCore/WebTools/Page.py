#!/usr/bin/env python

__revision__ = "$Id: Page.py,v 1.1 2009/01/07 11:56:51 metson Exp $"
__version__ = "$Revision: 1.1 $"

from cherrypy import log
from Cheetah.Template import Template
import logging, os

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
    
    