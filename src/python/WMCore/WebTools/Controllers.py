#!/usr/bin/env python
"""
__Controllers__

Controllers return java script and/or css from a static directory, after 
minimising setting appropriate headers and etags and gzip.  
"""

__revision__ = "$Id: Controllers.py,v 1.2 2009/01/20 12:09:41 metson Exp $"
__version__ = "$Revision: 1.2 $"

from cherrypy import expose, log, response
from cherrypy import config as cherryconf
# configuration and arguments
from ConfigParser import ConfigParser
from optparse import OptionParser
# Factory to load pages dynamically
from WMCore.WMFactory import WMFactory
# Logging
import WMCore.WMLogging
import logging, os, sys
from Page import Page

class Controllers(Page):
    def __init__(self, config):
        Page.__init__(self, config)
        print 
        print config
        print 
        self.dict = dict
        self.cssmap = {}
        self.jsmap = {}
        self.cache = {}
        try:
            self.cssmap = self.config['css']
        except:
            pass
        try:
            self.jsmap = self.config['js']
        except:
            pass
         
    @expose
    def index(self):
        return "style loads from <a href='css'>css</a>, javascript from <a href='js'>js</a>"
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()
    
    @expose
    def css(self, *args, **kwargs):
        """
        cat together the specified css files and return a single css include
        get css by calling: /controllers/css/file1/file2/file3
        """
        cherryconf.update ({'tools.encode.on': True, 'tools.gzip.on': True})
        
        args = list(args)
        args.insert(0, 'cms_reset')
        args.insert(0, 'reset')
        scripts = self.checkScripts(args, self.cssmap)
        id = "-".join(scripts)
        
        if id not in self.cache.keys():
            data = '@CHARSET "UTF-8";'
            for script in args:
                path = os.path.join(sys.path[0], self.cssmap[script])
                path = os.path.normpath(path)
                file = open(path)
                data = "\n".join ([data, file.read().replace('@CHARSET "UTF-8";', '')])
                file.close()
            #self.setHeaders ("text/css")
            self.cache[id] = self.minify(data)
        return self.cache[id] 
        
    @expose
    def js(self, *args, **kwargs):
        """
        cat together the specified js files and return a single js include
        get js by calling: /controllers/js/file1/file2/file3
        """
        cherryconf.update ({'tools.encode.on': True, 'tools.gzip.on': True})
        
        args = list(args)
        scripts = self.checkScripts(args, self.cssmap)
        id = "-".join(scripts)
        
        if id not in self.cache.keys():
            data = ''
            for script in args:
                path = os.path.join(sys.path[0], self.jsmap[script])
                path = os.path.normpath(path)
                file = open(path)
                data = "\n".join ([data, file.read()])
                file.close()
            #self.setHeaders ("text/js")
            self.cache[id] = data
        return self.cache[id] 
    
    def checkScripts(self, scripts, map):
        """
        Check a script is known to the map and that the script actually exists   
        """           
        for script in scripts:
            if script not in map.keys():
                log("%s not known" % script, 
                    context='SITEDB', severity=logging.WARNING)
                scripts.remove(script)
            else:
                path = os.path.join(sys.path[0], self.cssmap[script])
                path = os.path.normpath(path)
                if not os.path.exists(path):
                    log("%s not found at %s" % (script, path), 
                        context='SITEDB', severity=logging.WARNING)
                    scripts.remove(script)
        return scripts
    
    def minify(self, content):
        "remove whitespace"
        content = content.replace('\n', ' ')
        content = content.replace('\t', ' ')
        content = content.replace('   ', ' ')
        content = content.replace('  ', ' ')
        return content

    def setHeaders(type, size=0):
        if size > 0:
            response.headers['Content-Length'] = size
        response.headers['Content-Type'] = type
        response.headers['Expires'] = 'Sat, 14 Oct 2017 00:59:30 GMT'
    