#!/usr/bin/env python
"""
__Controllers__

Controllers return java script and/or css from a static directory, after 
minimising setting appropriate headers and etags and gzip.  
"""

__revision__ = "$Id: Controllers.py,v 1.12 2009/06/29 19:13:25 valya Exp $"
__version__ = "$Revision: 1.12 $"

import cherrypy
from cherrypy import expose, log, response
from cherrypy import config as cherryconf
from cherrypy.lib.static import serve_file
# Factory to load pages dynamically
from WMCore.WMFactory import WMFactory
# Logging
import WMCore.WMLogging
import logging, os, sys
from WMCore.WebTools.Page import Page, exposejs, exposecss

class Controllers(Page):
    """
    Controllers return java script and/or css from a static directory, after 
minimising setting appropriate headers and etags and gzip.
    """
    def __init__(self, config):
        Page.__init__(self, config)
        self.dict = dict
        self.cssmap = {}
        self.jsmap = {}
        self.cache = {}
        self.imagemap = {}
        try:
            self.cssmap = self.config.css
        except:
            pass
        try:
            self.jsmap = self.config.js
        except:
            pass
        try:
            self.imagemap = self.config.images
        except:
            pass
         
    @expose
    def index(self):
        return "style loads from <a href='css'>css</a>, javascript from <a href='js'>js</a>"
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()

    @expose
    def images(self, *args, **kwargs):
        """
        serve static images
        """
        mime_types = ['image/gif', 'image/png', 'image/jpg', 'image/jpeg']
        cherryconf.update ({'tools.encode.on': True, 'tools.gzip.on': True})
        accepts = cherrypy.request.headers.elements('Accept')
        for accept in accepts:
            if  accept.value in mime_types and len(args) == 1 \
                and self.imagemap.has_key(args[0]):
                image = self.imagemap[args[0]]
                return serve_file(image)
    
    @exposecss
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
                if  self.cssmap.has_key(script):
                    path = os.path.join(sys.path[0], self.cssmap[script])
                    path = os.path.normpath(path)
                    file = open(path)
                    data = "\n".join ([data, file.read().replace('@CHARSET "UTF-8";', '')])
                    file.close()
            #self.setHeaders ("text/css")
            self.cache[id] = self.minify(data)
        return self.cache[id] 
        
    @exposejs
    def js(self, *args, **kwargs):
        """
        cat together the specified js files and return a single js include
        get js by calling: /controllers/js/file1/file2/file3
        """
        cherryconf.update ({'tools.encode.on': True, 'tools.gzip.on': True})
        
        args = list(args)
        scripts = self.checkScripts(args, self.jsmap)
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
    
    @exposejs
    def yui(self, *args, **kwargs):
        """
        cat together the specified YUI files. args[0] should be the YUI version,
        and the scripts should be specified in the kwargs s=scriptname
        TODO: support scripts as path args
        TODO: support for CSS & images
        TODO: check that the script/css/image exists - we'll just assume you 
        have a working YUI install for now....
        """
        cherryconf.update ({'tools.encode.on': True, 'tools.gzip.on': True})
        version = args[0]
        scripts = self.makelist(kwargs['s'])
        
        
    def checkScripts(self, scripts, map):
        """
        Check a script is known to the map and that the script actually exists   
        """           
        for script in scripts:
            if script not in map.keys():
                self.warning("%s not known" % script)
                scripts.remove(script)
            else:
                path = os.path.join(sys.path[0], map[script])
                path = os.path.normpath(path)
                if not os.path.exists(path):
                    self.warning("%s not found at %s" % (script, path))
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
    
