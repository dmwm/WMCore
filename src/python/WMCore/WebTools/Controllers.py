#!/usr/bin/env python
"""
__Controllers__

Controllers return java script and/or css from a static directory, after 
minimising setting appropriate headers and etags and gzip.  
"""

__revision__ = "$Id: Controllers.py,v 1.13 2009/07/02 20:15:36 valya Exp $"
__version__ = "$Revision: 1.13 $"

import cherrypy
from cherrypy import expose, log, response
from cherrypy import config as cherryconf
from cherrypy import tools
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
        return """Controller settings:
<ul>
<li>style loads from <a href='css'>css</a></li>
<li>javascript loads from <a href='js'>js</a></li>
<li>images loads from <a href='images'>images</a></li>
</ul>
"""
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()

    @expose
    def images(self, *args, **kwargs):
        """
        serve static images
        """
        mime_types = ['*/*', 'image/gif', 'image/png', 'image/jpg', 'image/jpeg']
        accepts = cherrypy.request.headers.elements('Accept')
        for accept in accepts:
            if  accept.value in mime_types and len(args) == 1 \
                and self.imagemap.has_key(args[0]):
                image = self.imagemap[args[0]]
                # use image extension to pass correct content type
                ctype = 'image/%s' % image.split('.')[-1]
                cherrypy.response.headers['Content-type'] = ctype
                return serve_file(image, content_type=ctype)
    
    @exposecss
    @tools.gzip()
    def css(self, *args, **kwargs):
        """
        cat together the specified css files and return a single css include
        get css by calling: /controllers/css/file1/file2/file3
        """
        mime_types = ['text/css']
        cherryconf.update({'tools.encode.on': True, 
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                          })
        
        args = list(args)
        try:
            args.insert(0, 'cms_reset.css')
        except:
            pass
        try:
            args.insert(0, 'reset.css')
        except:
            pass
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
    @tools.gzip()
    def js(self, *args, **kwargs):
        """
        cat together the specified js files and return a single js include
        get js by calling: /controllers/js/file1/file2/file3
        """
        mime_types = ['application/javascript', 'text/javascript',
                      'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                           'tools.encode.on': True,
                          })
        
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
    
