#!/usr/bin/env python

"""
The documentation for the framework
"""
    
__revision__ = "$Id: WorkQueueMonitorPage.py,v 1.1 2010/04/30 21:26:26 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from cherrypy import expose
from WMCore.WebTools.Page import TemplatedPage
from os import listdir
import os.path
import cherrypy
from cherrypy.lib.static import serve_file

class WorkQueueMonitorPage(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    def index(self):
        """
        The index of the documentation
        """
        return serve_file(os.path.join(self.config.html, *args),
                              content_type='text/html')
        
    @expose
    def default(self, *args, **kwargs):
        """
        Show the documentation for a page or return the index
        """
        if len(args) > 0:
            return self.templatepage(args[0], config=self.config)
        else:
            return self.index()

    @expose
    def javascript(self, *args):
        return serve_file(os.path.join(self.config.javascript, *args),
                              content_type='text/html')
    
    @expose
    def css(self, *args):
        return serve_file(os.path.join(self.config.css, *args),
                              content_type='text/css')

    @expose
    def examples(self, *args):
        if len(args) == 0:
            examples = listdir(os.path.join(self.config.html, 'examples'))
            index = "<h1>WorkQueuMonitor Examples</h1>\n<ol>"
            for t in examples:
                index = "%s\n<li><a href='%s'>%s</a></li>" % (index, t, t)
            return index
        
        return serve_file(os.path.join(self.config.html, 'examples', *args),
                              content_type='text/html')
