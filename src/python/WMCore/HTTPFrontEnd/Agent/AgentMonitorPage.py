#!/usr/bin/env python

"""
The documentation for the framework
"""
    



from cherrypy import expose
from WMCore.WebTools.Page import TemplatedPage
from os import listdir
import os.path
import cherrypy
from cherrypy.lib.static import serve_file

class AgentMonitorPage(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    def index(self):
        """
        The index of the documentation
        """
        return serve_file(os.path.join(self.config.html, 'Agent', 'index.html'),
                              content_type='text/html')
        
    @expose
    def default(self, *args, **kwargs):
        """
        Show the documentation for a page or return the index
        """
        if len(args) > 0:
            return self.templatepage(args[0], config=self.config, **kwargs)
        else:
            return self.index()

    @expose
    def javascript(self, *args):
        if args[0] == "external":
            return serve_file(os.path.join(self.config.javascript, *args),
                              content_type='application/javascript')
        return serve_file(os.path.join(self.config.javascript, 
                                      'WMCore', 'WebTools', *args),
                              content_type='application/javascript')
    
    @expose
    def css(self, *args):
        return serve_file(os.path.join(self.config.css, *args),
                              content_type='text/css')

    @expose
    def examples(self, *args):
        if len(args) == 0:
            examples = listdir(os.path.join(self.config.html, 
                                            'Agent', 'examples'))
            index = "<h1>Agent Monitor Examples</h1>\n<ol>"
            for t in examples:
                index = """%s\n<li>
                           <a href='./examples/%s'>%s</a>
                           </li>""" % (index, t, t)
            return index
        
        return serve_file(os.path.join(self.config.html, 'Agent',
                                       'examples', *args),
                          content_type='text/html')
