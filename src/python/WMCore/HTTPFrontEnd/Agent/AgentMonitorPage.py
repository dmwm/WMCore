#!/usr/bin/env python

"""
The documentation for the framework
"""




from cherrypy import expose
from WMCore.WebTools.Page import TemplatedPage
from os import path
from cherrypy import HTTPError
from cherrypy.lib.static import serve_file

def serveFile(contentType, prefix, *args):
    """Return a workflow from the cache"""
    name = path.normpath(path.join(prefix, *args))
    if path.commonprefix([name, prefix]) != prefix:
        raise HTTPError(403)
    if not path.exists(name):
        raise HTTPError(404, "%s not found" % name)
    return serve_file(name, content_type = contentType)

class AgentMonitorPage(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    def index(self):
        """
        The index of the documentation
        """
        return serveFile('text/html', self.config.html, 'Agent', 'index.html')

    @expose
    def default(self, *args):
        """
        Show the documentation for a page or return the index
        """
        if len(args) > 0:
            return serveFile('text/html',
                             path.join(self.config.html, 'Agent'),*args)
        else:

            return self.index()

    @expose
    def javascript(self, *args):
        if args[0] == "external":
            return serveFile('application/javascript',
                             path.join(self.config.javascript), *args)
        return serveFile('application/javascript',
                          path.join(self.config.javascript,
                                    'WMCore', 'WebTools'), *args)

    @expose
    def css(self, *args):
        if args[0] == "external":
            return serveFile('text/css',
                             self.config.css, *args)
        return serveFile('text/css',
                          path.join(self.config.css,
                                    'WMCore', 'WebTools'), *args)
