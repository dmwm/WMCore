#!/usr/bin/env python

"""
The documentation for the framework
"""


import cherrypy
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
        raise HTTPError(404, "Page not found")
    return serve_file(name, content_type = contentType)

class WMBSMonitorPage(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    @cherrypy.tools.secmodv2()
    def index(self):
        """
        The index of the documentation
        """
        return serveFile('text/html', self.config.html, 'WMBS', 'index.html')

    @expose
    @cherrypy.tools.secmodv2()
    def default(self, *args):
        """
        Show the documentation for a page or return the index
        """
        if len(args) > 0:
            return serveFile('text/html',
                             path.join(self.config.html, 'WMBS'),*args)
        else:
            return self.index()

    @expose
    @cherrypy.tools.secmodv2()
    def javascript(self, *args):
        if args[0] == "external":
            return serveFile('application/javascript',
                             path.join(self.config.javascript), *args)
        return serveFile('application/javascript',
                          path.join(self.config.javascript,
                                    'WMCore', 'WebTools'), *args)

    @expose
    @cherrypy.tools.secmodv2()
    def css(self, *args):
        if args[0] == "external":
            return serveFile('text/css',
                             self.config.css, *args)
        return serveFile('text/css',
                          path.join(self.config.css,
                                    'WMCore', 'WebTools'), *args)

    @expose
    @cherrypy.tools.secmodv2()
    def template(self, *args, **kwargs):
        """
        Show the documentation for a page or return the index
        """
        if len(args) > 0:
            return self.templatepage(args[0], **kwargs)
        # make not found page
        return self.index()

    @expose
    @cherrypy.tools.secmodv2()
    def wmbsStatus(self, subscriptionType = "All"):
        """
        _wmbsStatus_

        Render the main monitoring page that displays the status for all
        subscriptions in WMBS.  The page itself takes a single parameter from
        the webserver:
          subType - The type of subscription to display.  This will default to
                    All.

        The template itself will take the subscription type and the WMBS
        instance name from the config.
        """

        return self.templatepage("WMBS", subType = subscriptionType,
                                           instance = "WMBS")

    @expose
    @cherrypy.tools.secmodv2()
    def subscriptionStatus(self, subscriptionId):
        """
        _subscriptionStatus_

        Render the subscription status page.  The page itself takes a single
        mandatory parameter from the webserver:
          subscriptionId - The id of the subscription to display.
        """

        return self.templatepage("WMBSSubscription",
                                           subscriptionId = int(subscriptionId))

    @expose
    @cherrypy.tools.secmodv2()
    def jobStatus(self, jobState = "success", interval = 7200):
        """
        _jobStatus_

        Render the job status page.  The page itself takes two parameters
        from the webserver:
          jobState - What state is displayed
          interval - The amount of time to display

        The defaults will be the success state and 2 hours.  The template itself
        takes the jobState interval, the wmbs instance name and a URL used to
        display the content of couch documents for jobs.
        """

        return self.templatepage("WMBSJobStatus", jobState = jobState,
                                  interval = int(interval),
                                  instance = self.config.instance,
                                  couchURL = self.config.couchURL)
