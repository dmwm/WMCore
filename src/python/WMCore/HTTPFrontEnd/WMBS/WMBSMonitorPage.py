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

class WMBSMonitorPage(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    def index(self):
        """
        The index of the documentation
        """
        return serve_file(os.path.join(self.config.html, 'WMBS', 'index.html'),
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
                                            'WMBS', 'examples'))
            index = "<h1>WMBS Monitor Examples</h1>\n<ol>"
            for t in examples:
                index = """%s\n<li>
                           <a href='./%s'>%s</a>
                           </li>""" % (index, t, t)
            return index
        
        return serve_file(os.path.join(self.config.html, 'WMBS',
                                       'examples', *args),
                          content_type='text/html')
        
    @expose    
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
