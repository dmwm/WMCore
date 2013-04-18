"""
Main ReqMgr web page handler.

"""


import os
import re

from WMCore.REST.Server import RESTFrontPage


class FrontPage(RESTFrontPage):
    
    def __init__(self, app, config, mount):
        """
        :arg app: reference to the application object.
        :arg config: reference to the configuration.
        :arg str mount: URL mount point.
        
        """
        
        # must be in a static content directory
        frontpage = "html/reqmgr/index.html"
        CONTENT = os.path.abspath(__file__).rsplit('/', 5)[0]
        X = (__file__.find("/xlib/") >= 0 and "x") or ""
        
        roots = \
        {
            "html":
            {
                # without repeating the 'html' here, it doesn't work
                # due to path gymnastics in WMCore.REST.Server.py
                "root": "%s/%sdata/html/" % (CONTENT, X),
                # TODO:
                # this needs tuning here!
                # was giving error:
                # ERROR: front-page 'html/reqmgr/style/style.css' not matched by rx '^[a-z]+/[-a-z0-9]+\.(?:css|js|png|gif|html)$' for 'html'
                #"rx": re.compile(r"^[a-z]+/[-a-z0-9]+\.(?:css|js|png|gif|html)$")
                "rx": re.compile(r"^.*$")
            },
        }
        RESTFrontPage.__init__(self, app, config, mount, frontpage, roots)