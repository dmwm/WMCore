#!/usr/bin/env python
"""
__Controllers__

Controllers return java script and/or css from a static directory, after
minimising setting appropriate headers and etags and gzip.
"""




import os
from WMCore.WebTools.Page import TemplatedPage, exposejs, exposecss
from Controllers import Controllers
from cherrypy import expose

class Masthead(TemplatedPage, Controllers):
    """
    The WebTools masthead
    """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        Controllers.__init__(self, config)

    @expose
    def index(self):
        return self.masthead()

    @exposejs
    def masthead (self):
        return self.templatepage ('masthead')

    @exposejs
    def masthead_table(self):
        return self.templatepage ('masthead_table')

    @exposecss
    def mastheadcss(self, site=None, *args, **kwargs):
        files = ['dmwt_main.css', 'dmwt_masthead.css']
        if site:
            files+= ['dmwt_masthead_%s.css' % site]
        path = __file__.rsplit('/',1)[0]
        data = ""
        for f in files:
            filename = "%s/css/%s" % (path, f)
            if os.path.exists(filename):
                lines = open(filename).readlines()
                for l in lines:
                    data = data + l

        return self.minify(self.templatepage ('data', {'data':data}))

    @exposecss
    def mastheadcss_table(self, site=None, *args, **kwargs):
        files = ['dmwt_main_table.css', 'dmwt_masthead.css']
        if site:
            files+= ['dmwt_masthead_table_%s.css' % site]
        path = __file__.rsplit('/',1)[0]
        data = ""
        for f in files:
            filename = "%s/css/%s" % (path, f)
            if os.path.exists(filename):
                lines = open(filename).readlines()
                for l in lines:
                    data = data + l

        return self.minify(self.templatePage ('data', {'data':data}))
