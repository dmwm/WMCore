#!/usr/bin/env python

"""
Data Discovery package
"""
__author__ = "Valentin Kuznetsov <vk@mail.lns.cornell.edu>"
__revision__ = 1

from RestService import *

__all__ = ['setup_rest']

class REST(object):
    exposed = True

    def GET(self):
        cherrypy.response.status = '404 Not Found'
        cherrypy.response.body = 'Not Found'

def setup_rest():
    rest = REST()
    rest.model = RestService()
    return rest

