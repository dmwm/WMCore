#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
# Author:  Valentin Kuznetsov, 2008
"""
REST service implementation
"""
from services.rest.Resource import Resource

class RestService(Resource):
    """REST service base class"""
    # expose all methods of RestService into public
    exposed = True
    # declare internal data members used by Resource
    # to be defined by RestServer at run time
    _url   = ""
    _host  = "" 
    _murl  = ""
    _verbose = 0
    _formatter = None
    _model = None
    def GET(self, *args, **kwargs):
        """implement GET method via parent handle get method"""
        return self.handle_get(args, kwargs)
    def POST(self, *args, **kwargs):
        """implement POST method via parent handle post method"""
        return self.handle_post(args, kwargs)
    def PUT(self, *args, **kwargs):
        """implement PUT method via parent handle put method"""
        return self.handle_put(args, kwargs)
    def DELETE(self, *args, **kwargs):
        """implement DELETE method via parent handle delete method"""
        return self.handle_delete(args, kwargs)

