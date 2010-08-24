#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2008 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2008
"""
REST service implementation
"""
from services.rest.Resource import Resource
#from services.test.TestModel import *
#from services.test.TestFormatter import *

class RestService(Resource):
    # expose all methods of RestService into public
    exposed = True
    # define internal class to be used by Resource
    # SO FAR IT IS TestModel, write your own Model class and add it here
#    _source_class=TestModel
#    _verbose = 0
#    _formatter = DataCache()
    # define internal url used by Resource
    # to be defined by RestServer at run time
    _url   = ""
    _host  = "" 
    _fUrl  = ""
    _mUrl  = ""
#    _source_class= None
    _verbose = 0
    _formatter = None
    _model = None
    def GET(self,*args,**kwargs):
        return self.handle_GET(args,kwargs)
    def POST(self,*args,**kwargs):
        return self.handle_POST(args,kwargs)
    def PUT(self,*args,**kwargs):
        return self.handle_PUT(args,kwargs)
    def DELETE(self,*args,**kwargs):
        return self.handle_DELETE(args,kwargs)

