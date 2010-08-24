#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

from Framework import Controller
from Framework.PluginManager import DeclarePlugin
from cherrypy import expose

from services.rest.RestServer import *

DeclarePlugin ("/Controllers/REST/RestServer", RestServer, {"baseUrl": "/services"})
