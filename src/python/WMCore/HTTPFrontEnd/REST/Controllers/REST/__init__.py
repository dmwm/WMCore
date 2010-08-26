#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Plugin declaration for WEBTOOLS
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id:"
__version__ = "$Revision:"

from Framework import Controller
from Framework.PluginManager import DeclarePlugin
from cherrypy import expose

from WMCore.HTTPFrontEnd.REST.services.rest.RestServer import RestServer

DeclarePlugin ("/Controllers/REST/RestServer", 
                RestServer, {"baseUrl": "/services"})
