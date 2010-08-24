#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: RESTModel.py,v 1.1 2009/03/05 15:08:49 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WebTools.Page import DatabasePage

class RESTModel(DatabasePage):
   """Rest model class implementation"""
   def handle_get(self, args, kwargs):
       """Example of handle_get implementation"""
       data = "RestModel handle_get method=%s params=%s" % (args, kwargs)
       self.debug(data)
       return data

   def handle_post(self, args, kwargs):
       """Example of handle_post implementation"""
       data = "RestModel handle_post method=%s params=%s" % (args, kwargs)
       self.debug(data)
       return data

   def handle_put(self, args, kwargs):
       """Example of handle_put implementation"""
       data = "RestModel handle_put method=%s params=%s" % (args, kwargs)
       self.debug(data)
       return data

   def handle_delete(self, args, kwargs):
       """Example of handle_delete implementation"""
       data = "RestModel handle_delete method=%s params=%s" % (args, kwargs)
       self.debug(data)
       return data
   
   def handle_update(self, args, kwargs):
       """Example of handle_update implementation"""
       data = "RestModel handle_update method=%s params=%s" % (args, kwargs)
       self.debug(data)
       return data
   