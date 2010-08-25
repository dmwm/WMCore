#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: RESTModel.py,v 1.5 2009/05/12 11:28:22 metson Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WebTools.WebAPI import WebAPI

class RESTModel(WebAPI):
   """Rest model class implementation"""
   def handle_get(self, args=[], kwargs={}):
       """Example of handle_get implementation"""
       data = {"message": "Unsupported verb method: GET",
               "args": args,
               "kwargs": kwargs}
       self.debug(str(data))
       return data

   def handle_post(self, args=[], kwargs={}):
       """Example of handle_post implementation"""
       data = {"message": "Unsupported verb method: POST",
               "args": args,
               "kwargs": kwargs}
       self.debug(str(data))
       return data

   def handle_put(self, args=[], kwargs={}):
       """Example of handle_put implementation"""
       data = {"message": "Unsupported verb method: PUT",
               "args": args,
               "kwargs": kwargs}
       self.debug(str(data))
       return data

   def handle_delete(self, args=[], kwargs={}):
       """Example of handle_delete implementation"""
       data = {"message": "Unsupported verb method: DELETE",
               "args": args,
               "kwargs": kwargs}
       self.debug(str(data))
       return data