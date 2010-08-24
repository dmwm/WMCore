#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: RESTModel.py,v 1.2 2009/03/10 02:02:48 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WebTools.Page import DatabasePage

class RESTModel(DatabasePage):
   """Rest model class implementation"""
   def handle_get(self, args, kwargs):
       """Example of handle_get implementation"""
       data = {"args": args, "kwargs": kwargs}
       self.debug(str(data))
       return data

   def handle_post(self, args, kwargs):
       """Example of handle_post implementation"""
       data = {"args": args, "kwargs": kwargs}
       self.debug(str(data))
       return data

   def handle_put(self, args, kwargs):
       """Example of handle_put implementation"""
       data = {"args": args, "kwargs": kwargs}
       self.debug(str(data))
       return data

   def handle_delete(self, args, kwargs):
       """Example of handle_delete implementation"""
       data = {"args": args, "kwargs": kwargs}
       self.debug(str(data))
       return data
   
   def handle_update(self, args, kwargs):
       """Example of handle_update implementation"""
       data = {"args": args, "kwargs": kwargs}
       self.debug(str(data))
       return data
   