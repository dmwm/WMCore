#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
REST service Test Model implementation
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: TestModel.py,v 1.5 2008/12/19 01:17:10 valya Exp $"
__version__ = "$Revision: 1.5 $"


#
# An example of underlying model, 
# the TestModel class implements basic methods to be used by
# Resource class to provide a REST services.
#
class TestModel:
    """Example of model class implementation"""
    def __init__(self):
        self.data = None

    def getdata(self, method, params=None):
        """Example of getdata implementation"""
        data = "TestModel getdata method=%s params=%s" % \
               (str(method),str(params))
        return data

    def createdata(self, method, params=None):
        """Example of createdata implementation"""
        data = "TestModel getdata method=%s params=%s" % \
               (str(method),str(params))
        return data

    def deletedata(self, method, params=None):
        """Example of deletedata implementation"""
        data = "TestModel getdata method=%s params=%s" % \
               (str(method),str(params))
        return data

    def updatedata(self, method, params=None):
        """Example of updatedata implementation"""
        data = "TestModel getdata method=%s params=%s" % \
               (str(method),str(params))
        return data

