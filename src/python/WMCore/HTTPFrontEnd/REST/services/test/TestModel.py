#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
REST service Test Model implementation
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id:"
__version__ = "$Revision:"


#
# An example of underlying model, 
# the TestModel class implements basic methods to be used by
# Resource class to provide a REST services.
#
class TestModel:
    """Example of model class implementation"""
    def __init__(self):
        self.data = "DATA"
    def getdata(self, method, params=None):
        """Example of getdata implementation"""
        self.data = "TestModel getdata method=%s params=%s" % \
                    (str(method),str(params))
        return self.data
    def createdata(self, method, params=None):
        """Example of createdata implementation"""
        self.data = "TestModel createdata method=%s params=%s" % \
                    (str(method),str(params))
        return self.data
    def deletedata(self, method, params=None):
        """Example of deletedata implementation"""
        self.data = "TestModel deletedata method=%s params=%s" % \
                    (str(method),str(params))
        return self.data
    def updatedata(self, method, params=None):
        """Example of updatedata implementation"""
        self.data = "TestModel updatedata method=%s params=%s" % \
                    (str(method),str(params))
        return self.data
