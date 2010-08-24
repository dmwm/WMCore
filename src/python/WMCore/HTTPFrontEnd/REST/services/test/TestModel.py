#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
# Author:  Valentin Kuznetsov, 2008
"""
REST service Test Model implementation
"""

#
# An example of underlying model, 
# the TestModel class implements basic methods to be used by
# Resource class to provide a REST services.
#
class TestModel:
    """Example of model class implementation"""
    def __init__(self):
        self.data = "DATA"
    def getdata(self, *args, **kwargs):
        """Example of getdata implementation"""
        self.data = "TestModel getData args='%s' kwargs='%s'" % \
                    (str(args),str(kwargs))
        return self.data
    def createdata(self, *args, **kwargs):
        """Example of createdata implementation"""
        self.data = "TestModel createData args='%s' kwargs='%s'" % \
                    (str(args),str(kwargs))
        return self.data
    def deletedata(self, *args, **kwargs):
        """Example of deletedata implementation"""
        self.data = "TestModel deleteData args='%s' kwargs='%s'" % \
                    (str(args),str(kwargs))
        return self.data
    def updatedata(self, *args, **kwargs):
        """Example of updatedata implementation"""
        self.data = "TestModel updateData args='%s' kwargs='%s'" % \
                    (str(args),str(kwargs))
        return self.data
