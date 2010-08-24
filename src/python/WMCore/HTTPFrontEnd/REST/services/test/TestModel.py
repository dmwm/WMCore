#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2008 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2008
"""
REST service Test Model implementation
"""

#
# An example of underlying model, the TestModel class implements basic methods to be used by
# Resource class to provide a REST services.
#
class TestModel:
    def __init__(self):
        self.data="DATA"
    def getData(self,args,**kwargs):
        self.data="TestModel getData args='%s' kwargs='%s'"%(str(args),str(kwargs))
        return self.data
    def createData(self,args,**kwargs):
        self.data="TestModel createData args='%s' kwargs='%s'"%(str(args),str(kwargs))
        return self.data
    def deleteData(self,args,**kwargs):
        self.data="TestModel deleteData args='%s' kwargs='%s'"%(str(args),str(kwargs))
        return self.data
    def updateData(self,args,**kwargs):
        self.data="TestModel updateData args='%s' kwargs='%s'"%(str(args),str(kwargs))
        return self.data
