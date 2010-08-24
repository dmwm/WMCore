#!/usr/bin/env python
"""
DBS Buffer handler for BufferSuccess event
"""
__all__ = []

__revision__ = "$Id: BufferSuccess.py,v 1.4 2008/10/27 21:38:30 afaq Exp $"
__version__ = "$Reivison: $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile


import cPickle
import os
import string
import logging

from WMCore.WMFactory import WMFactory

class BufferSuccess(BaseHandler):
    """
    Default handler for buffering success. 
    Lets assume for now that this is called in result of a POLL or something
    """


    """
    def __init__(self):
	    BaseHandler.__init__(self)
	    print "THIS is Called"
    """


    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.

	    #print "I am not sure about thread pools here"

        #self.threadpool = ThreadPool(\
        #    "WMComponent.DBSBuffer.Handler.DefaultRunSlave", \
        #    self.component, 'BufferSuccess', \
        #    self.component.config.DBSBuffer.maxThreads)

        # this we overload from the base handler

    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        # OK, lets read the Database and find out if there are 
        # Datasets/Files that needs uploading to DBS
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface=factory.loadObject("UploadToDBS")
        
        
        #import pdb
        #pdb.set_trace()
        
        datasets=dbinterface.findUploadableDatasets()
        for aDataset in datasets:
            print aDataset
            files=dbinterface.findUploadableFiles(aDataset)
            print "Total files", len(files)
            for aFile in files:
                print aFile

        
        
        
	