#!/usr/bin/env python
"""
DBS Buffer handler for BufferSuccess event
"""
__all__ = []

__revision__ = "$Id: BufferSuccess.py,v 1.11 2008/11/19 19:12:38 afaq Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

from ProdCommon.DataMgmt.DBS import DBSWriterObjects

import base64
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
        self.dbsurl='http://cmssrv17.fnal.gov:8989/DBS_2_0_3_TEST/servlet/DBSServlet'
        #args = { "url" : self.dbsurl, "level" : 'ERROR', "user" :'NORMAL', "version" :'DBS_2_0_3'}
        #dbswriter = DbsApi(args)
        #dbswriter = DBSWriter('fakeurl') 
        self.dbswriter = DBSWriter(self.dbsurl, level='ERROR', user='NORMAL', version='DBS_2_0_3')
        
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
        
        datasets=dbinterface.findUploadableDatasets()
        for aDataset in datasets:
            #Check Dataset for AlgoInDBS (Uploaded to DBS or not)    
            #We need to get algos anyways for File insertion
            
            #WE can upload dataset (primary/Processed) here as well, in case workflow-spec fails to load them (May be after some testing)
            algos = dbinterface.findAlgos(aDataset)
            if aDataset['AlgoInDBS'] == 0:
                #Check to See if Algo exists
                #it has PSetHash
                #and then Upload it to DBS
                #TODO: Check that Algo has PSetHash and then Upload it to DBS
                for algo in algos:
                    DBSWriterObjects.createAlgorithm(dict(algo), configMetadata = None, apiRef = self.dbswriter.dbs)
                    #TODO: Update Algorithm status in DBS
                   
                    dbinterface.updateDSAlgo(dict(aDataset))
                
            #Find files for each dataset and then UPLOAD 10 files at a time 
            #(10 is just a number of choice now, later it will be a configurable parameter)
            files=dbinterface.findUploadableFiles(aDataset)

            print "Total files", len(files)
            #base64.decodestring(aFile['RunLumiInfo'])
            
            self.dbswriter.insertFilesForDBSBuffer(files, dict(aDataset), algos, jobType = "NotMerge", insertDetectorData = False)
            #Update UnMigratedFile Count here !!!!
            
            print "COMMENTED line below for testing..."
            #dbinterface.updateDSFileCount(aDataset, 10)
            #TODO: Update the files as well to Migrated
            
            print "NEXT to be implemented"
            dbinterface.updateFilesStatus(files)
            

        return
