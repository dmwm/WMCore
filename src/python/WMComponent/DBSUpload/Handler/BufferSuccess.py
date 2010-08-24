#!/usr/bin/env python
"""
DBS Buffer handler for BufferSuccess event
"""
__all__ = []

__revision__ = "$Id: BufferSuccess.py,v 1.8 2008/11/07 03:49:04 afaq Exp $"
__version__ = "$Reivison: $"
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
            print aDataset.keys()
            algos = dbinterface.findAlgos(aDataset)
            if aDataset['AlgoInDBS'] == 0:
                #Check to See if Algo exists
                #it has PSetHash
                #and then Upload it to DBS
                print aDataset.keys()
            #Find files for each dataset and then UPLOAD 10 files at a time 
            #(10 is just a number of choice now, later it will be a configurable parameter)
            files=dbinterface.findUploadableFiles(aDataset)

            print "Total files", len(files)
            #base64.decodestring(aFile['RunLumiInfo'])
            
            self.dbswriter.insertFilesForDBSBuffer(files, dict(aDataset), algos, jobType = "NotMerge", insertDetectorData = False)

        return
    def uploadDataset(self, workflowFile):
        """
        
        NOT YET USED, May never be used 
        _newDatasetEvent_

        Extract relevant info from the WorkFlowSpecification and loop over Dataset
        """
        #  //                                                                      
        # //  Contact DBS using the DBSWriter
        #//`
        logging.info("DBSURL %s"%self.args['DBSURL'])
        
        #  //
        # //  Create Processing Datsets based on workflow
        #//

        logging.info(">>>>> create Processing Dataset ")
        #// optionally drop dataset parentage 
        if self.DropParent:
           for adataset in workflowSpec.payload._OutputDatasets:
               adataset['ParentDataset']=None

        dbswriter.createDatasets(workflowSpec)
        #  //
        # //  Create Merged Datasets for that workflow as well
        #//
        logging.info(">>>>> create Merged Dataset ")
        dbswriter.createMergeDatasets(workflowSpec,getFastMergeConfig())
        return

        
        
	