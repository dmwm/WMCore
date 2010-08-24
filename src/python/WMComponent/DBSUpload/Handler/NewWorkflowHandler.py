#!/usr/bin/env python
"""
DBS Uploader handler for NewWorkflow event
"""
__all__ = []

__revision__ = "$Id: NewWorkflowHandler.py,v 1.11 2009/01/13 19:35:22 afaq Exp $"
__version__ = "$Reivison: $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS import DBSWriterObjects
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsAlgorithm import DbsAlgorithm

import os
import string
import logging

from WMCore.WMFactory import WMFactory

class NewWorkflowHandler(BaseHandler):
    """
    Default handler for buffering success. 
    Lets assume for now that this is called in result of a POLL or something
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        #TODO: These parameters should come from the Component Config
        self.dbsurl=self.component.config.DBSUpload.dbsurl
        self.dbsversion=self.component.config.DBSUpload.dbsversion

        #self.dbsurl='http://cmssrv17.fnal.gov:8989/DBS205Local/servlet/DBSServlet'
        self.DropParent=False
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.
        #print "I am not sure about thread pools here"
        #self.threadpool = ThreadPool(\
        #    "WMComponent.DBSBuffer.Handler.DefaultRunSlave", \
        #    self.component, 'BufferSuccess', \
        #    self.component.config.DBSBuffer.maxThreads)

        # this we overload from the base handler

    def __call__(self, event, workflowFile):
        """
        Extract relevant info from the WorkFlowSpecification and loop over Datasets
        Store them in DBSBuffer database and create in DBS
        """
        #

        logging.debug("Reading the NewDataset event payload from WorkFlowSpec: ")
        workflowFile=string.replace(workflowFile,'file://','')
        if not os.path.exists(workflowFile):
            logging.error("Workflow File Not Found: %s" % workflowFile)
            raise InvalidWorkFlowSpec(workflowFile)
        try:
            workflowSpec = WorkflowSpec()
            workflowSpec.load(workflowFile)
        except:
            logging.error("Invalid Workflow File: %s" % workflowFile)
            raise InvalidWorkFlowSpec(workflowFile)
        #  //                                                                      
        # //  Contact DBS using the DBSWriter
        #//`
        #
        #
        logging.info("DBSURL %s"%self.dbsurl)
        args = { "url" : self.dbsurl, "level" : 'ERROR', "user" :'NORMAL', "version" : self.dbsversion }
        dbswriter = DbsApi(args)
        #  //
        # //  Create Processing Datsets based on workflow
        #//
        logging.info(">>>>> create Processing Dataset ")
        #// optionally drop dataset parentage 
        if self.DropParent:
           for adataset in workflowSpec.payload._OutputDatasets:
               adataset['ParentDataset']=None

        datasets = workflowSpec.outputDatasetsWithPSet()        
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database.Interface")
        addToBuffer=factory.loadObject("AddToBuffer")
        print len(datasets)
        for dataset in datasets:
            #dataset['PSetContent']="TMP Contents, actual contents too long so dropping them for testing"
            #print "\n\n\nATTENTION: PSetContent being trimmed for TESTING, please delete line above in real world\n\n\n"
            
            primary = DBSWriterObjects.createPrimaryDataset(dataset, dbswriter)
            algoInDBS=0 #Using binary values 0/1 
            if dataset['PSetHash'] != None:  #Which probably is not the case
                algo = DBSWriterObjects.createAlgorithm(dataset, None, dbswriter)
		processed = DBSWriterObjects.createProcessedDataset(primary, algo, dataset, dbswriter)
                algoInDBS=1
            else: 
		algo = DBSWriterObjects.createAlgorithm(dataset) #Just create the object, do not upload in DBS
            	processed = DBSWriterObjects.createProcessedDataset(primary, None, dataset, dbswriter)
            # RECORD ALGO in DBSBuffer, do not create in DBS if PSetHASH is not present
            # Record this dataset in DBSBuffer
            #First ADD Algo (dataset object contains ALGO information)
            addToBuffer.addAlgo(dataset)
            #Than Add Processed Dataset
            addToBuffer.addDataset(dataset, algoInDBS)
        #  //
        # //  Create Merged Datasets for that workflow as well
        #//
        #TODO: Investigate following ?????? (Merged datasets ?)
        print ">>>>> create Merged Dataset ???????????????  <<<<<<<<"
        #dbswriter.createMergeDatasets(workflowSpec,getFastMergeConfig())
        #
        return

