#!/usr/bin/env python
"""
DBS Uploader handler for NewWorkflow event
"""
__all__ = []

__revision__ = "$Id: NewWorkflowHandler.py,v 1.1 2008/10/29 18:00:54 afaq Exp $"
__version__ = "$Reivison: $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from DBSAPI.dbsApiException import DbsException

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
        try:
            self.dbsApi = DbsApi(args)
        except DbsException, ex:
            msg = "Error in DBSWriterError with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
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
        Extract relevant info from the WorkFlowSpecification and loop over Dataset
        """
        #Payload in this case is a workflow configuration.
        #TODO: EXTRACT Dataset information and store it in database at this point ?
        # OR just go ahead and create it in DBS ?
        
        
        #factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        #dbinterface=factory.loadObject("UploadToDBS")
        #datasets=dbinterface.findUploadableDatasets()
        
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
        logging.info("DBSURL %s"%self.args['DBSURL'])
        #dbswriter = DBSWriter('fakeurl') 
        dbswriter = DBSWriter(self.args['DBSURL'],level='ERROR')
        #  //
        # //  Create Processing Datsets based on workflow
        #//
        logging.info(">>>>> create Processing Dataset ")
        #// optionally drop dataset parentage 
        if self.DropParent:
           for adataset in workflowSpec.payload._OutputDatasets:
               adataset['ParentDataset']=None

        #createDatasets(workflowSpec)
        datasets = workflowSpec.outputDatasets()
        
        cfgMeta = None
        try:
            cfgInt = pnode.cfgInterface
            cfgMeta = cfgInt.configMetadata
            cfgMeta['Type'] = self.workflow.parameters["RequestCategory"]    
            #
            print     cfgMeta['ApplicationName']
            print     cfgMeta['ApplicationVersion']
            print     cfgMeta["ApplicationFamily"]  
            print     cfgMeta.get('PSetContent',None)
            psetHash = cfgMeta.get('PSetHash',None)
            print psetHash
            
        except Exception, ex:
            msg = "Unable to Extract cfg data from workflow"
            msg += str(ex)
            logging.error(msg)
            return
        
        for dataset in datasets:
            primary = DBSWriterObjects.createPrimaryDataset(dataset, self.dbsApi)
            if psetHas != None:  #Which probably is the case
                algo = DBSWriterObjects.createAlgorithm(dataset, cfgMeta, self.apiRef)
            else: algo = DbsAlgorithm()
            processed = DBSWriterObjects.createProcessedDataset(
                primary, algo, dataset, self.apiRef)
            # RECORD ALGO in DBSBuffer, do not create in DBS if PSetHASH is not present
            # Record this dataset in DBSBuffer
   
        #  //
        # //  Create Merged Datasets for that workflow as well
        #//
        #TODO: Investigate following ?????? (Merged datasets ?)
        logging.info(">>>>> create Merged Dataset ???????????????  <<<<<<<<")
        #dbswriter.createMergeDatasets(workflowSpec,getFastMergeConfig())
        #
        #TODO: UPDATE DATASET STATUS in Database???
        #Get the dataset PATH and then update the status in database
        #Store the Algo/PSet information as well
        
        return




        
        
    