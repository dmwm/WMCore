#!/usr/bin/env python

import logging
import os
import ProdCommon.MCPayloads.WorkflowTools as MCWorkflowTools
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

#WMBS db stuff

#from sqlalchemy import create_engine
#import sqlalchemy.pool as pool
from sqlalchemy.exceptions import IntegrityError, OperationalError
#from WMCore.WMBS.Factory import SQLFactory 

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Actions.CreateWMBS import CreateWMBSAction


from ProdCommon.MCPayloads.LFNAlgorithm import mergedLFNBase, unmergedLFNBase
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
import ProdCommon.MCPayloads.WorkflowTools as MCWorkflowTools
from ProdCommon.MCPayloads.MergeTools import createMergeJobWorkflow

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow

logging.basicConfig(level=logging.ERROR) #DEBUG

#TODO: Put everything into a single transaction


def hashSubscription(sub):
    """
    Function to hash a subscription
    """
    return "%s:%s:%s:%s" % \
            (sub.workflow.name, sub.fileset.name, sub.type, sub.owner)

def subHashesFromJobspec(spec, datasets, owner):
    """
    Return appropriate sub hashes - multiple subs possible
    """
    return ["%s:%s:%s:%s" % (spec.workflowName(),
                         dataset.name(),
                         spec.parameters['WorkflowType'],
                         owner) for dataset in datasets
                ]

class WMBSAccountant:
    """
    class to handle WMBS things like
    inset new filesets, subscriptions and files
    mark files  as complete or failed 
    """
    
    def __init__(self, dbparams, label, spec_dir):
        """
        connect to wmbs db etc
        """
        #logger = logging.getLogger()
        self.logger = logging.getLogger('wmbs')
        #TODO: create connection string properly
        #engine = create_engine(dbparams['dbName'], convert_unicode=True,
        #                            encoding='utf-8', pool_size=10,
        #                            pool_recycle=30)
        self.db = DBFactory(self.logger, 'sqlite:///filesettest.lite')
        #CreateWMBSAction(self.logger).execute(dbinterface=self.db.connect())
        #self.dao = DAOFactory(package='WMCore.WMBS', logger=self.logger, dbinterface=self.db.connect())
        #self.dao(classname='CreateWMBS').execute()
        #factory = SQLFactory(logging)
        #factory = SQLFactory(logging)
        #self.wmbs = factory.connect(engine)       
        #try:
        #    #TODO: move into client setup script
        #    self.wmbs.createWMBS()
        #except OperationalError:
        #    pass
        
        #self.setKnownInfo()
#        self.new_datasets = {}
#        self.new_workflows = {}
#        self.new_subs = {}
        self.label = label
        self.spec_dir = spec_dir
        self.ms = None
        self.subscriptions = {} #TODO: Fill this


    
    def setMessager(self, ms):
        """
        set message system, ms is a function that will be called to send
        messages e.g. InputAvailable, ImportFileset etc.
        """
        self.ms = ms
        
    
    def sendMessage(self, type, payload='', delay='00:00:00'):
        """
        function that should be overrideen by implementation to send messages
        """
        
        if self.ms is not None:
            return self.ms(type, payload, delay)
    
        raise NotImplementedError, 'sendMessage not set via setMessager()'
    
    
    def subscriptionsFromProcWorkflow(self, spec, specfile):
        """
        Return the subscription that apply to this workflow
        """
        subs = []
        #    //
        #  //     Load input datasets
        #//
        inputDatasets = []
        for dataset in spec.inputDatasets():
            fileset = Fileset(dataset.name(), dbinterface=self.db).populate()
            inputDatasets.append(fileset)
        
        #    //
        #  //     Load output datasets
        #//
        outputDatasets = []
        for dataset in spec.outputDatasets():
            fileset = Fileset(dataset.name(), dbinterface=self.db).populate()
            outputDatasets.append(fileset)
            
        #    //
        #  //     Form subscription 
        #// 
        parentageLevel = int(spec.parameters.get('ParentageLevel', 0))
        proc_workflow = Workflow(specfile, self.label, spec.workflowName(),
                                            self.label, dbinterface=self.db)
            
        for indataset in inputDatasets:
            subs.append(Subscription(indataset, workflow=proc_workflow, 
                           type=spec.parameters['WorkflowType'],
                           parentage=parentageLevel, dbinterface=self.db)
                           )
        return subs
        
    
    def mergeSubscriptionsFromProcWorkflow(self, spec):
        """
        Return list of merge subscriptions for proccessing workflow
        """
        subs = []
        
        # create merge workflows (one for each output dataset)
        mergeWorkflows = self.createMergeWorkflow(spec)
        for mergeWFPath, mergeWF in mergeWorkflows:
            
            # save wf to wmbs
            workflow = Workflow(mergeWFPath, self.label,
                                mergeWF.workflowName(), dbinterface=self.db)
            
            #    //
            #  //     Load/Create datasets - assume only 1 for merge
            #//
            unmergedFileset = Fileset(mergeWF.inputDatasets()[0], dbinterface=self.db).populate()
            
            mergedFileset = Fileset(mergeWF.outputDatasets()[0].name(),
                                    dbinterface=self.db, is_open=True,
                                    parents=[unmergedFileset.parents])
            if not mergedFileset.exists():
                mergedFileset.create()

            #    //
            #  //     Setup subscription 
            #// 
            sub = Subscription(unmergedFileset, workflow=workflow,
                                            type='Merge', dbinterface=self.db)
            subs.append(sub)

        return subs
    
    
    def createSubscription(self, workflowSpecFile, mergeable=True):
        """
        Take a worklfow and create the neccessary 
        subscriptions (including merging) assuming that the fileset has 
        been entered into wmbs
        """ 
        
        #TODO: Either trap db errors or check for existence first
        
        spec = WorkflowSpec()
        try:
            spec.load(workflowSpecFile)
        except Exception, msg:
            logging.error("Cannot read workflow file: " + str(msg))
            raise
        
        # only work on appropriate workflows
        if spec.parameters['WorkflowType'] != 'Processing':
            logging.info('Ignoring non-processing workflow %s' % \
                                                        workflowSpecFile)
            return
        
        logging.info("Create subscriptions for %s" % workflowSpecFile)
        
        #  //
        # //  Create Subscriptions
        #//
        subscriptions = self.subscriptionsFromProcWorkflow(spec, workflowSpecFile)
        subscriptions.extend(self.mergeSubscriptionsFromProcWorkflow(spec))
        for sub in subscriptions:
            subHash = hashSubscription(sub)
            if self.subscriptions.has_key(subHash):
                logging.info('%s Subscription for %s on %s already exists' % \
                                (sub.type, sub.workflow.name, sub.fileset.name))
                continue
            
            logging.info("Creating %s subscription for %s on %s" % \
                                (sub.type, sub.workflow.name, sub.fileset.name))
            if not sub.workflow.exists():
                sub.workflow.create() 
            sub.create()
            self.subscriptions[subHash] = sub

        return

    # TODO: see if inputDatasets can be moved into fjr
    def handleJobReport(self, jobReportFile, inputDatasets, label=None):
        """
        Handle JobSuccess
        add to wmbs and can publish InputAvailable
        
        Needs to know what datasets this job ran over - the fjr may not
        contain this - hence need the caller to provide it
        """
        # get default label if not passed
        if label is None:
            label = self.label
        
        #TODO: Does this work if processing jobs produce merged data?
        try:
            reports = readJobReport(jobReportFile)
        except StandardError, ex:
            msg = "Error loading Job Report: %s\n" % jobReportFile
            logging.error(msg)
            raise
        
        logging.debug("ReportFile: %s" % jobReportFile)
        logging.debug("Contains: %s reports" % len(reports))
        
        for report in reports:
            
            try:
                if not report.jobType in ('Processing', 'Merge'):
                    logging.info("Ignoring fjr - %s" % report.jobSpecId)
                    continue
                
                logging.debug("Inserting into db %s" % report.jobSpecId)

                # update subscription with success or failure
                self.updateSubscriptions(report, inputDatasets, label)
                
                # add new files to wmbs for successful reports
                if report.wasSuccess():
                    self.recordOutputFiles(report)    

            except StandardError, ex:
                logging.error("Failed to handle fjr %s: %s" % \
                                            (report.jobSpecId, str(ex)))
                raise


    def updateSubscriptions(self, report, inputDatasets, label):
        """
        Update subscriptions from a fjr
        """
        subHashes = subHashesFromJobspec(report, inputDatasets, label)
        files = [File(input['LFN'], dbinterface=self.db).load() \
                                    for input in report.inputFiles]
        #TODO: SkippedFiles ?
        for subHash in subHashes:
            sub = self.subscriptions.get(subHash, None)
            if sub:
                if report.wasSuccess():
                    sub.completeFiles(files)
                else:
                    sub.failFiles(files)
    
    
    def recordOutputFiles(self, report):
        """
        Add Output files to wmbs
        """
        
        for ofile in report.files:
            #TODO: needed? or just lfns
            inputs = [File(input['LFN'], dbinterface=self.db).load() for \
                                            input in ofile.inputFiles]
            outputFile = File(lfn=ofile['LFN'],
                              size=ofile['Size'],
                              events=ofile['TotalEvents'],
                              parents=inputs,
                              locations=[ofile['SEName']])
                
            for dataset in ofile.dataset:
                fileset = Fileset(dataset.name(), dbinterface=self.db)
                if not fileset.exists():
                    raise RuntimeError, 'Fileset %s unknown' % dataset.name()
                fileset.addFile(outputFile)
                fileset.commit()    #TODO: move this?
                self.inputAvailable(dataset.name())


    def createMergeWorkflow(self, procWorkflow):
        """
        create the merging workflow for a processing workflow
        """
        
        results = []

        #create merge workflow
        mergeWorkflows = createMergeJobWorkflow(procWorkflow, 
                                isFastMerge = False, doCleanUp = False)
        
        # create merge workflows for each output dataset
        for watchedDatasetName, mergeWF in mergeWorkflows.items():

            # add bare cfg template to workflow
            cmsRun = mergeWF.payload
            cfg = CMSSWConfig()
            cmsRun.cfgInterface = cfg
            cfg.sourceType = "PoolSource"
            cfg.setInputMaxEvents(-1)
            outMod = cfg.getOutputModule("Merged")
            
            # save it
            fileName = watchedDatasetName.replace('/','#') + '-workflow.xml'
            workflowPath = os.path.join(self.spec_dir, 'merges', fileName)
            if not os.path.exists(os.path.dirname(workflowPath)):
                os.mkdir(os.path.dirname(workflowPath))
            mergeWF.save(workflowPath) 
            self.newWF(workflowPath)
            results.append(tuple(workflowPath, mergeWF))
            
        return results



    def inputAvailable(self, fileset):
        return self.sendMessage('InputAvailable', fileset)
        
    def importFileset(self, fileset):
        return self.sendMessage('ImportFileset', fileset)
    
    def newWF(self, path):
        return self.sendMessage('NewWorkflow', path)
