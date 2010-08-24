#!/usr/bin/env python

import logging
import os
import ProdCommon.MCPayloads.WorkflowTools as MCWorkflowTools
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

#WMBS db stuff

from sqlalchemy import create_engine
import sqlalchemy.pool as pool
from sqlalchemy.exceptions import IntegrityError, OperationalError

from WMCore.WMBS.Factory import SQLFactory 

from ProdCommon.MCPayloads.LFNAlgorithm import mergedLFNBase, unmergedLFNBase
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
import ProdCommon.MCPayloads.WorkflowTools as MCWorkflowTools
from ProdCommon.MCPayloads.MergeTools import createMergeJobWorkflow

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Fileset import Subscription
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow

logging.basicConfig(level=logging.ERROR) #DEBUG

#TODO: Put everything into a single transaction

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
        
        #TODO: create connection string properly
        engine = create_engine(dbparams['dbName'], convert_unicode=True,
                                    encoding='utf-8', pool_size=10,
                                    pool_recycle=30)
        #factory = SQLFactory(logging)
        factory = SQLFactory(logging)
        self.wmbs = factory.connect(engine)       
        try:
            #TODO: move into client setup script
            self.wmbs.createWMBS()
        except OperationalError:
            pass
        
        self.setKnownInfo()
        self.new_datasets = {}
        self.new_workflows = {}
        self.new_subs = {}
        self.label = label
        self.spec_dir = spec_dir
        self.ms = None
    
    
    def setKnownInfo(self):
        """
        get workflows and filesets already available in db
        """
        self.known_datasets = {}
        temp = self.wmbs.showAllFilesets()
        
        for fs in temp:
            self.known_datasets[fs] = Fileset(fs, self.wmbs).populate()
    
        self.known_workflows = {}
        temp = self.wmbs.showAllWorkflows()
        for wf, owner in temp:
            self.known_workflows[wf] = Workflow(wf, owner, self.wmbs)

        self.known_subs = {}
        #TODO: fill this
        

    def updateKnownInfo(self):
        """
        update known info with items seen since last update
        """
        self.known_datasets.update(self.new_datasets)
        self.known_workflows.update(self.new_workflows)
        self.known_subs.update(self.new_subs)
        self.new_datasets = {}
        self.new_workflows = {}
        self.new_subs = {}

    
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
    
    
    def newWorkflow(self, workflowSpecFile, mergeable=True):
        """
        take a workflowSpec file and setup subscription in wmbs
        """
        print "start new workflow"
        spec = WorkflowSpec()
        try:
            spec.load(workflowSpecFile)
        except Exception, msg:
            logging.error("Cannot read workflow file: " + str(msg))
            raise
        
        # only work on appropriate workflows
        if spec.parameters['WorkflowType'] in ('Merge'):
            logging.info('Ignoring workflow %s' % workflowSpecFile)
            return
        
        # exit if already watched
        # move to watching workflows
        # - allow child processing to be registered before parent
        if self.known_workflows.has_key(spec.workflowName()):
            logging.info('Workflow known %s - skipping %s insertion' 
                                        % spec.workflowName(), workflowSpecFile)
            return
        logging.info('Reading workflow from %s' % workflowSpecFile)
        
        proc_workflow = Workflow(spec.workflowName(), self.label, self.wmbs).create()
        self.new_workflows[spec.workflowName()] = proc_workflow
        
        #    //
        #  //     Insert pileup datasets
        #//
        for dataset in spec.pileupDatasets():
            if not self.known_datasets.has_key(dataset.name()):
                fileset = Fileset(dataset.name(), self.wmbs, is_open=False).create()
                self.importFileset(dataset.name())
                self.new_datasets[dataset.name()] = fileset
            else:
                logging.warning("pileup dataset %s already known - skip import") % dataset.name()
 
        #    //
        #  //     Insert input datasets
        #//
        inputDatasets = []
        for dataset in spec.inputDatasets():
            if not self.known_datasets.has_key(dataset.name()):
                # need to find out from workflow if open - default true
                fileset = Fileset(dataset.name(), self.wmbs).create()
                self.importFileset(dataset.name())
                self.new_datasets[dataset.name()] = fileset
            else:
                logging.warning("input dataset %s already known - skip import") % dataset.name()
            inputDatasets.append(self.new_datasets[dataset.name()])
        print "input datasets saved %s" % [x.name for x in inputDatasets]
 
        #    //
        #  //     Insert output datasets
        #//
        for dataset in spec.outputDatasets():
            if not self.known_datasets.has_key(dataset.name()):
            #dont add pileup to parentage
                fileset = Fileset(dataset.name(), self.wmbs, is_open=True, 
                              parents=inputDatasets).create()
                self.new_datasets[dataset.name()] = fileset

        # set up subscription 
        #  - assume only 1 input dataset for the moment
        #  - assume parentage set by num input datasets move to workflow spec
        #TODO: Revisit this!!!
        sub = Subscription(inputDatasets[-1], workflow=proc_workflow, 
                           type=spec.parameters['WorkflowType'],
                           parentage=len(spec.inputDatasets())-1, wmbs=self.wmbs)
        sub.create()
        self.new_subs['%s:%s' % 
                (proc_workflow.spec, spec.parameters['WorkflowType'])] = sub
        
        if mergeable:
            self.createMergeWorkflow(spec)

        self.updateKnownInfo()
        return
    
    
    def jobSuccess(self, jobReportFile):
        """
        Handle JobSuccess
        add to wmbs and can publish InputAvailable
        """
        try:
            reports = readJobReport(jobReportFile)
        except StandardError, ex:
            msg = "Error loading Job Report: %s\n" % jobReportFile
            logging.error(msg)
            raise
        
        logging.debug("ReportFile: %s" % jobReportFile)
        logging.debug("Contains: %s reports" % len(reports))
        
        for report in reports:
            logging.debug("Inserting into db %s" % report.jobSpecId)
            try:
                # get from job instead?
                sub = self.known_subs.get('%s:%s' % (report.workflowSpecId, report.jobType), None)
                if not sub:
                    raise RuntimeError, 'Subscription not found %s:%s' % (report.workflowSpecId, report.jobType)
                
                if report.jobType in ('Processing', 'Merge'):
                    newfilesets = self.insertFile(report, sub)
                    
                    # Make it known that new files are available for a dataset
                    [self.inputAvailable(x) for x in newfilesets]
                    
            except StandardError, ex:
                # If error on insert save for later retry
                logging.error("Failed to insert job files into the wmbs: %s" % str(ex))
                raise

        
        
    def insertFile(self, jobReport, subscription):
        """
        insert file to wmbs from job report
        """
        filesFor = set()
        
        for ofile in jobReport.files:
            inputs = set()
            for input in ofile.inputFiles:
                #TODO: Add rest of file metadata
                file = File(input['LFN'], wmbs=self.wmbs).load()
                inputs.add(file)
            subscription.completeFiles(inputs)

            outputFile = File(ofile['LFN'], parents=inputs, locations=[ofile['SEName']])
            
            for dataset in ofile.dataset:
                #TODO: Method for this somewhere
                dsName = "/%s/%s/%s" % (dataset['PrimaryDataset'],
                                        dataset['ProcessedDataset'],
                                        dataset['DataTier'])
                ds = self.known_datasets.get(dsName, None)
                if not ds:
                    raise RuntimeError, 'Fileset %s unknown' % dsName
                ds.addFile(outputFile)
                ds.commit()
                filesFor.add(dsName)
                
        return tuple(filesFor)
    
    
    def jobFailure(self, jobReportFile):
        """
        Handle JobFailure
        mark file in error state
        """
        #TODO: - mark all inout files failed - what to do here?
        #TODO: how get subscription???
        
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
                
                sub = self.known_subs.get('%s:%s' % (report.workflowSpecId, report.jobType), None)
                if not sub:
                    raise RuntimeError, 'Subscription not found %s:%s' % (report.workflowSpecId, report.jobType)
                
                if report.jobType in ('Processing', 'Merge'):
                    failedFiles = [File(x['Lfn'], wmbs=self.wmbs).load() for x in report.skippedFiles]
                    sub.failFiles(failedFiles)
            except StandardError, ex:
                # If error on insert save for later retry
                logging.error("Failed to mark files for job as failed: %s" % str(ex))
                raise


    def createMergeWorkflow(self, procWorkflow):
        """
        create the merging workflow for a processing workflow
        """
        #create merge workflow
        mergeWorkflow = createMergeJobWorkflow(procWorkflow, 
                                isFastMerge = False, doCleanUp = False)
        
        # create workflows for each dataset
        for watchedDatasetName, mergeWF in mergeWorkflow.items():

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
            
            # save wf to wmbs
            workflow = Workflow(mergeWF.workflowName(), 
                                self.label, self.wmbs).create()
            self.new_workflows[mergeWF.workflowName()] = workflow
            
            #    //
            #  //     Insert datasets
            #//
        
            procDataset = Fileset(watchedDatasetName, self.wmbs).populate()
            
            fileset = Fileset(mergeWF.outputDatasets()[0].name(), self.wmbs, 
                              is_open=True, parents=[procDataset]).create()
            self.new_datasets[fileset.name] = fileset



            # set up subscription 
            #  - assume only 1 input dataset for the moment
            #  - assume parentage set by num input datasets move to workflow spec
            #TODO: Revisit this!!!
            sub = Subscription(procDataset, workflow=workflow,
                                            type='Merge', wmbs=self.wmbs)
            sub.create()
            self.new_subs['%s:%s' % (workflow.spec, 'Merge')] = sub
            logging.info('Merge subscription for %s on %s created' % 
                                    (workflow.spec, procDataset.name))

        return mergeWorkflow.values()


    def inputAvailable(self, fileset):
        return self.sendMessage('InputAvailable', fileset)
        
    def importFileset(self, fileset):
        return self.sendMessage('ImportFileset', fileset)
    
    def newWF(self, path):
        return self.sendMessage('NewWorkflow', path)