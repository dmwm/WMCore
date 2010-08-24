#!/usr/bin/env python
"""
_FilesetJobFactory_

Given a processing workflow, generate a complete set of
job specs for it.


"""

import os
import logging

from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from WMCore.JobFactory.PileupDataset import createPileupDatasets

#class FilterSites:
#    """
#    Functor for filtering site lists
#    """
#    def __init__(self, allowed):
#        self.allowed = allowed
#    def __call__(self, object):
#        return object in self.allowed



class GeneratorMaker(dict):
    """
    _GeneratorMaker_

    Operate on a workflow spec and create a map of node name
    to CfgGenerator instance

    """
    def __init__(self):
        dict.__init__(self)


    def __call__(self, payloadNode):
        if payloadNode.cfgInterface != None:
            generator = CfgGenerator(payloadNode.cfgInterface, False,
                                     payloadNode.applicationControls)
            self[payloadNode.name] = generator
            return
            
        if payloadNode.configuration in ("", None):
            #  //
            # // Isnt a config file
            #//
            return
        try:
            generator = CfgGenerator(payloadNode.configuration, True,
                                         payloadNode.applicationControls)
            self[payloadNode.name] = generator
        except StandardError, ex:
            #  //
            # // Cant read config file => not a config file
            #//
            return
    
        



class FilesetJobFactory:
    """
    _DatasetJobFactory_

    Working from a processing Workflow template, generate
    a complete set of job spec files from it

    """
    def __init__(self, workflowSpec, workingDir, **args):
        self.workingDir = workingDir
        self.useInputDataset = None
        self.workflowSpec = workflowSpec

        self.count = args.get("InitialRun", 0)

        self.currentJob = None
        self.currentJobDef = None
        
        if args.has_key("InputDataset"):
            self.useInputDataset = args['InputDataset']
            # why not get from workflow spec ??? can we lose this ???

        self.allowedSites = []

        self.splitType = \
                self.workflowSpec.parameters.get("SplitType", "file").lower()
        self.splitSize = int(self.workflowSpec.parameters.get("SplitSize", 1))

        self.generators = GeneratorMaker()
        self.generators(self.workflowSpec.payload)

        self.pileupDatasets = {}
        
        #  //
        # // Does the workflow contain a site restriction??
        #//
        siteRestriction = \
           self.workflowSpec.parameters.get("OnlySites", None)          
        if siteRestriction != None:
            #  //
            # // restriction on sites present, populate allowedSites list
            #//
            msg = "Site restriction provided in Workflow Spec:\n"
            msg += "%s\n" % siteRestriction
            logging.info(msg)
            siteList = siteRestriction.split(",")
            for site in siteList:
                if len(site.strip()) > 0:
                    self.allowedSites.append(site.strip())
            
        #  //
        # // Cache Area for JobSpecs
        #//
        self.specCache = os.path.join(
            workingDir, 'Processing', self.workflowSpec.workflowName())
        if not os.path.exists(self.specCache):
            os.makedirs(self.specCache)
            

    def validateJobDef(self, jobDef):
        """
        Check the job def given fits with the workflow
        """
        
        # check all sites are permitted
        if self.allowedSites:
            for site in jobDef['SENames']:
                if site not in self.allowedSites:
                    raise RuntimeError, \
                            "Given site %s not in workflow whitelist" % site
        
    def __call__(self, jobDef):
        """
        _operator()_

        Generate the appropriate number of job specs based on the
        workflow and settings in the file

        """
        
        
        self.validateJobDef(jobDef)

        self.loadPileupDatasets()

        #result = []
        #for jobDef in self.processDataset():
        return self.createJobSpec(jobDef)
#        newJobSpec = self.createJobSpec(jobDef)
#            jobDict = {
#                "JobSpecId" : self.currentJob,
#                "JobSpecFile": newJobSpec,
#                "JobType" : "Processing",
#                "WorkflowSpecId" : self.workflowSpec.workflowName(),
#                "WorkflowPriority" : 10,
#                "Sites" : jobDef['SENames'],
#                }
#            result.append(jobDict)
#            self.count += 1
#            
#        return result

# May not need - original would never work
#    def overrideInputDataset(self, inputDataset):
#        """
#        _overrideInputDataset_
#
#        Set the name of the dataset in the workflow spec instance
#        and override it
#
#        """
#        self.useInputDataset = inputDataset
#        return

    def loadPileupDatasets(self):
        """
        _loadPileupDatasets_
        
        Are we dealing with pileup? If so pull in the file list
        
        """
        puDatasets = self.workflowSpec.pileupDatasets()
        if len(puDatasets) > 0:
            logging.info("Found %s Pileup Datasets for Workflow: %s" % (
                len(puDatasets), self.workflowSpec.workflowName(),
                ))
            self.pileupDatasets = createPileupDatasets(self.workflowSpec)
        return

    

#    def processDataset(self):
#        """
#        _processDataset_
#
#        Import the Dataset contents and create a set of jobs from it
#
#        """
#        
#        #  //
#        # // Now create the job definitions
#        #//
#        logging.debug("SplitSize = %s" % self.splitSize)
#        logging.debug("SplitType = %s" % self.splitType)
#
#        logging.debug("AllowedSites = %s" % self.allowedSites)
#        logging.debug("AllowedBlocks = %s" % self.allowedBlocks)
#        
#        if self.splitType == "event":
#            jobDefs = splitDatasetByEvents(self.inputDataset(),
#                                           self.dbsUrl, self.splitSize,
#                                           self.onlyClosedBlocks,
#                                           self.allowedSites,
#                                           self.allowedBlocks)
#            logging.debug("Retrieved %s job definitions split by event" % len(jobDefs))
#            
#        else:
#            jobDefs = splitDatasetByFiles(self.inputDataset(),
#                                          self.dbsUrl, self.splitSize,
#                                          self.onlyClosedBlocks,
#                                          self.allowedSites,
#                                          self.allowedBlocks)
#            logging.debug("Retrieved %s job definitions split by file" % len(jobDefs))
#        return jobDefs
    
        


    def inputDataset(self):
        """
        _inputDataset_

        Extract the input Dataset from this workflow

        """
        if self.useInputDataset != None:
            return self.useInputDataset
        topNode = self.workflowSpec.payload
        try:
            inputDataset = topNode._InputDatasets[-1]
        except StandardError, ex:
            msg = "Error extracting input dataset from Workflow:\n"
            msg += str(ex)
            logging.error(msg)
            return None

        return inputDataset.name()
        
            
    def createJobSpec(self, jobDef):
        """
        _createJobSpec_

        Load the WorkflowSpec object and generate a JobSpec from it

        """
        jobSpec = self.workflowSpec.createJobSpec()
        jobName = "%s-%s" % (
            self.workflowSpec.workflowName(),
            self.count,
            )

        logging.debug("Creating Job Spec: %s" % jobName)
        self.currentJob = jobName
        self.currentJobDef = jobDef
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = self.count


        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.operate(self.generateJobConfig)


        specCacheDir =  os.path.join(
            self.specCache, str(self.count // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)
        
        #  //
        # // Add site pref from job def
        #//
        if jobDef['SENames']:
            for site in jobDef['SENames']:
                jobSpec.addWhitelistSite(site)
        #  //
        # // Or if not present from workflow
        #//
        elif self.allowedSites:
            for site in self.allowedSites:
                jobSpec.addWhitelistSite(site)
             
        jobSpec.save(jobSpecFile)
        
        self.count += 1
        return jobSpecFile
        
        
    def generateJobConfig(self, jobSpecNode):
        """
        _generateJobConfig_
        
        Operator to act on a JobSpecNode tree to convert the template
        config file into a JobSpecific Config File
                
        """
        if jobSpecNode.name not in self.generators.keys():
            return

        generator = self.generators[jobSpecNode.name]
        
        
        maxEvents = self.currentJobDef.get("MaxEvents", None)
        skipEvents = self.currentJobDef.get("SkipEvents", None)
        
        args = {
            'fileNames' : self.currentJobDef.getOrderedLFNs(),
            }
            
        if self.splitType == "file":
           maxEvents = -1
        if maxEvents != None:
            args['maxEvents'] = maxEvents
        if skipEvents != None:
            args['skipEvents'] = skipEvents

        jobCfg = generator(self.currentJob, **args)
        #  //
        # // Is there pileup for this node?
        #//
        if self.pileupDatasets.has_key(jobSpecNode.name):
            puDataset = self.pileupDatasets[jobSpecNode.name]
            logging.debug("Node: %s has a pileup dataset: %s" % (
                jobSpecNode.name,  puDataset.dataset,
                ))
            
            fileList = puDataset.getPileupFiles(
                *self.currentJobDef.get("SENames", [])
                )
            jobCfg.pileupFiles = fileList
            
        #  //
        # // insert parent files
        #//
        parentFiles = self.currentJobDef.getOrderedParentLFNs()
        if parentFiles:
            logging.debug("Node: %s has SecondaryInputFiles: %s" % \
                                        (jobSpecNode.name, parentFiles))
            jobCfg.addExtension("SecondaryInputFiles", parentFiles)
        
        jobSpecNode.cfgInterface = jobCfg
        return
    


    

