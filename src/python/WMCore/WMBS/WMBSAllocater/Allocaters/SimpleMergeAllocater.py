#!/usr/bin/env python

import logging
import time
import os

from WMCore.WMBS.WMBSAllocater.AllocaterImpl import AllocaterImpl 
from WMCore.WMBS.WMBSAllocater.Registry import registerAllocaterImpl

from ProdCommon.MCPayloads.LFNAlgorithm import mergedLFNBase, DefaultLFNMaker, JobSpecLFNMaker
from WMCore.JobFactory.JobDefinition import JobDefinition
from WMCore.WMBS.WMBSAllocater.AllocaterImpl import hashList

class SimpleMergeAllocater(AllocaterImpl):
    """
    Simple Merge job allocater - ignore 0 event files
    """
    
#    def __init__(self, ms, specdir):
#        AllocaterImpl.__init__(self, ms, specdir)
        
        
    def allocate(self, files):
        
        tempSize = 0
        tempFiles = []
        takenIndices = []
        #takenFilesByJob = []
        #filesTaken = []
        jobs = []
        
        startingFile = -1 #0
        #for file in files:
        while startingFile < len(files) - 1:
            startingFile = startingFile + 1
            
            # skip files taken in an earlier pass
            if startingFile in takenIndices:
                #startingFile = startingFile + 1
                continue
            
            file = files[startingFile]
            
            
            #TODO: Tidy this up - maybe move into JobSplitter package?
            
            rightIndex = -1 #0
            while rightIndex < len(files) - 1:
                rightIndex = rightIndex + 1
            
                # skip files taken in an earlier pass
                if rightIndex in takenIndices:
                    continue

                file = files[rightIndex]
            
                # Would dropping 0 event files upset provenance info?
#                if not file.event:
#                    logging.info("Files %s has 0 events - ignore" % file.name)
#                    # mark as complete - even though will send no jobs for it
#                    tempFiles.append(file)
#                    takenIndices.append(rightIndex)
#                    continue
                
#            if file.size > self.args['mergeMaxSize']:
#                logging.error("File %s too large (%s > %s) will not be merged"\
#                              % (file.name, file.size, self.args['mergeMaxSize']))
#                startingFile = startingFile + 1
#                continue
            
                # can we add it?
                if tempSize + file.size < self.args['mergeMaxSize']:
                    tempSize = tempSize + file.size
                    tempFiles.append(file)
                    takenIndices.append(rightIndex)
                    
                # end inner loop
                
            # check file closure conditions or force merge
            # TODO: if all files in run complete force merge
            if (tempSize > ['mergeMinSize'] and \
                tempSize < ['mergeMaxSize']) or \
                self.force:
                    
                    # create job
                    #filesTaken.extend(tempFiles)
                    #self.jobsCreated.append(self.createJob(tempFiles))
                    #self.filesPerJob.append(tempFiles)
                    #takenFilesByJob.append(tempFiles)
                    job = JobDefinition()
                    tempFiles.sort()
                    job['Files'] = tempFiles
                    job['SENames'] = files[0].locations
                    jobs.append(job)
                    
                    # reset loop
                    tempSize = 0
                    tempFiles = []
                
        return jobs #, filesTaken
    

    
    def createJob(self, job):
        """
        create merge job
        """

        jobId =  "%s-%s-mergejob-%s" % (self.subscription.workflow.name, \
                                        hashList(job['SENames']), \
                                        time.time())
        jobSpec = self.spec.createJobSpec()
        jobSpec.setJobName(jobId)
        jobSpec.setJobType("Merge")

        # add SE list
        [jobSpec.addWhitelistSite(site) for site in job['SENames']]

        # get PSet
        cfg = jobSpec.payload.cfgInterface

        # set output module
        outModule = cfg.getOutputModule('Merged')

        # compute LFN group based on merge jobs counter
        #group = str(status['mergedjobs'] // 1000).zfill(4)
        #TODO: Change this - much too heavy a call
        rough_num = len(self.subscription.completedFiles()) + \
                    len(self.subscription.failedFiles()) + \
                    len(self.subscription.acquiredFiles()) + 1
        group = str(rough_num // 1000).zfill(4)
        jobSpec.run = rough_num #TODO: is this correct?
        #jobSpec.parameters[''] = rough_num

        # set output file name
        #tier = outModule['dataTier']
        fileName = "%s-%s-%s.root" % (outModule['primaryDataset'],
                                      rough_num,
                                      outModule['dataTier'])
        cfg.outModule['fileName'] = fileName
        #TODO: find out why this is here....
        # make sure masons LFN changes are reproduced
        
#        baseFileName = "%s-%s-%s.root" % (dataset[0], jobSpec.run, tier)
#        outModule['fileName'] = baseFileName 
#        lfnMaker = DefaultLFNMaker(jobSpec)
#        lfnMaker(jobSpec.payload)
        lfnMaker = JobSpecLFNMaker(jobId, rough_num)
        lfnMaker(jobSpec.payload)
        
#        prim=properties['primaryDataset']
#        tier=properties['dataTier']
#        lastBit=properties['processedDataset']

        #acqEra=None
        #if .has_key("AcquisitionEra"):



        #TODO: get LFN from mergedLFNBase

#        acqEra = self.spec.parameters.get("AcquisitionEra", None)
#        remainingBits=lastBit
#        if acqEra != None:
#          thingtoStrip="%s_" % acqEra
#          mypieces=lastBit.split(thingtoStrip,1)
#          if len(mypieces)>1:  
#            remainingBits=mypieces[1].split("-unmerged",1)[0]
#          else:
#            remainingBits=lastBit 
#
#        extendedlfnBase = os.path.join(lfnBase,prim,tier,remainingBits,group)
#        baseFileName = "%s-%s-%s.root" % (dataset[0], outputFile, tier)
#        outModule['fileName'] = baseFileName
#        outModule['logicalFileName'] = os.path.join(extendedlfnBase, baseFileName)
        
        # set output catalog
        outModule['catalog'] = "%s-merge.xml" % jobId
        
        # set input module
        # get input file names (expects a trivial catalog on site)
        cfg.inputFiles = [file.lfn for file in job['Files']]
        
        # target file name
        mergeJobSpecFile = os.path.join(self.specdir, 'Merge',
                                self.subscription.workflow.name,
                                '%s-spec.xml' % jobId)
        if not os.path.exists(os.path.dirname(mergeJobSpecFile)):
            os.mkdir(os.path.dirname(mergeJobSpecFile))

        # save job specification
        jobSpec.save(mergeJobSpecFile)

        return mergeJobSpecFile

    
registerAllocaterImpl('Merge', SimpleMergeAllocater)
#registerAllocaterImpl(SimpleMergeAllocater.__name__, \
#                                SimpleMergeAllocater)