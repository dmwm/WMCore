#!/usr/bin/env python

import logging

from WMCore.WMBS.WMBSAllocater.AllocaterImpl import AllocaterImpl 
from WMCore.WMBS.WMBSAllocater.Registry import registerAllocaterImpl

from WMCore.JobFactory.JobSplitter import splitByFiles, splitByEvents
from WMCore.JobFactory.FilesetJobFactory import FilesetJobFactory

class SimpleFileProcessingAllocater(AllocaterImpl):
    """
    Create processing jobs split either by files or events
    """
    
#    def __init__(self, ms, specdir):
#        AllocaterImpl.__init__(self, ms, specdir)
        
        
    def allocate(self, files):
        
        splitType = self.spec.parameters.get("SplitType", "file").lower()
        splitSize = self.spec.parameters.get("SplitSize", 1)
        
        if splitType == "event":
            jobDefs = splitByEvents(files, int(splitSize),
                        self.siteWhiteList, self.force)
            logging.debug("Retrieved %s job definitions split by event" % \
                                                                 len(jobDefs))
        elif splitType == "file":
            jobDefs = splitByFiles(files, int(splitSize),
                        self.siteWhiteList, self.force)
            logging.debug("Retrieved %s job definitions split by file" % \
                                                                len(jobDefs))
        else:
            raise RuntimeError, "Workflow SplitType %s unknown" % splitType
        
        return jobDefs
    
    
    def createJob(self, job):
        
        # bit hacky - anywhere else to put this??
        # maybe merge specs need some kind of factory also - help make run numbers correct?
        if not hasattr(self, 'jobFactory'):
            self.jobFactory = FilesetJobFactory(self.spec, self.specdir) # any other args???
        
        return self.jobFactory(job)
    
    
    
    
registerAllocaterImpl('Processing', \
                                SimpleFileProcessingAllocater)