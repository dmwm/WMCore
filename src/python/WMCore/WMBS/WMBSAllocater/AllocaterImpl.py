#!/usr/bin/env python

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from WMCore.JobFactory.JobDefinition import JobDefinition

import os
import logging

def hashList(items):
    """
    _hashList_

    Sort and string hash a list of sites so that lists of the
    same sites can be grouped in  dictionary
    
    """
    items.sort()
    return '#'.join(items)


class AllocaterImpl:
    """
    Interface class for WMBS Allocaters
    """
    
    
    def __init__(self, ms, specdir, **args):
        self.ms = ms
        self.specdir = specdir
        self.subscription = None
        self.filesBySite = {}
        self.args = args
        self.spec = None
        self.siteWhiteList = None

    def allocate(self, files):
        """
        Function that implementations will override
        to allocate files to jobs for a site
        """
        raise NotImplementedError, "allocate"


    def createJob(self, sites, files):
        """
        Function that implementations will override
        to create jobs for the given files
        """
        raise NotImplementedError, "createJob"


    def __call__(self, subscription, files, force=False):
        """
        takes in a list of files then match to jobs
        
        TODO: Will likely need changing for DQM, per job jobs
        """
        self.subscription = subscription
        
        # do we want to force job creation (ala ForceMerge)
        self.force = force or not self.subscription.fileset.open
        
        self.spec = WorkflowSpec()
        try:
            self.spec.load(self.subscription.workflow.spec)
        except Exception, msg:
            logging.error("Cannot read workflow file: " + str(msg))
            raise
        
        # which sites are we using in this workflow?
        self.populateSiteWhiteList()
        # group files at same sites
        self.filesBySite = self.arrangeFilesBySites(files)
        
        created = []
        filesTaken = []
        
        #   //
        # //  Loop over files at same sites
        #//
        for filesAtASite in self.filesBySite.items():
            
            #   //
            # //  divide files into jobs and create job specs
            #//
            #jobs = self.allocate(filesAtASite)
            for job in self.allocate(filesAtASite):
            #for job in jobs:
            #TODO: Here or in File.py?
                #lambda x, y: x.run != y.run and cmp(x.run, y.run) or cmp(x.lumi, y.lumi)
                job['Files'].sort() # cmssw prefers files in run/lumi order
                created.append(self.createJob(job))
                filesTaken.extend(job['Files'])
        
        # mark taken files in wmbs
        subscription.acquireFiles(filesTaken)
        
        # return created job specs
        return created
    
    
    def populateSiteWhiteList(self):
        """
        Which sites are we using?
        """
        siteRestriction = self.spec.parameters.get("OnlySites", None)
        if siteRestriction != None:
            self.siteWhiteList = []
            msg = "Site restriction provided in Workflow Spec:\n"
            msg += "%s\n" % siteRestriction
            logging.info(msg)
            siteList = siteRestriction.split(",")
            for site in siteList:
                if len(site.strip()) > 0:
                    self.siteWhiteList.append(site.strip())
    
    
    def arrangeFilesBySites(self, files):
        """
        arrange files into a dict with the key being the hash
        of all the hosting sites or the white listed subset if given
        """
        result = {}
        for file in files:
            locations = []
            if self.siteWhiteList:
                for site in file.locations:
                    if site in self.siteWhiteList:
                        locations.append(site)
                if not locations:
                    logging.error("Ignoring file not at whitelist site: %s" % \
                                                                    file.name)
                    continue
            else:
                locations = file.locations

            file.locations = locations
            result.setdefault(hashList(locations), []).append(file)
        return result
    
