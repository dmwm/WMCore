#!/usr/bin/env python
"""
_WorkSpecParser_

A class that parses WMSpec files and provides relevant info
"""

__all__ = []
__revision__ = "$Id: WorkSpecParser.py,v 1.2 2009/05/12 16:41:15 swakef Exp $"
__version__ = "$Revision: 1.2 $"

import pickle
from ProdCommon.DataMgmt.DBS import DBSReader


class WorkSpecParser(Object):
    """
    Helper object to parse a WMSpec and return chunks of work
    """
    
    def __init__(self, url, defaultBlockSize=100):
        self.specUrl = url
        self.wmSpec = pickle.load(open(self.specUrlurl)) #TODO: Replace by WMSpec load method
        self.initialTask = self.wmSpec.taskIterator()
        self.dbs = DBSReader(self.wmSpec.dbsUrl)
        self.defaultBlockSize = defaultBlockSize
        self.results = [] # [name (block or fake), [blocks], jobs] 
        self.__split()


    def __split(self):
        """
        Take the wmspec and divide into units of work
        
        A unit of work corresponds to a significant 
          amount i.e. processing a block
        
        defaultBlockSize is used for WMSpecs that don't contain 
        splitting criteria i.e. Generation jobs
        """
        # job split constraints
        splitType = self.initialTask.get('splitType', 'File')
        splitSize = self.initialTask.get('splitSize', 1)
        
        
        if not self.initialTask.inputDatasets():
            # we don't have any input data - divide into blocks of default size
            return self.__split_no_input()
            
        # data processing - assume blocks are reasonable size so queue them
        for dataset in self.initialTask.inputDatasets():
            for block in self.dbs.getBlocks(dataset):
                if splitType == 'Event':
                    jobs = self.__estimateJobs(splitSize, block['NumEvents'])
                elif splitType == 'File':
                    jobs = self.__estimateJobs(splitSize, block['NumFiles'])
                else:
                    raise RuntimeError, 'Unsupported SplitType: %s' % splitType
                
                #TODO: Get parentage list etc.
                self.results.append((block['Name'], block['Name'], jobs))


    def __split_no_input(self):
        """
        We have no input data - split by events
        """
        total = self.initialTask.totalEvents()
        perJob = self.initialTask.get('splitSize', 100)
        # have to be splitting by event
#        while total:
#            self.results.append(('', (), eventsPerBlock / eventsPerJob))
#            if total < eventsPerBlock:
#                eventsPerBlock = total
        count = 0
        while total > 0:
            jobs = self.__estimateJobs(self.defaultBlockSize * perJob, total)
            #count += 1
            total -= jobs * perJob
        #for i in range(self.__estimateJobs(self.defaultBlockSize, total)):
            self.results.append((str(i), (), jobs))
            

    def __estimateJobs(self, unit, total):
        """
        Estimate the number of jobs resulting from a block of work
        """
        #TODO: Possibility to run JobSplitting in DryRun mode, need changes
        # there for this though. Also maybe unessecary as subscriptions need 
        # a fileset setup etc... might be able to fake without persisting in db though...
        # for now fake this
        count = 0
        while total:
            count += 1
            if total < eventsPerBlock:
                eventsPerBlock = total
        return count


    def __iter__(self):
        """
        Take the wmspec and divide into units of work
        
        A unit of work corresponds to a significant 
          amount i.e. processing a block
        
        defaultBlockSize is used for WMSpecs that don't contain 
        splitting criteria i.e. Generation jobs
        
        """
#        while self.results:
#            yield self.results.pop()
        return self.results.__iter__()
    
    
    def siteWhitelist(self):
        """
        Site whitelist as defined in task
        """
        return self.initialTask.constraints.sites.whitelist
    
  
    def siteBlacklist(self):
        """
        Site blacklist as defined in task
        """
        return self.initialTask.constraints.sites.blacklist
    

    def priority(self):
        """
        Return priority of workflow
        """
        return self.wmSpec.get('Priroity', 1)
      