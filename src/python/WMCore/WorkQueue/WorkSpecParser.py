#!/usr/bin/env python
"""
_WorkSpecParser_

A class that parses WMSpec files and provides relevant info
"""

__all__ = []
__revision__ = "$Id: WorkSpecParser.py,v 1.4 2009/05/28 17:14:31 swakef Exp $"
__version__ = "$Revision: 1.4 $"

import pickle
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper, newWorkload


class WorkUnit:
    """
    Represents a chunk of work
    Either processing a block or a reasonable sized production request
    """
    def __init__(self, name, primaryBlock, blocks, jobs):
        self.name = name
        self.primaryBlock = primaryBlock
        self.blocks = blocks
        self.jobs = jobs


class WorkSpecParser:
    """
    Helper object to parse a WMSpec and return chunks of work
    """
    
    def __init__(self, url, defaultBlockSize=100):
        self.specUrl = url
        self.wmSpec = pickle.load(open(self.specUrl)) #TODO: Replace by WMSpec load method
        self.initialTask = self.wmSpec.taskIterator().next()
        self.dbsUrl = getattr(self.wmSpec, 'dbsUrl', 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')
        self.dbs = DBSReader(self.dbsUrl)
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
        splitType = getattr(self.initialTask, 'splitType', 'File')
        splitSize = getattr(self.initialTask, 'splitSize', 1)
        
        
        if not getattr(self.initialTask, 'inputDatasets', ()):
            # we don't have any input data - divide into blocks of default size
            return self.__split_no_input()
            
        # data processing - assume blocks are reasonable size so queue them
        #TODO: Only run over closed blocks - probably need to change this
        for dataset in self.initialTask.inputDatasets():
            blocks = self.dbs.getFileBlocksInfo(dataset, onlyClosedBlocks=True)
            for block in blocks:
                name = block['Name']
                if splitType == 'Event':
                    jobs = self.__estimateJobs(splitSize, block['NumEvents'])
                elif splitType == 'File':
                    jobs = self.__estimateJobs(splitSize, block['NumFiles'])
                else:
                    raise RuntimeError, 'Unsupported SplitType: %s' % splitType
                
                if bool(getattr(self.initialTask, 'Parents', "False")):
                    blocks = block['Parents']
                else:
                    blocks = []
                self.results.append(WorkUnit(name, name, blocks, jobs))



    def __split_no_input(self):
        """
        We have no input data - split by events
        """
        total = getattr(self.initialTask, 'totalEvents', 10000)
        perJob = getattr(self.initialTask, 'splitSize', 10)
        blockTotal = self.defaultBlockSize * perJob
        count = 0
        while total > 0:
            jobs = self.__estimateJobs(perJob, blockTotal)
            count += 1
            total -= (jobs * perJob)
            self.results.append(WorkUnit(str(count), None, (), jobs))
            if total < blockTotal: blockTotal = total
            

    def __estimateJobs(self, unit, total):
        """
        Estimate the number of jobs resulting from a block of work
        """
        #TODO: Possibility to run JobSplitting in DryRun mode, need changes
        # there for this though. Also maybe unnecessary as subscriptions need 
        # a fileset setup etc... might be able to fake without persisting in db though...
        # for now fake this
        count = 0
        while total > 0:
            count += 1
            total -= unit
#            if total < unit:
#                eventsPerBlock = total
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
        return getattr(self.initialTask, 'constraints.sites.whitelist', ())
    
  
    def siteBlacklist(self):
        """
        Site blacklist as defined in task
        """
        return getattr(self.initialTask, 'constraints.sites.blacklist', ())
    

    def priority(self):
        """
        Return priority of workflow
        """
        return getattr(self.wmSpec, 'Priroity', 1)
      