#!/usr/bin/env python
"""
_WorkSpecParser_

A class that parses WMSpec files and provides relevant info
"""

__all__ = []
__revision__ = "$Id: WorkSpecParser.py,v 1.8 2009/06/25 16:41:30 sryu Exp $"
__version__ = "$Revision: 1.8 $"

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.MCPayloads.UUID import makeUUID

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper, newWorkload
from WMCore.WorkQueue.DataStructs.Block import Block

#TODO: Pull useful stuff out of wmspec then free it - large data structure
#TODO: Cleanup, logArchive etc. WorkflowTypes needed???

class WorkUnit:
    """
    Represents a chunk of work
    Either processing a block or a reasonable sized production request
    """
    def __init__(self, primaryBlock, blocks, jobs):
        self.primaryBlock = primaryBlock
        self.blocks = blocks
        self.jobs = jobs


globalDBS = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'

class WorkSpecParser:
    """
    Helper object to parse a WMSpec and return chunks of work
    """
    
    #TODO: Close wmspec file after pulling what we need
    def __init__(self, url):
        self.specUrl = url
        import pickle
        input = open(self.specUrl)
        self.wmSpec = pickle.load(input) #TODO: Replace by WMSpec load method
        self.initialTask = self.wmSpec.taskIterator().next()
        #self.dbsUrl = getattr(self.wmSpec, 'dbsUrl', 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')
        #self.dbs = DBSReader(self.dbsUrl)
        input.close()


#TODO: Resume this kind of thing at some point
#    def parse(self):
#        """Parse spec file & cache useful info"""
#        import pickle
#        input = open(self.specUrl)
#        spec = pickle.load(input) #TODO: Replace by WMSpec load method
#        initialTask = spec.taskIterator().next()
#        
#        self.validateWorkflow(spec, initialTask)
#        
#        # get useful stuff
#        params = self.specParams
#        params['whitelist'] = getattr(initialTask,
#                                      'constraints.sites.whitelist', ())
#        params['blacklist'] = getattr(initialTask,
#                                      'constraints.sites.blacklist', ())
#        params['priority'] = getattr(spec.data, 'Priority', 1)
#        params['dbs'] = getattr(spec.data, 'dbs', globalDBS)
#        params['input_datasets'] = getattr(initialTask.data.parameters,
#                                           'inputDatasets', ())
#        del initialTask, spec
#        input.close()
        

    def split(self, dbs_pool = None):
        """
        Take the wmspec and divide into units of work
        
        A unit of work corresponds to a significant 
          amount i.e. processing a block
        
        defaultBlockSize is used for WMSpecs that don't contain 
        splitting criteria i.e. Generation jobs
        """
        self.validateWorkflow()
        
        results = []
        #print self.wmSpec.name()
                
        #print "######### %s" % dbs_pool
        if not self.inputDatasets:
            # we don't have any input data - divide into one block
            jobs = self.__estimateJobs(self.splitSize, self.totalEvents)
            block = Block()
            block["Name"] = "ProductionBlock-%s" % makeUUID()
            block["NumFiles"] = 0
            block["NumEvents"] = self.totalEvents
            block["Size"] = 0
            results.append(WorkUnit(block, None, jobs))
            return results
        
        #print "######### %s" % self.dbs_url
        # data processing - assume blocks are reasonable size
        #Only run over closed blocks - may need to change this
        if dbs_pool and dbs_pool.has_key(self.dbs_url):
            dbs = dbs_pool[self.dbs_url]
        else:
            dbs = DBSReader(self.dbs_url)
        for dataset in self.inputDatasets:
            blocks = dbs.getFileBlocksInfo(dataset, onlyClosedBlocks=True)
            for block in blocks:
                #name = block['Name']
                if self.splitType == 'Event':
                    jobs = self.__estimateJobs(self.splitSize, block['NumEvents'])
                elif self.splitType == 'File':
                    jobs = self.__estimateJobs(self.splitSize, block['NumFiles'])
                else:
                    raise RuntimeError, 'Unsupported SplitType: %s' % self.splitType

                parentBlocks = None 
                if self.parentFlag:
                    #TODO: get parent blocks' info from dbs or is it returning dbs block? - needed for calculating jobs
                    parentBlocks = block['Parents']
                    if not parentBlocks:
                        msg = "Parentage required but no parents found for %s"
                        raise RuntimeError, msg % block['Name']
                else:
                    blocks = []
                results.append(WorkUnit(block, parentBlocks, jobs))
        return results


#    #Was used to split production into multiple blocks
#    #Now dont split these
#    def __split_no_input(self):
#        """
#        We have no input data - split by events
#        """
#        results = []
#        total = getattr(self.initialTask, 'totalEvents', 10000)
#        perJob = getattr(self.initialTask, 'splitSize', 10)
#        blockTotal = self.defaultBlockSize * perJob
#        count = 0
#        while total > 0:
#            jobs = self.__estimateJobs(perJob, blockTotal)
#            count += 1
#            total -= (jobs * perJob)
#            results.append(WorkUnit(str(count), None, (), jobs))
#            if total < blockTotal: blockTotal = total
#        return results
            

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

    
    def validateWorkflow(self):
        """Check necessary params set"""
        required = ('splitType', 'splitSize')
        for key in required:
            try:
                getattr(self, key)
            except AttributeError:
                msg = "Required parameter \'%s\' missing from %s" 
                raise RuntimeError, msg % (key, self.specUrl)
        for site in self.whitelist:
            if site in self.blacklist:
                msg = "Site \'%s\' in both white & black lists"
                raise RuntimeError, msg
        if self.inputDatasets:
            return self.validateForProcessing()
        else:
            return self.validateForProduction()

    
    def validateForProduction(self):
        """Check for needed production params"""
        if self.splitType != 'Event':
            msg = "splitType == %s, only \'Event\' valid for workflows with no input" % self.splitType
            raise RuntimeError, msg
        if not self.totalEvents:
            msg = "Production type workflow missing \'totalEvents\' parameter"
            raise RuntimeError, msg


    def validateForProcessing(self):
        """Check for needed processing params"""
        if self.totalEvents:
            msg = "Processing type workflow cannot have totalEvents parameter"
            raise RuntimeError, msg


#  //
# //     Helper functions for getting info out of a wm spec
#//

    def name(self):
        """wm spec name - should be unique"""
        return self.wmSpec.name()
    name = property(name)
    
    def owner(self):
        """wm spec owner - should be unique"""
        #TODO currently spec doesn't have owner property. - need to be added
        #return self.wmSpec.owner
        return "wmspecOwner"
    owner = property(owner)
    
    def topLevelTaskName(self):
        """topLevel task name name - should be unique"""
        return self.initialTask.name()
    topLevelTaskName = property(topLevelTaskName)
    
    def whitelist(self):
        """Site whitelist as defined in task"""
        return self.initialTask.data.constraints.sites.whitelist
    whitelist = property(whitelist)

  
    def blacklist(self):
        """Site blacklist as defined in task"""
        return self.initialTask.data.constraints.sites.blacklist
    blacklist = property(blacklist)


    def priority(self):
        """Return priority of workflow"""
        #return self.specParams['priority'] 
        return getattr(self.wmSpec, 'Priority', 1)
    priority = property(priority)
    

    def dbs_url(self):
        """Return dbsUrl"""
#        return self.specParams['dbs']
        #Throw if no dbs???
        return getattr(self.wmSpec.data, 'dbs', globalDBS)
    dbs_url = property(dbs_url)


    def inputDatasets(self):
        """Return input datasets"""
#        return self.specParams['input_datasets']
#        return self.simpleMemoize('input_datasets',
#                                  self.initialTask.data.parameters, 'inputDatasets', ())
        return getattr(self.initialTask.data.parameters, 'inputDatasets', ())
    inputDatasets = property(inputDatasets)
    
    
    def splitType(self, throw = True):
        """Return split type"""
        if not throw:
            return getattr(self.initialTask.data.parameters, 'splitType', None)
        return getattr(self.initialTask.data.parameters, 'splitType')
    splitType = property(splitType)


    def splitSize(self, throw = True):
        """Return SplitSize"""
        if not throw:
            return getattr(self.initialTask.data.parameters, 'splitSize', None)
        return getattr(self.initialTask.data.parameters, 'splitSize')
    splitSize = property(splitSize)


    def totalEvents(self):
        """Return total events"""
        return getattr(self.initialTask.data.parameters, 'totalEvents', None)
    totalEvents = property(totalEvents)


    def parents(self):
        """Do we need parents"""
        return getattr(self.initialTask.data.parameters, 'parentage', False)
    parentFlag = property(parents)

#    def simpleMemoize(self, name, obj, item, default = None):
#        """Poor mans memoize"""
#        try:
#            return self.specParams[name]
#        except IndexError:
#            if default:
#                value = getattr(obj, item, default)
#            else:
#                value = getattr(obj, item)
#            self.specParams[name] = value
#            return value
