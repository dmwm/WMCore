#!/usr/bin/env python
"""
_WMBSHelper_

Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""

__revision__ = "$Id: WMBSHelper.py,v 1.18 2010/03/30 20:04:46 sryu Exp $"
__version__ = "$Revision: 1.18 $"

import logging
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.Services.UUID import makeUUID
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

class WMBSHelper:

    def __init__(self, wmSpecName, wmSpecUrl, wmSpecOwner, taskName, 
                 taskType, whitelist, blacklist,blockName):
        #TODO: 
        # 1. get the top level task.
        # 2. get the top level step and input
        # 3. generated the spec, owner, name from task
        # 4. get input file list from top level step
        # 5. generate the file set from work flow.
       	self.wmSpecName = wmSpecName
        self.wmSpecUrl = wmSpecUrl
        self.wmSpecOwner = wmSpecOwner
        self.topLevelTaskName = taskName
        self.topLevelTaskType = taskType
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.block = blockName or None
        self.topLevelFileset = None
        self.topLevelSubscription = None
        #TODO: in case there are multiple top level tasks are
        # exist find the way to avoid loading wmspec multiple times
        wmspec = WMWorkloadHelper()
        # the url should be url from cache 
        # (check whether that is updated correctly in DB)
        wmspec.load(self.wmSpecUrl)    
        self.topLevelTask = wmspec.getTask(self.topLevelTaskName)

    def createSubscription(self):
        self.createTopLevelFilesset()
        return self._createChildSubscription(self.topLevelTask, self.topLevelFileset)
        
    def _createChildSubscription(self, task, fileset):
        # create workflow
        # make up workflow name from wmspec name
        workflow = Workflow(self.wmSpecUrl, self.wmSpecOwner, 
                                 self.wmSpecName,
                                 task.getPathName())
        workflow.create()
        subs = Subscription(fileset = fileset, workflow = workflow,
                            split_algo = task.jobSplittingAlgorithm(),
                            type = task.taskType())
        subs.create()
        if self.topLevelSubscription == None:
            self.topLevelSubscription = subs
        
        outputModules =  task.getOutputModulesForStep(task.getTopStepName())
        for outputModuleName in outputModules.listSections_():
            if task.taskType() == "Merge":
                outputFilesetName = "%s/merged-%s" % (task.getPathName(),
                                                      outputModuleName)
            else:
                outputFilesetName = "%s/unmerged-%s" % (task.getPathName(),
                                                        outputModuleName)
    
            outputFileset = Fileset(name = outputFilesetName)
            outputFileset.create()
    
            workflow.addOutput(outputModuleName, outputFileset)
    
            for childTask in task.childTaskIterator():
                if childTask.data.input.outputModule == outputModuleName:
                    self._createChildSubscription(childTask, outputFileset) 
        
        return self.topLevelSubscription

    def createTopLevelFilesset(self):
        # create fileset
        # make up fileset name from task name
        filesetName = ("%s-%s" % (self.wmSpecName, self.topLevelTaskName))
        if self.block:
            filesetName += "-%s" % self.block
        else:
            #create empty fileset for production job
            filesetName += "-%s" % makeUUID()
            
        self.topLevelFileset = Fileset(filesetName)
        self.topLevelFileset.create()
        return self.topLevelFileset


    def addMCFakeFile(self):
        mcFakeFileName = "MCFakeFile-%s" % makeUUID()
        wmbsFile = File(lfn = mcFakeFileName,
                        size = 0,
                        events = 0,
                        checksums = 0
                        )
        
        self.topLevelFileset.addFile(wmbsFile)
        self.topLevelFileset.commit()
        self.topLevelFileset.markOpen(False)
    
    def addFiles(self, dbsBlock):
        """
        _createFiles_
        
        create wmbs files from given dbs block.
        as well as run lumi update
        """

        for dbsFile in dbsBlock['Files']:
            self.topLevelFileset.addFile(self._convertDBSFileToWMBSFile(dbsFile, 
                                            dbsBlock['StorageElements']))
                    
        self.topLevelFileset.commit()
        self.topLevelFileset.markOpen(False)
        

    def _convertDBSFileToWMBSFile(self, dbsFile, storageElements):
        """
        There are two assumptions made to make this method behave properly,
        1. DBS returns only one level of ParentList.
           If DBS returns multiple level of parentage, it will be still get handled.
           However that might not be what we wanted. In that case, restrict to one level.
        2. Assumes parents files are in the same location as child files.
           This is not True in general case, but workquue should only select work only
           where child and parent files are in the same location  
        """
        wmbsParents = []
        
        for parent in dbsFile["ParentList"]:
            wmbsParents.append(self._convertDBSFileToWMBSFile(parent, storageElements))
        
        checksums = {}
        if dbsFile.get('Checksum'):
            checksums['cksum'] = dbsFile['Checksum']
        if dbsFile.get('Adler32'):
            checksums['adler32'] = dbsFile['Adler32']
            
        wmbsFile = File(lfn = dbsFile["LogicalFileName"],
                        size = dbsFile["FileSize"],
                        events = dbsFile["NumberOfEvents"],
                        checksums = checksums,
                        #TODO: need to get list of parent lfn
                        parents = wmbsParents,
                        locations = set(storageElements))
        
        logging.info("WMBS File: %s\n on Location: %s" 
                     % (wmbsFile['lfn'], wmbsFile['locations']))
        return wmbsFile