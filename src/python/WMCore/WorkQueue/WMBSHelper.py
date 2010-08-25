#!/usr/bin/env python
"""
Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""
__revision__ = "$Id: WMBSHelper.py,v 1.9 2009/09/24 20:17:11 sryu Exp $"
__version__ = "$Revision: 1.9 $"
from sets import Set

from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.Services.UUID import makeUUID
from WMCore.WMSpec.WMWorkload import getWorkloadFromTask

class WMBSHelper:

    def __init__(self, task, blockName):
        #TODO: 
        # 1. get the top level task.
        # 2. get the top level step and input
        # 3. generated the spec, owner, name from task
        # 4. get input file list from top level step
        # 5. generate the file set from work flow.
        self.wmSpec = getWorkloadFromTask(task)
        self.topLevelTask = task
        self.block = blockName or None
        self.fileset = None
        self.workflow = None
        self.subscription = None

    def createWorkflow(self):
        # create workflow
        # make up workflow name from task name
        workflowName = self.wmSpec.name()
        print "******** %s" % self.wmSpec.specUrl
        #if self.workflow == None:
        self.workflow = Workflow(self.wmSpec.specUrl,
                             self.wmSpec.owner(), workflowName,
                             self.topLevelTask.name())
        self.workflow.create()

        return self.workflow

    def createFilesset(self):
        # create fileset
        # make up fileset name from task name
        filesetName = ("%s-%s" % (self.wmSpec.name(), self.topLevelTask.name()))
        if self.block:
            filesetName += "-%s" % self.block
        else:
            filesetName += "-%s" % makeUUID()
        self.fileset = Fileset(filesetName)
        self.fileset.create()
        return self.fileset


    def createSubscription(self):
        """
        _createSubscription_
        
        create the wmbs subscription by a given fileset name and workflow name
        """
        #if self.subscription == None:
        self.createFilesset()
        self.createWorkflow()
        self.subscription = Subscription(self.fileset, self.workflow,
                                         whitelist = self.topLevelTask.siteWhitelist(),
                                         blacklist = self.topLevelTask.siteBlacklist())
        self.subscription.create()
        return self.subscription

    def addFiles(self, dbsFiles, locations):
        """
        _createFiles_
        
        create wmbs files from given dbs files.
        as well as run lumi update
        """

        if type(dbsFiles) != list:
            dbsFiles = [dbsFiles]

        fileset = self.createFilesset()
        for dbsFile in dbsFiles:
            wmbsFile = File(lfn = dbsFile["LogicalFileName"],
                            size = dbsFile["FileSize"],
                            events = dbsFile["NumberOfEvents"],
                            cksum = dbsFile["Checksum"],
                            parents = dbsFile["ParentList"],
                            locations = Set(locations))
            wmbsFile.create()
            fileset.addFile(wmbsFile)
        fileset.commit()
