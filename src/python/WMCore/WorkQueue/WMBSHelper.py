#!/usr/bin/env python
"""
_WMBSHelper_

Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""

__revision__ = "$Id: WMBSHelper.py,v 1.13 2009/12/16 17:45:42 sfoulkes Exp $"
__version__ = "$Revision: 1.13 $"

from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.Services.UUID import makeUUID

class WMBSHelper:

    def __init__(self, wmSpecName, wmSpecUrl, wmSpecOwner, taskName, 
                 whitelist, blacklist,blockName):
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
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.block = blockName or None
        self.fileset = None
        self.workflow = None
        self.subscription = None

    def createWorkflow(self):
        # create workflow
        # make up workflow name from wmspec name
        self.workflow = Workflow(self.wmSpecUrl, self.wmSpecOwner, 
                                 self.wmSpecName,
                                 self.topLevelTaskName)
        self.workflow.create()

        return self.workflow

    def createFilesset(self):
        # create fileset
        # make up fileset name from task name
        filesetName = ("%s-%s" % (self.wmSpecName, self.topLevelTaskName))
        if self.block:
            filesetName += "-%s" % self.block
        else:
            #create empty fileset for production job
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
                                         whitelist = self.whitelist,
                                         blacklist = self.blacklist)
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
                            locations = set(locations))
            wmbsFile.create()
            fileset.addFile(wmbsFile)
        fileset.commit()
