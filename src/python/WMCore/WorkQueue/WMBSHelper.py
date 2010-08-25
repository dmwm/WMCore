#!/usr/bin/env python
"""
Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""
__revision__ = "$Id: WMBSHelper.py,v 1.2 2009/05/11 16:34:59 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription

class WMBSHelper(Object):
    
    def __init__(self, wmSpec):        
        #TODO: 
        # 1. get the top level task.
        # 2. get the top level step and inpup
        # 3. generated the spec, owner, name from task
        # 4. get input file list from top level step
        # 5. generate the file set from work flow.
        self.wmSpec = wmSpec
        self.fileset = None
        self.workflow = None
        
        # un pickle the object 
        #self.wmSpec = unpickled wmspeck
        pass
    
    def createWorkflow(self, name):
        # create workflow
        # make up workflow name from task name 
        self.workflow = Workflow(self.wmSpec.specUrl, 
                                 self.wmSpec.owner, name, 
                                 self.wmSpec.topLevelTask)
        self.workflow.create()
        
        return self.workflow
    
    def createFilesset(self, name):
        # create fileset
        # make up fileset name from task name 
        self.fileset = Fileset(name)
        self.fileset.create()
        return self.fileset
        
        
    def createSubscription(self, filesetName=None, workflowName=None):
        """
        _createSubscription_
        
        create the wmbs subscription by a given fileset name and workflow name
        """
        if self.fileset == None:
            self.createFilesset(filesetName)
        if self.workflow == None:
            self.createWorkflow(workflowName)
        self.subscription = Subscription(fileset, workflow)
        return self.subscription
    
    def createFiles(self, dbsFiles, locations):
        """
        _createFiles_
        
        create wmbs files from given dbs files.
        as well as run lumi update
        """
        
        if type(dbsFiles) != list:
            dbsFiles = [dbsFiles]
        
        wmbsFileList = []
        for dbsFile in dbsFiles:    
            wmbsFile = File(lfn = dbsFile["LogicalFileName"], 
                            size = dbsFile["FileSize"], 
                            events = dbsFile["NumberOfEvents"], 
                            cksum = dbsFile["Checksum"],
                            parents = dbsFile["ParentList"],
                            locations = locations)
            wmbsFile.create()
            wmbsFileList.append()
        
        