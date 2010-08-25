#!/usr/bin/env python
"""
Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""
__revision__ = "$Id: WMBSHelper.py,v 1.1 2009/05/11 11:49:35 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription

class WMBSHelper(Object):
    
    def __init__(self, url):        
        #TODO: 
        # 1. get the top level task.
        # 2. get the top level step and inpup
        # 3. generated the spec, owner, name from task
        # 4. get input file list from top level step
        # 5. generate the file set from work flow.
        self.specUrl = url
        # un pickle the object 
        
        self.wmSpec = unpickled wmspeck
        pass
    
    def createWorkflow(self):
        # create workflow
        # make up workflow name from task name 
        wmbsWorkflow = Workflow(self.specUrl, owner, name, taskName)
        wmbsWorkflow.create()
        
        return wmbsWorkflow
        
    def createFilesset(self):
        # create fileset
        # make up fileset name from task name 
        wmbsFileset = Fileset(name)
        wmbsFileset.create()
        
        
    def createSubscription(self):
        wmbsSubscription = Subscription(fileset, workflow)
        pass
    
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
        
        