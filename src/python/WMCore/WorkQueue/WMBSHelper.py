#!/usr/bin/env python
"""
Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""
__revision__ = "$Id: WMBSHelper.py,v 1.5 2009/08/12 17:10:11 sryu Exp $"
__version__ = "$Revision: 1.5 $"
from sets import Set

from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription

class WMBSHelper:
    
    def __init__(self, wmSpec):        
        #TODO: 
        # 1. get the top level task.
        # 2. get the top level step and inpup
        # 3. generated the spec, owner, name from task
        # 4. get input file list from top level step
        # 5. generate the file set from work flow.
        self.wmSpec = wmSpec
        self._fileset = None
        self._workflow = None
        self._subscription = None
        
    def createWorkflow(self):
        # create workflow
        # make up workflow name from task name
        if self._workflow == None:
            workflowName = ("%s-%s-%s" % (self.wmSpec.name, self.wmSpec.topLevelTaskName,
                                         self.wmSpec.owner))
            self._workflow = Workflow(self.wmSpec.specUrl, 
                                     self.wmSpec.owner, workflowName, 
                                     self.wmSpec.topLevelTaskName)
            self._workflow.create()
            
        return self._workflow
    
    def createFilesset(self):
        if self._fileset == None:
            # create fileset
            # make up fileset name from task name 
            filesetName = ("%s-%s" % (self.wmSpec.name, self.wmSpec.topLevelTaskName))
            self._fileset = Fileset(filesetName)
            self._fileset.create()
        return self._fileset
        
        
    def createSubscription(self):
        """
        _createSubscription_
        
        create the wmbs subscription by a given fileset name and workflow name
        """
        if self._subscription == None:
            fileset = self.createFilesset()
            workflow = self.createWorkflow()
            self._subscription = Subscription(fileset, workflow)
            self._subscription.create()
        return self._subscription
    
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
        
        