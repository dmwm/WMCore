#!/usr/bin/env python
"""
Default handler for add dataset to watch
"""
__all__ = []
__revision__ = "$Id: ReleaseWork.py,v 1.1 2009/05/08 15:32:33 sryu Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.WorkQueueManager.JobGenerator import JobGenerator
from WMCore.WorkQueueManager.WorkQueue import WorkQueue
from WMCore.WorkQueueManager.DBSHelper import DBSHelper

class ReleaseWork(BaseHandler):
    """
    Handler for getting message about available resource from resource manager.
    
    1. Select elements form WorkQueue according given top priority 
    2. If it is production job just create the job and jobgroup, 
       if not, download file information by block from dbs 
    
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # TODO set number of thread
        
     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        
        wq = WorkQueue()
        blocks = wq.getTopPriorityBlocks()
        for block in blocks:
            files = DBSHelper.getWMBSFiles(block)
            jobGen = JobGenerator()
            jobGen(files)
            wq.removeFromQueue(block)