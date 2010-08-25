#!/usr/bin/env python
"""
_BossLiteDBWM_

"""

__version__ = "$Id: BossLiteDBWM.py,v 1.12 2010/05/21 12:19:33 spigafi Exp $"
__revision__ = "$Revision: 1.12 $"

import threading

from WMCore.BossLite.Common.Exceptions  import DbError

from WMCore.BossLite.DbObjects.Task         import Task
from WMCore.BossLite.DbObjects.Job          import Job
from WMCore.BossLite.DbObjects.RunningJob   import RunningJob

from WMCore.BossLite.API.BossLiteDBInterface    import BossLiteDBInterface
from WMCore.WMConnectionBase    import WMConnectionBase

def dbTransaction(func):
    """
    Basic transaction decorator function
    """
    
    def wrapper(self, *args, **kwargs):
        """
        Decorator for db transaction
        """
        
        self.existingTransaction = self.engine.beginTransaction()
        try:
            res = func(self, *args, **kwargs)
            self.engine.commitTransaction(self.existingTransaction)
        except Exception, ex:
            msg = "Failure in TrackingDB class"
            msg += str(ex)
            # Is this correct?
            myThread = threading.currentThread()
            """
            ---> pylint error: 
            E:42:dbTransaction.wrapper: Instance of '_DummyThread' has no 
               'transaction' member (but some types could not be inferred)
            """
            myThread.transaction.rollback()
            raise DbError(msg)        
        return res
    return wrapper

class BossLiteDBWM(BossLiteDBInterface):
    """
    _BossLiteDBWM_
    
    This class is *strongly* specialized to use WMCore DB back-end
    """

    def __init__(self):
        """
        __init__
        """

        super(BossLiteDBWM, self).__init__()
                                  
        # Initialize WMCore database ...
        self.engine = WMConnectionBase(daoPackage = "WMCore.BossLite")

        self.existingTransaction = None
        
        return
    
    
    ##########################################################################
    # Methods for DbObjects (basic)
    ##########################################################################
    
    @dbTransaction
    def objExists(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            action = self.engine.daofactory(classname = 'Task.Exists')
            binds = {'name': obj.data['name']}
            
        elif type(obj) == Job :
            action = self.engine.daofactory(classname = "Job.Exists")
            binds = {'name': obj.data['name']}
            
        elif type(obj) == RunningJob :
            action = self.engine.daofactory(classname = "RunningJob.Exists")
            binds = {'jobId': obj.data['jobId'], 
                     'taskId': obj.data['taskId'], 
                     'submission': obj.data['submission'] }
            
        else :
            raise NotImplementedError
        
        tmpId = action.execute(binds = binds,
                               conn = self.engine.getDBConn(),
                               transaction = self.existingTransaction)
        
        return tmpId
        
    
    @dbTransaction
    def objSave(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            action = self.engine.daofactory(classname = 'Task.Save')
        
        elif type(obj) == Job :
            action = self.engine.daofactory(classname = "Job.Save")
        
        elif type(obj) == RunningJob :
            action = self.engine.daofactory(classname = "RunningJob.Save")
                
        else :
            raise NotImplementedError    
        
        action.execute(binds = obj.data,
                       conn = self.engine.getDBConn(),
                       transaction = self.existingTransaction)
        
        return
    
    
    @dbTransaction
    def objCreate(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            action = self.engine.daofactory(classname = 'Task.New')
            
        elif type(obj) == Job :
            action = self.engine.daofactory(classname = "Job.New")
            
        elif type(obj) == RunningJob :
            action = self.engine.daofactory(classname = "RunningJob.New")
            
        else :
            raise NotImplementedError 
        
        action.execute(binds = obj.data,
                       conn = self.engine.getDBConn(),
                       transaction = self.existingTransaction)
        
        return
    
    
    @dbTransaction
    def objLoad(self, obj, classname = None):
        """
        put your description here
        """

        if type(obj) == Task :
            
            if classname == 'Task.GetJobs' :
                binds = {'taskId' : obj.data['id'] }
                
                action = self.engine.daofactory( classname = classname )
                result = action.execute(binds = binds,
                                        conn = self.engine.getDBConn(),
                                        transaction = self.existingTransaction)
                
                return result
            
            elif obj.data['id'] > 0:
                classname = "Task.Load"
                binds = {'id' : obj.data['id'] }
                
            elif obj.data['name']:
                classname = "Task.Load"
                binds = {'name' : obj.data['name'] }
                
            else:
                # Then you're screwed
                return []
        
        elif type(obj) == Job :
            
            if obj.data['id'] > 0:
                binds = { 'id' : obj.data['id'] }
            
            elif obj.data['jobId'] > 0 and obj.data['taskId'] > 0:
                binds = { 'job_id' : obj.data['jobId'],
                          'task_id' : obj.data['taskId'] }
                
            elif obj.data['name']:
                binds = { 'name' : obj.data['name'] }
                
            else:
                # We have no identifiers.  We're screwed
                # this branch doesn't exist
                return []
        
        elif type(obj) == RunningJob :
            
            if (obj.data['jobId'] and obj.data['taskId'] and \
                                                obj.data['submission']) :
                binds = {'task_id' : obj.data['taskId'],
                         'job_id' : obj.data['jobId'],
                         'submission' : obj.data['submission'] }
                
            elif obj.data['id'] > 0:
                binds = {'id' : obj.data['id'] } 
                
            else:
                # We have nothing
                return []
            
        else :
            raise NotImplementedError        
        
        return self.objAdvancedLoad(obj = obj, binds = binds)
    
    
    @dbTransaction
    def objUpdate(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            raise NotImplementedError
        
        elif type(obj) == Job :
            raise NotImplementedError
        
        elif type(obj) == RunningJob :
            raise NotImplementedError
        
        else :
            raise NotImplementedError        
        
        return
    
    
    @dbTransaction
    def objRemove(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            action = self.engine.daofactory(classname = 'Task.Delete')
            
            # verify data is complete
            if not obj.valid(['id']):
                binds = {'name' : obj.data['name'] }
            else :
                binds = {'id' : obj.data['id'] }
        
        elif type(obj) == Job :
            action = self.engine.daofactory(classname = "Job.Delete")
            
            # verify data is complete
            if not obj.valid(['id']):
                binds = {'name' : obj.data['name'] }
            else :
                binds = {'id' : obj.data['id'] }
            
        elif type(obj) == RunningJob :
            action = self.engine.daofactory(classname = "RunningJob.Delete")
            
            # verify data is complete
            if not obj.valid(['id']):
                binds = {'job_id' : obj.data['jobId'], 
                         'task_id' : obj.data['taskId'], 
                         'submission' : obj.data['submission'] }
            else :
                binds = {'id' : obj.data['id'] }
            
        else :
            raise NotImplementedError      
        
        action.execute(binds = binds,
                       conn = self.engine.getDBConn(),
                       transaction = self.existingTransaction)  

        return
    
    
    ##########################################################################
    # Methods for DbObjects (advanced)
    ##########################################################################
    
    @dbTransaction
    def objAdvancedLoad(self, obj, binds):
        """
        put your description here
        """
        
        if type(obj) == Task : 
            
            action = self.engine.daofactory(classname = "Task.Load")
            
        elif type(obj) == Job :
            
            action = self.engine.daofactory(classname = "Job.Load")
        
        elif type(obj) == RunningJob :
            
            action = self.engine.daofactory( classname = "RunningJob.Load" )
            
        else :
            raise NotImplementedError 
    
        result = action.execute(binds = binds,
                                conn = self.engine.getDBConn(),
                                transaction = self.existingTransaction)
            
        return result
        
    
    @dbTransaction
    def jobLoadByRunningAttr(self,  binds, limit = None):
        """
        put your description here
        """
        
        action = self.engine.daofactory(classname = "Job.LoadByRunningJobAttr")
        result = action.execute(binds = binds,
                                limit = limit,
                                conn = self.engine.getDBConn(),
                                transaction = self.existingTransaction)
        
        return result
    
    
    ##########################################################################
    # Method for execute raw SQL statements through general-purpose DAO
    ##########################################################################
    
    @dbTransaction
    def executeSQL(self, query):
        """
        Method for execute raw SQL statements through general-purpose DAO
        """
        
        action = self.engine.daofactory(classname = "BLGenericDAO")
        result = action.execute(rawSql = query,
                           conn = self.engine.getDBConn(),
                           transaction = self.existingTransaction) 
        
        return result
