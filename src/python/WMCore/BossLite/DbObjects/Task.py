#!/usr/bin/env python
"""
_Task_

"""

__version__ = "$Id: Task.py,v 1.13 2010/05/03 08:38:06 spigafi Exp $"
__revision__ = "$Revision: 1.13 $"

import os.path
# import threading # seems unused
# import logging

from WMCore.Services.UUID import makeUUID

from WMCore.BossLite.DbObjects.DbObject import DbObject
# from WMCore.BossLite.DbObjects.Job      import Job
from WMCore.BossLite.Common.Exceptions  import TaskError
# from WMCore.BossLite.Common.Exceptions import JobError, DbError # seem unused

class Task(DbObject):
    """
    Task object
    """

    # fields on the object and their names on database
    fields = { 'id' : 'id',
               'name' : 'name',
               'dataset' : 'dataset',
               'startDirectory' : 'start_dir',
               'outputDirectory' : 'output_dir',
               'globalSandbox' : 'global_sandbox',
               'cfgName' : 'cfg_name',
               'serverName' : 'server_name',
               'jobType' : 'job_type',
               'totalEvents' : 'total_events',
               'user_proxy' : 'user_proxy',
               'outfileBasename' : 'outfile_basename',
               'commonRequirements' : 'common_requirements'
             }

    # mapping between field names and database fields
    mapping = fields

    # default values for fields
    defaults = { 'id' : None,
                 'name' : None,
                 'dataset' : None,
                 'startDirectory' : None,
                 'outputDirectory' : None,
                 'globalSandbox' : None,
                 'cfgName' : None,
                 'serverName' : None,
                 'jobType' : None,
                 'totalEvents' : 0,
                 'user_proxy' : None,
                 'outfileBasename' : None,
                 'commonRequirements' : None
              }

    # database properties
    tableName = "bl_task"
    tableIndex = ["id"]

    # exception class
    exception = TaskError

    ##########################################################################

    def __init__(self, parameters = {}):
        """
        initialize a Task instance
        """

        # call super class init method
        DbObject.__init__(self, parameters)

        # Init a bunch of variables to reasonable values
        if not self.data['name']:
            self.data['name'] = makeUUID()
        if not self.data['id']:
            self.data['id'] = -1

        # initialize job set structure
        self.jobs = []
        self.jobLoaded = 0
        self.jobIndex = []
        self.warnings = []
        
        # object not in database
        self.existsInDataBase = False


    ##########################################################################

    def exists(self, db, noDB = False):
        """
        If the task exists, return ID
        
        """
        
        if not noDB:
            
            """
            action = db.daofactory(classname = 'Task.Exists')
            tmpId = action.execute(name = self.data['name'],
                                   conn = db.getDBConn(),
                                   transaction = db.existingTransaction)
            """
            
            tmpId = db.objExists(self)
            
            if tmpId:
                self.data['id'] = tmpId
        else:
            if self.data['id'] < 0:
                return False
            else:
                tmpId = self.data['id']
        
        return tmpId


    ####################################################################

    def save(self, db, deep = True):
        """
        Save the task if there is new information in it.  
        """
        
        status = 0
        
        if self.existsInDataBase : 
            """
            action = self.daofactory(classname = "Task.Save")
            action.execute(binds = self.data,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
            """
            db.objSave(self)
            
        else:
            self.create(db)
        
        if deep :
            for job in self.jobs:
                job['taskId'] = self.data['id']
                job.save()
                job.existsInDataBase = True
                status += 1
        
        return status

    #########################################################################

    def create(self, db):
        """
        Create a new task in the database      
        """
        
        """
        action = engine.daofactory(classname = "Task.New")
        action.execute(binds = self.data,
                       conn = engine.getDBConn(),
                       transaction = engine.existingTransaction)
        """
        db.objCreate(self)
        
        # update ID & check... necessary call!
        if self.exists(db) : 
            self.existsInDataBase = True
        
    ###########################################################################

    def load(self, db, deep = True):
        """
        Load a task     
        """
        
        """
        if self.data['id'] > 0:
            action = self.daofactory(classname = "Task.SelectTask")
            result = action.execute(value = self.data['id'],
                                    column = 'id',
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
        elif self.data['name']:
            action = self.daofactory(classname = "Task.SelectTask")
            result = action.execute(value = self.data['name'],
                                    column = 'name',
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
        else:
            # Then you're screwed
            return
        """
        result = db.objLoad(self)
        
        if result == []:
            raise TaskError("No task instances corresponds to the," + \
                     " template specified: %s" % self)
        
        
        if len(result) > 1 :
            # bad message, I would like to change it...
            raise TaskError("Multiple task instances corresponds to the" + \
                     " template specified: %s" % self)
        
        
        # If we're calling this internally, we only care about the first task
        self.data.update(result[0])
        
        if deep :
            self.loadJobs(db)

        # is this method necessary?
        self.updateInternalData()
        
        self.existsInDataBase = True
        
        return

    ###################################################################

    def loadJobs(self, db):
        """
        Load jobs from the database
        """
        
        if self.data['id'] < 0:
            self.exists(db)
        
        """
        action = self.daofactory(classname = 'Task.GetJobs')
        jobList = action.execute(id = self.data['id'],
                                 conn = self.getDBConn(),
                                 transaction = self.existingTransaction)
        """
        jobList = db.objLoad(self, classname = 'Task.GetJobs') 
        
        
        # update the jobs information
        for job in jobList:
            tmp = Job()
            tmp.data.update(job)
            tmp.getRunningInstance()
            tmp.existsInDataBase = True
            
            self.jobs.append(tmp)
            
            # fill structure to use 'getJob' method
            self.jobIndex.append( job['jobId'] )
            self.jobLoaded += 1
            
        return self.jobLoaded


    ###################################################################

    def update(self, db, deep = True):
        """
        update task object from database (with all jobs)       
        """
        
        # return number of entries updated
        return self.save(deep)

    ##########################################################################

    def remove(self, db):
        """
        remove task object from database (with all jobs)
        """
        
        if not self.exists(db):
            raise TaskError("The following task instance cannot be removed" + \
                      " since it is not in the database: %s" % self)
        
        """
        action = self.daofactory(classname = 'Task.Delete')
        
        # verify data is complete
        if not self.valid(['id']):
            # We can delete by name without an ID
            action.execute(column = 'name',
                           value = self.data['name'],
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
        else:
            action.execute(column = 'id',
                           value = self.data['id'],
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
        """
        db.objRemove(self) 
                
        # update status
        self.existsInDataBase = False

        # return number of entries removed
        return 1

    
    ########################################################################

    def appendJob(self, job):
        """
        append a job into the task
        """

        # assign task id if possible
        if self.data['id'] is None:
            raise TaskError( "Task not loaded %s" %self)

        if self.data['id'] != job['taskId'] :
            raise TaskError(
                "Mismatching taskId: %d for the task, %d for the job" \
                % ( self.data['id'], job['taskId'] )
                )

        # insert job
        pos = len( self.jobIndex )
        while pos :
            pos -= 1
            if self.jobIndex[pos] < job['jobId']:
                pos += 1
                break

        self.jobIndex.insert( pos, job['jobId'] )
        self.jobs.insert( pos, job )

    ##########################################################################

    def appendJobs(self, listOfJobs):
        """
        append jobs into the task
        """

        for job in listOfJobs:
            self.appendJob(job)

    ##########################################################################

    def addJob(self, job):
        """
        insert a job into the task
        """

        # assign id to the job
        self.jobLoaded += 1
        job['jobId'] = self.jobLoaded

        # assign task id if possible
        if self.data['id'] is not None:
            job['taskId'] = self.data['id']

        # insert job
        self.jobIndex.append( job['jobId'] )
        self.jobs.append(job)

    ##########################################################################

    def addJobs(self, listOfJobs):
        """
        insert jobs into the task
        """

        for job in listOfJobs:
            self.addJob(job)

    ##########################################################################

    def getJob(self, jobId):
        """
        return the job with matching jobId
        """

        try :
            return self.jobs[ self.jobIndex.index( long(jobId ) ) ]
        except ValueError:
            return None

    ##########################################################################

    def getJobs(self):
        """
        return the list of jobs in task
        """

        return self.jobs

    ##########################################################################
    
    def updateInternalData(self):
        """
        update private information on it and on its jobs
        """
        
        # update job status and private information
        for job in self.jobs:

            # compute full path for output files
            job['fullPathOutputFiles'] = [
                self.joinPath( self.data['outputDirectory'],  ofile)
                for ofile in job['outputFiles']
                if ofile != '']
            
            # compute full path for output files
            job['fullPathInputFiles'] = [
                self.joinPath( self.data['startDirectory'],  ofile)
                for ifile in job['inputFiles']
                if ifile != '']
        
        
   ##########################################################################

    def joinPath(self, path, name):
        """
        joining files with base directory
        """
        if path is None or path == '' :
            return name

        if name.find( 'file:/' ) == 0:
            return name

        return os.path.join(path, name)
