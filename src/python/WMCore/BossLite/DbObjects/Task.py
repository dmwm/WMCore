#!/usr/bin/env python
"""
_Task_

"""

__version__ = "$Id: Task.py,v 1.8 2010/04/21 10:55:10 spigafi Exp $"
__revision__ = "$Revision: 1.8 $"

import os.path
import threading
import logging

from WMCore.Services.UUID import makeUUID

from WMCore.BossLite.DbObjects.DbObject import DbObject, dbTransaction
from WMCore.BossLite.DbObjects.Job      import Job
from WMCore.BossLite.Common.Exceptions  import TaskError, JobError, DbError

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


    ##########################################################################

    @dbTransaction
    def exists(self, noDB = False):
        """
        If the task exists, return ID
        
        """
        
        if not noDB:
            action = self.daofactory(classname = 'Task.Exists')
            id = action.execute(name = self.data['name'],
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
            if id:
                self.data['id'] = id
        else:
            if self.data['id'] < 0:
                return False
            else:
                id = self.data['id']
        
        return id


    ####################################################################

    @dbTransaction
    def save(self, deep = True):
        """
        Save the task if there is new information in it.    
        """
        
        status = 0

        if self.exists():
            action = self.daofactory(classname = "Task.Save")
            action.execute(binds = self.data,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
        else:
            self.create()
        
        if deep : 
            for job in self.jobs:
                    job.save()
                    status += 1
                    
        return status

    #########################################################################

    @dbTransaction
    def create(self):
        """
        Create a new task in the database      
        """
        
        action = self.daofactory(classname = "Task.New")
        action.execute(binds = self.data,
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction)

        return self.exists()

    ###########################################################################

    @dbTransaction
    def load(self, deep = True):
        """
        Load a task     
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
                
        if result == []:
            # Then we have nothing
            logging.error('Attempted to load non-existant task with parameters:\n %s' %(self.data))
            return
        
        # If we're calling this internally, we only care about the first task
        self.data.update(result[0])
        
        if deep :
            self.loadJobs()
            
            # to check
            for job in self.jobs:
                job.getRunningInstance()
            
        return

    ###################################################################

    @dbTransaction
    def loadJobs(self):
        """
        Load jobs from the database
        """
        
        if self.data['id'] < 0:
            self.exists()
        
        action = self.daofactory(classname = 'Task.GetJobs')
        jobList = action.execute(id = self.data['id'],
                                 conn = self.getDBConn(),
                                 transaction = self.existingTransaction)


        # update the jobs information, no runninJob associated (?)...
        for job in jobList:
            tmp = Job()
            tmp.data.update(job)
            self.jobs.append(tmp)

        return


    ###################################################################

    def update(self, deep = True):
        """
        update task object from database (with all jobs)       
        """
        
        status = self.save(deep)
        
        # return number of entries updated
        return status

    ##########################################################################

    @dbTransaction
    def remove(self):
        """
        remove task object from database (with all jobs)
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

#    def save(self, db):
#        """
#        save complete task object in database
#        """
#
#        # verify if the task is new 
#        if self.valid(['id']):
#            raise TaskError("The following task instance cannot be saved, " + \
#                      "since it specifies an ID. Consider using " + \
#                      "update instead of save: %s" % self)
#
#        # verify that the task has a name
#        if not self.valid(['name']):
#            raise TaskError("The following task instance cannot be saved, " + \
#                      "since it is not completely specified: %s" % \
#                      self)
#
#        # insert complete task
#        try:
#
#            # insert task
#            status = db.insert(self)
#            if status != 1:
#                raise TaskError("Cannot insert task %s" % self)
#
#            # get task id
#            listOfTasks = db.select(self)
#            if len(listOfTasks) != 1:
#                raise TaskError("Cannot insert task %s" % self)
#
#            # store task id in task and in all jobs
#            self.data['id'] = listOfTasks[0]['id']
#            for job in self.jobs:
#                job['taskId'] = self.data['id']
#
#            # store all jobs
#            for job in self.jobs:
#                status += job.save(db)
#
#        # database error
#        except DbError, msg:
#            raise TaskError(str(msg))
#
#        # job error
#        except JobError, msg:
#            raise TaskError(str(msg))
#
#        # update status
#        self.existsInDataBase = True
#
#        return status
#

    ##########################################################################

    #def remove(self, db):
    #    """
    #    remove task object from database (with all jobs)
    #    """
    #
    #    # verify data is complete
    #    if not self.valid(['id']):
    #        raise TaskError("The following task instance cannot be removed" + \
    #                  " since it is not completely specified: %s" % self)
    #
    #    # remove from database
    #    try:
    #        status = db.delete(self)
    #        if status < 1:
    #            raise TaskError("Cannot remove task %s" % str(self))
    #
    #    # database error
    #    except DbError, msg:
    #        raise TaskError(str(msg))
    #
    #    # update status
    #    self.existsInDataBase = False
    #
    #    # return number of entries removed
    #    return status

   ##########################################################################

#    def update(self, db, deep = True):
#        """
#        update task object from database (with all jobs)
#        """
#
#        # verify if the object exists in database
#        if not self.existsInDataBase:
#
#            # no, use save instead of update
#            return self.save(db)
#
#        # verify data is complete
#        if not self.valid(['id']):
#            raise TaskError("The following task instance cannot be updated," + \
#                     " since it is not completely specified: %s" % self)
#
#        # update task object in database
#        try:
#            status = db.update(self)
#
#            # update all jobs
#            if deep:
#                for job in self.jobs:
#                    status += job.update(db, deep)
#
#        # database error
#        except DbError, msg:
#            raise TaskError(str(msg))
#
#        # job error
#        except JobError, msg:
#            raise TaskError(str(msg))
#
#        # return number of entries updated
#        return status

   ##########################################################################

#    def load(self, db, deep = True):
#        """
#        load information from database
#        """
#
#        # verify data is complete
#        if not self.valid(['id']) and not self.valid(['name']):
#            raise TaskError("The following task instance cannot be loaded" + \
#                     " since it is not completely specified: %s" % self)
#
#        # get information from database based on template object
#        try:
#            objects = db.select(self)
#
#        # database error
#        except DbError, msg:
#            raise TaskError(str(msg))
#
#        # since required data is a key, it should be a single object list
#        if len(objects) == 0:
#            raise TaskError("No task instances corresponds to the," + \
#                     " template specified: %s" % self)
#
#        if len(objects) > 1:
#            raise TaskError("Multiple task instances corresponds to the" + \
#                     " template specified: %s" % self)
#
#        # copy fields
#        for key in self.fields:
#            self.data[key] = objects[0][key]
#
#        # get job objects if requested
#        if deep:
#
#            # create basic template if not provided
#            if self.jobs == []:
#                template = Job({'taskId' : self.data['id']})
#
#            # select single job as template checking proper specification
#            else:
#                if len(self.jobs) != 1:
#                    raise TaskError("Multiple job instances specified as" + \
#                     " templates in load task operation for task: %s" % self)
#
#                # update template for matching
#                template = self.jobs[0]
#                template['taskId'] = self['id']
#                template['jobId'] = None
#
#            # get jobs
#            self.jobs = db.select(template)
#
#            # get associated running jobs (if any)
#            for job in self.jobs:
#                job.getRunningInstance(db)
#
#        # update status
#        self.existsInDataBase = True
#
#        # update job status
#        for job in self.jobs:
#            job.existsInDataBase = True
#
#        # update private data
#        self.updateInternalData()

   ##########################################################################

    def updateInternalData(self):
        """
        update private information on it and on its jobs
        """

        # update job status and private information
        for job in self.jobs:

            # comput full path for output files
            job['fullPathOutputFiles'] = [
                self.joinPath( self.data['outputDirectory'],  ofile)
                for ofile in job['outputFiles']
                if ofile != '']

        # get input directory
        if self.data['globalSandbox'] is not None:
            inputDirectory = self.data['globalSandbox']
        else:
            inputDirectory = ""

        # update job status and private information
        for job in self.jobs:

            # comput full path for output files
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

        return os.path.join(dir, name)
