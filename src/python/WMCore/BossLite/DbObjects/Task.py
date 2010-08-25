#!/usr/bin/env python
"""
_Task_

"""

__version__ = "$Id: Task.py,v 1.17 2010/05/10 12:45:06 spigafi Exp $"
__revision__ = "$Revision: 1.17 $"

import os.path

from WMCore.Services.UUID import makeUUID

from WMCore.BossLite.DbObjects.DbObject import DbObject, DbObjectDBFormatter
from WMCore.BossLite.DbObjects.Job      import Job

from WMCore.BossLite.Common.Exceptions  import TaskError

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
            
            db.objSave(self)
            
        else:
            self.create(db)
        
        if deep :
            for job in self.jobs:
                job['taskId'] = self.data['id']
                job.save(db)
                job.existsInDataBase = True
                status += 1
        
        return status

    #########################################################################

    def create(self, db):
        """
        Create a new task in the database      
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
        
        # this check could be improved...
        if self.data['id'] < 0:
            self.exists(db)
        
        jobList = db.objLoad(self, classname = 'Task.GetJobs') 
           
        # update the jobs information
        for job in jobList:
            tmp = Job()
            tmp.data.update(job)
            tmp.getRunningInstance(db)
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
        return self.save(db, deep)

    ##########################################################################

    def remove(self, db):
        """
        remove task object from database (with all jobs)
        """
        
        if not self.existsInDataBase :
            # could this message be changed?
            raise TaskError("The following task instance cannot be removed" + \
                      " since it is not in the database: %s" % self)
        
        
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
                self.joinPath( self.data['startDirectory'],  ifile)
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


class TaskDBFormatter(DbObjectDBFormatter):
    """
    TaskDBFormatter 
    """
    
    def preFormat(self, res):
        """
        It maps database fields with object dictionary and it translate python 
        List and timestamps in well formatted string. This is useful for any 
        kind of database engine!
        """
        
        result = {}
        
        # result['id']               = entry['id']
        result['startDirectory']     = res['startDirectory']
        result['outputDirectory']    = res['outputDirectory']
        result['globalSandbox']      = res['globalSandbox']
        result['cfgName']            = res['cfgName']
        result['serverName']         = res['serverName']
        result['jobType']            = res['jobType']
        result['outfileBasename']    = res['outfileBasename']
        result['commonRequirements'] = res['commonRequirements']
        result['name']               = res['name']
        result['dataset']            = res['dataset']
        result['user_proxy']         = res['user_proxy']
            
        return result
    
    def postFormat(self, res):
        """
        Format the results into the right output. This is useful for any 
        kind of database engine!
        """
        
        final = []
        for entry in res:
            result = {}
            result['id']                 = entry['id']
            result['startDirectory']     = entry['startdirectory']
            result['outputDirectory']    = entry['outputdirectory']
            result['globalSandbox']      = entry['globalsandbox']
            result['cfgName']            = entry['cfgname']
            result['serverName']         = entry['servername']
            result['jobType']            = entry['jobtype']
            result['outfileBasename']    = entry['outfilebasename']
            result['commonRequirements'] = entry['commonrequirements']
            result['name']               = entry['name']
            result['dataset']            = entry['dataset']
            result['user_proxy']         = entry['user_proxy']
            final.append(result)

        return final