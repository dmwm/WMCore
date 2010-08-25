#!/usr/bin/env python
"""
_BossLiteAPI_

"""

__version__ = "$Id: BossLiteAPI.py,v 1.3 2010/04/19 20:44:41 mnorman Exp $"
__revision__ = "$Revision: 1.3 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import logging
import copy

# Task and job objects
from WMCore.BossLite.DbObjects.Job        import Job
from WMCore.BossLite.DbObjects.Task       import Task
from WMCore.BossLite.DbObjects.RunningJob import RunningJob
from WMCore.BossLite.Common.Exceptions    import TaskError, JobError, DbError
from WMCore.BossLite.DbObjects.DbObject   import dbTransaction

# WMCore framework
from WMCore.WMConnectionBase              import WMConnectionBase


##########################################################################
def parseRange(  nRange, rangeSep = ':', listSep = ',' ) :
    """
    Utility for parsing ranges and/or lists of tasks/jobs
    """

    if type( nRange ) == int or type( nRange ) == long:
        return [ str(nRange) ]

    nList = []

    for subRange in nRange.split( listSep ) :
        if subRange.find( rangeSep ) == -1 :
            start = int( subRange )
            end = int( subRange )
        else :
            s, e = subRange.split( rangeSep )
            start = int( s )
            end = int( e )
        nList.extend( range( start, end+1 ) )

    nList.sort()
    return nList


##########################################################################

class BossLiteAPI(WMConnectionBase):
    """
    High level API class for DBObjets.
    It allows load/operate/update jobs and taks using just id ranges
    """

    def __init__(self, database = None, dbConfig=None, pool=None, makePool=False):
        """
        initialize the API instance
        - database can be both MySQl or SQLite

        - dbConfig can be a dictionary with the format
           {'dbName':'BossLiteDB',
               'host':'localhost',
               'user':'BossLiteUser',
               'passwd':'BossLitePass',
               'socketFileLocation':'/var/run/mysql/mysql.sock',
               'portNr':'',
               'refreshPeriod' : 4*3600 ,
               'maxConnectionAttempts' : 5,
               'dbWaitingTime' : 10
              }

        Passing only dbConfig, a SafeSession will be used for db connection.
        Passing a pool or setting makePool, a pool of SafeSession (SafePool)
        will be used, enabling a better multithread usage
        """

        #if database == "WMCore":
        #    from ProdCommon.BossLite.API.BossLiteDBWMCore import BossLiteDBWMCore
        #    self.bossLiteDB = BossLiteDBWMCore( database, dbConfig=dbConfig )
        #elif pool is None and makePool == False:
        #    self.bossLiteDB = BossLiteDB( database, dbConfig=dbConfig )
        #else :
        #    from ProdCommon.BossLite.API.BossLitePoolDB import BossLitePoolDB
        #    self.bossLiteDB = BossLitePoolDB( database, dbConfig=dbConfig, \
        #                                      pool=pool )

        WMConnectionBase.__init__(self, daoPackage = "WMCore.BossLite")
        
        self.db = None


        return


    ######################################################################
    # Okay, this is where I start trying to redesign the API functions   #
    # I'm going to rely on the new DBObjects
    # And also not on any sort of database carrying
    #     -mnorman
    ######################################################################



    # Start with task functions
    
    ##########################################################################


    def saveTask( self, task ):
        """
        register task related informations in the db
        """

        # save task
        try :
            task.save()
        except TaskError, err:
            if str(err).find( 'column name is not unique') == -1 and \
                   str(err).find( 'Duplicate entry') == -1 and \
                   task['id'] is not None :
                self.removeTask( task )
                task = None
            raise

        return task

    ##########################################################################


    def loadTask( self, taskId, jobRange='all', deep=True ) :
        """
        retrieve task information from db using task id

        the jobs loading can be tuned using jobRange:
        - None       : no jobs are loaded
        - 'all'      : all jobs are loaded
        - list/range : only selected jobs are loaded - DISABLED
        """

        # db connect
        if self.db is None :
            self.connect()

        # create template for task
        task = Task(parameters = {'id': int(taskId)})
        task.load()

        if jobRange == 'all':
            # Then load all the jobs
            task.loadJobs()

        return task

    ##########################################################################


    def loadTaskByName( self, name, jobRange='all', deep=True ) :
        """
        retrieve task information from db for task 'name'
        """

        # create template for task and load
        task = Task(parameters = {'name': name})
        task.load()

        if jobRange == 'all':
            task.loadJobs()

        return task


    ##########################################################################

    @dbTransaction
    def loadTasksByProxy( self, name, deep=True ) :
        """
        retrieve task information from db for all tasks
        with user proxy set to name
        """

        taskList = []

        action = self.daofactory(classname = "Task.SelectTask")
        result = action.execute(column = 'user_proxy',
                                value = name,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction)
        if type(result) == dict:
            result = [result]  # It has to be a list

        for entry in result:
            # Each entry should be a task dictionary
            task = Task()
            task.data.update(entry)
            taskList.append(task)

        return taskList



    ##########################################################################


    def loadJob( self, taskId, jobId ) :
        """
        retrieve job information from db using task and job id
        """

        # creating job
        jobAttributes = { 'taskId' : taskId, "jobId" : jobId}
        job = Job(parameters = jobAttributes)

        # load job from db
        job.load()

        return job

    ##########################################################################

    @dbTransaction
    def loadJobsByAttr( self, jobAttribute, value) :
        """
        retrieve job information from db for job matching attributes
        """
        jobList = []
        binds = []
        if type(value) == list:
            for entry in value:
                binds.append({'value': entry})
        else:
            binds = value

        action = self.daofactory(classname = "Job.SelectJob")
        result = action.execute(column = jobAttribute,
                                value = binds,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction)

        for entry in result:
            job = Job()
            job.data.update(entry)
            jobList.append(job)
        

        return jobList

    ###########################################################################



    def loadJobByName( self, jobName ) :
        """
        retrieve job information from db for jobs with name 'name'
        """

        params = {'name': jobName}
        job = Job(parameters = params)
        job.load()

        return job

    ############################################################################



    def getRunningInstance( self, job, runningAttrs = None ) :
        """
        retrieve RunningInstance where existing or create it
        """


        # check whether the running instance is still loaded
        if job.runningJob is not None :
            return

        # load if exixts
        job.getRunningInstance()

        # create it otherwise
        if job.runningJob is None :
            job.newRunningInstance()
            if type(runningAttrs) == dict:
                job.runningJob.data.update(runningAttrs)
            
        return

    ##########################################################################


    def getNewRunningInstance( self, job, runningAttrs = None ) :
        """
        create a new RunningInstance
        """

        job.newRunningInstance()
        if type(runningAttrs) == dict:
            job.runningJob.data.update(runningAttrs)

        return

    ############################################################################


    @dbTransaction
    def loadJobsByRunningAttr( self, attribute, value) :
        """
        retrieve job information from db for job
        whose running instance match attributes
        """

        jobList = []
        binds   = []

        if type(value) == list:
            for entry in value:
                binds.append({'value': entry})
        else:
            binds = value

        action = self.daofactory(classname = "Job.LoadByRunningJobAttr")
        result = action.execute(column = attribute,
                                value = binds,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction)

        for entry in result:
            job = Job()
            job.data.update(entry)
            jobList.append(job)

        return jobList


    
    ############################################################################


    def loadCreated( self, attributes = None, limit=None, offset=None  ) :
        """
        retrieve information from db for jobs created but not submitted using:
    
        Takes the highest submission number for each job
        """

        return self.loadJobsByRunningAttr(attribute = 'status', value = 'W')



    ##########################################################################
    
    def loadSubmitted( self, attributes = None, limit=None, offset=None  ) :
        """
        retrieve information from db for jobs submitted using:

        Takes the highest submission number for each job
        """

        return self.loadJobsByRunningAttr(attribute = 'closed', value = 'N')



    ##########################################################################
    
    def loadEnded( self, attributes = None, limit=None, offset=None ) :
        """
        retrieve information from db for jobs successfully using:

        Takes the highest submission number for each job
        """

        return self.loadJobsByRunningAttr(attribute = 'status', value = 'SD')


    ##########################################################################


    def loadFailed( self, attributes = None, limit=None, offset=None ) :
        """
        retrieve information from db for jobs aborted/killed using:
        - range of tasks
        - range of jobs inside a task

        Takes the highest submission number for each job
        """

        jobList = []
        jobList.extend(self.loadJobsByRunningAttr(attribute = 'status',
                                                  value = 'A'))
        jobList.extend(self.loadJobsByRunningAttr(attribute = 'status',
                                                  value = 'K'))


        return jobList


    ##########################################################################


    def updateRunningInstances( self, task, notSkipClosed=True ) :
        """
        update runningInstances of a task in the DB
        """

        # update
        for job in task.jobs:

            # update only loaded instances!
            if job.runningJob is not None:
                job.updateRunningInstance()

        return

    ############################################################################



    def archive(self, obj ):
        """
        Close running jobs?  Works for either task or job
        """

        # the object passed is a Job
        if type(obj) == Job :
            obj.runningJob['closed'] = 'Y'

        # the object passed is a Task
        elif type(obj) == Task :
            for job in obj.jobs:
                job.runningJob['closed'] = 'Y'

        # update object
        obj.update()



    ############################################################################
        
    def getTaskFromJob( self, job):
        """
        retrieve Task object from Job object and perform association
        """

        # creating task
        task = self.loadTask(taskId = job['taskId'], jobRange = None)

        # perform association
        task.appendJob(job)

        # operation validity checks
        if len( task.jobs ) != 1 :
            raise DbError( "ERROR: too many jobs loaded %s" % \
                                 len( task.jobs ))
        if id( task.jobs[0] ) != id( job ) :
            raise DbError( "Fatal ERROR: mismatching job" )

        # return task
        return task



    ############################################################################

    def load( self, task, jobRange="all"):
        """
        This is a crippled version of the original load function

        """


        task.load()

        if jobRange == None:
            return task
        else:
            task.loadJobs()
            for job in task.jobs:
                job.getRunningInstance()


        return task


    ##########################################################################

    def removeTask(self, task):
        """
        remove task, jobs and their running instances from db
        NOT SQLite safe
        """

        # remove task
        task.remove()

        task = None

        return task


    ##########################################################################
    
    def removeJob( self, job ):
        """
        remove job and its running instances from db
        NOT SQLite safe
        """

        # remove job
        job.remove()

        job = None

        return job


    #def loadLastJobByName( self, jobName ) :
    #    """
    #    retrieve job information from db for jobs with name 'name'
    #
    #    Really?  Name is unique, so all you have to do with a given name
    #    is just retrieve the job.
    #
    #    Thus I'm not implementing it at this time.
    #    """
    #
    #    self.loadJobByName(jobName = jobName)

        


    ######################################################################
    # And this is where I stopped working
    #   -mnorman
    ######################################################################




























        


    ##########################################################################
    def connect ( self ) :
        """
        recreate a session and db access
        """

        # open Db and create a db access
        #self.bossLiteDB.connect()
        #self.db = TrackingDB(self.bossLiteDB)


    ##########################################################################
    def updateDB( self, obj ) :
        """
        update any object table in the DB
        works for tasks, jobs, runningJobs
        """

        # db connect
        if self.db is None :
            self.connect()

        # update
        obj.update(self.db)
        self.bossLiteDB.commit()


    ##########################################################################
    #def updateRunningInstances( self, task, notSkipClosed=True ) :
    #    """
    #    update runningInstances of a task in the DB
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # update
    #    for job in task.jobs:
    #
    #        # update only loaded instances!
    #        if job.runningJob is not None:
    #            job.updateRunningInstance(self.db, notSkipClosed)
    #
    #    self.bossLiteDB.commit()


    ##########################################################################
    def declare( self, xml, proxyFile=None ) :
        """
        register job related informations in the db
        """

        taskInfo, jobInfos, rjAttrs = self.deserialize( xml )

        # reconstruct task
        task = Task( taskInfo )
        task['user_proxy'] = proxyFile
        self.saveTask( task )

        # reconstruct jobs and fill the data
        jobs = []
        for jI in jobInfos:
            job = Job( jI )
            subn = int( job['submissionNumber'] )
            if subn > 0 :
                job['submissionNumber'] = subn - 1
            else :
                job['submissionNumber'] = subn
            jobs.append(job)

        task.addJobs(jobs)

        for job in task.jobs:
            attrs = rjAttrs[ str(job['name']) ]
            self.getRunningInstance( job, attrs )
            self.updateDB( job )

        # self.updateDB( task )

        # all done
        return task


    ##########################################################################
    #ef saveTask( self, task ):
    #   """
    #   register task related informations in the db
    #   """
    #
    #   # db connect
    #   if self.db is None :
    #       self.connect()
    #
    #   # save task
    #   try :
    #       task.updateInternalData()
    #       task.save(self.db)
    #       self.bossLiteDB.commit()
    #   except TaskError, err:
    #       if str(err).find( 'column name is not unique') == -1 and \
    #              str(err).find( 'Duplicate entry') == -1 and \
    #              task['id'] is not None :
    #           self.removeTask( task )
    #           task = None
    #       raise
    #
    #   return task


    ##########################################################################
    #def removeJob( self, job ):
    #    """
    #    remove job and its running instances from db
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # remove runnningjobs in db with non relational checks
    #    if self.bossLiteDB.database == "SQLite":
    #
    #        # load running jobs
    #        rjob = RunningJob( { 'jobId' : job['jobId'],
    #                             'taskId' : job['taskId'] } )
    #        rjobList = self.db.select( rjob)
    #        for rjob in rjobList :
    #            rjob.remove( self.db )
    #
    #    # remove job
    #    job.remove( self.db )
    #    self.bossLiteDB.commit()
    #
    #    job = None
    #
    #    return job

    ##########################################################################
    #def removeTask( self, task ):
    #    """
    #    remove task, jobs and their running instances from db
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # remove jobs in db with non relational checks
    #    if self.bossLiteDB.database == "SQLite":
    #
    #        # load running jobs
    #        rjob = RunningJob( { 'taskId' : task['id'] } )
    #        rjobList = self.db.select( rjob)
    #        for rjob in rjobList :
    #            rjob.remove( self.db )
    #
    #        # load jobs
    #        job = Job( { 'taskId' : task['id'] } )
    #        jobList = self.db.select( job)
    #        for job in jobList :
    #            job.remove( self.db )
    #
    #    # remove task
    #    task.remove( self.db )
    #    self.bossLiteDB.commit()
    #
    #    task = None
    #
    #    return task

    ##########################################################################
    #def loadTask( self, taskId, jobRange='all', deep=True ) :
    #    """
    #    retrieve task information from db using task id
    #
    #    the jobs loading can be tuned using jobRange:
    #    - None       : no jobs are loaded
    #    - 'all'      : all jobs are loaded
    #    - list/range : only selected jobs are loaded
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # create template for task
    #    task = Task()
    #    task['id'] = int(taskId)
    #    task.load(self.db, False)
    #
    #    # load jobs
    #    # # backward compatible 'deep' parameter handling
    #    if jobRange is not None and deep != False :
    #        self.load( task, jobRange )
    #
    #    return task


    ##########################################################################
    #def loadTaskByName( self, name, jobRange='all', deep=True ) :
    #    """
    #    retrieve task information from db for task 'name'
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # create template for task and load
    #    task = Task()
    #    task['name'] = name
    #    task.load(self.db, False)
    #
    #    # load jobs
    #    # # backward compatible 'deep' parameter handling
    #    if jobRange is not None and deep != False :
    #        self.load( task, jobRange )
    #
    #    return task


    ##########################################################################
    #ef loadTasksByUser( self, user, deep=True ) :
    #   """
    #   retrieve task information from db for task owned by user
    #   """
    #
    #   # db connect
    #   if self.db is None :
    #       self.connect()
    #
    #   # create template for task
    #   task = Task()
    #   task['user'] = user
    #
    #   # load task
    #   taskList = self.db.select(task, deep)
    #
    #   return taskList


    ##########################################################################
    #def loadTasksByProxy( self, name, deep=True ) :
    #    """
    #    retrieve task information from db for all tasks
    #    with user proxy set to name
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # create template for task
    #    task = Task()
    #    task['user_proxy'] = name
    #
    #    # load task
    #    taskList = self.db.select(task, deep)
    #
    #    return taskList


###     ##########################################################################
###     def load( self, taskRange, jobRange="all", jobAttributes=None, runningAttrs=None, strict=True, limit=None, offset=None ) :
###         """
###         retrieve information from db for:
###         - range of tasks or even a task object
###         - range of jobs inside a task or a python list of id's
###         - various job attributes (logic and)
###
###         In some way these should be the option to build the query.
###         Maybe, same options should be used also in
###         loadSubmitted, loadCreated, loadEnded, loadFailed
###
###         Takes the highest submission number for each job
###         """
###
###         # db connect
###         if self.db is None :
###             self.connect()
###
###         # defining default
###         taskList = []
###         if jobAttributes is None :
###             jobAttributes = {}
###
###         # identify jobRange
###         if type( jobRange ) == list :
###             jobList = jobRange
###         elif jobRange is None :
###             jobList = []
###         elif jobRange == 'all' :
###             jobList = None
###         else:
###             jobList = parseRange( jobRange )
###
###         # already loaded task?
###         if type( taskRange ) == Task :
###
###             # provided a job list: load just missings
###             if jobList is not None:
###                 s = [ str(job['jobId']) for job in taskRange.jobs ]
###                 jobList = [str(x) for x in jobList if str(x) not in s]
###                 jobList.sort()
###             if jobList == [] :
###                 jobList = None
###
###             # no need to load if the task already has jobs
###             #    and no other jobs are requested
###             if taskRange.jobs == [] or jobList is not None :
###                 # new
###                 jobAttributes['taskId'] = int( taskRange['id'] )
###                 jobs = self.loadJobsByRunningAttr( runningAttrs, \
###                                                    jobAttributes, \
###                                                    strict=strict, \
###                                                    limit=limit, offset=offset,\
###                                                    jobList=jobList )
###                 taskRange.appendJobs( jobs )
###             taskList.append( taskRange )
###             return taskList
###
###         # loop over tasks
###         for taskId in parseRange( taskRange ) :
###
###             # create template and load
###             task = Task()
###             task['id'] = int( taskId )
###             task.load( self.db, deep = False )
###             # new
###             jobAttributes['taskId'] = int( task['id'] )
###             jobs = self.loadJobsByRunningAttr( runningAttrs, \
###                                                jobAttributes, \
###                                                strict=strict, \
###                                                limit=limit, offset=offset, \
###                                                jobList=jobList )
###             task.appendJobs( jobs )
###
###             # update task list
###             task.updateInternalData()
###             taskList.append( task )
###
###         return taskList


    ##########################################################################
    #def load( self, task, jobRange="all", jobAttributes=None, runningAttrs=None, strict=True, limit=None, offset=None ) :
    #    """
    #    retrieve information from db for:
    #    - jobRange can be of the form:
    #         'a,b:c,d,e'
    #         ['a',b','c']
    #         'all'
    #         None (no jobs to be loaded
    #
    #    In some way these should be the option to build the query.
    #    Maybe, same options should be used also in
    #    loadSubmitted, loadCreated, loadEnded, loadFailed
    #
    #    Takes the highest submission number for each job
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # already loaded task?
    #    if not isinstance( task, Task ) :
    #        task = Task({'id' : task})
    #        task.load(self.db, False)
    #    elif jobRange == 'all' and task.jobs != []:
    #        jobRange = None
    #
    #    # simple case: no jobs loading request
    #    if jobRange is None:
    #        return task
    #
    #    # defining default
    #    if jobAttributes is None :
    #        jobAttributes = {}
    #
    #    # evaluate job list
    #    jobList = None
    #    if jobRange is not None and jobRange != 'all':
    #
    #        # identify jobRange
    #        if type( jobRange ) == list :
    #            jobList = jobRange
    #        else :
    #            jobList = parseRange( jobRange )
    #
    #        # if there are loaded jobs, load just missing
    #        if task.jobs != []:
    #            s = [ str(job['jobId']) for job in task.jobs ]
    #            jobList = [str(x) for x in jobList if str(x) not in s]
    #
    #        # no jobs to be loaded?
    #        if jobList == [] :
    #            return task
    #        elif jobList is not None:
    #            jobList.sort()
    #
    #    # load
    #    jobAttributes['taskId'] = int( task['id'] )
    #    jobs = self.loadJobsByRunningAttr( runningAttrs, \
    #                                       jobAttributes, \
    #                                       strict=strict, \
    #                                       limit=limit, offset=offset,\
    #                                       jobList=jobList )
    #    task.appendJobs( jobs )
    #
    #    return task


    ##########################################################################
    #def loadJob( self, taskId, jobId ) :
    #    """
    #    retrieve job information from db using task and job id
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # creating job
    #    jobAttributes = { 'taskId' : taskId, "jobId" : jobId}
    #    job = Job( jobAttributes )
    #
    #    # load job from db
    #    job.load(self.db)
    #
    #    return job


    ##########################################################################
    #def loadJobsByAttr( self, jobAttributes ) :
    #    """
    #    retrieve job information from db for job matching attributes
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # creating jobs
    #    job = Job( jobAttributes )
    #
    #    # load job from db
    #    jobList = self.db.select(job)
    #
    #    return jobList


    ##########################################################################
    #def getNewRunningInstance( self, job, runningAttrs = None ) :
    #    """
    #    create a new RunningInstance
    #    """
    #
    #    if runningAttrs is None :
    #        run = RunningJob()
    #    else :
    #        run = RunningJob(runningAttrs)
    #
    #    job.newRunningInstance( run, self.db )


    ##########################################################################
    #def getRunningInstance( self, job, runningAttrs = None ) :
    #    """
    #    retrieve RunningInstance where existing or create it
    #    """
    #
    #
    #    # check whether the running instance is still loaded
    #    if job.runningJob is not None :
    #        return
    #
    #    # load if exixts
    #    job.getRunningInstance(self.db)
    #
    #    # create it otherwise
    #    if job.runningJob is None :
    #
    #        if runningAttrs is None :
    #            run = RunningJob()
    #        else :
    #            run = RunningJob(runningAttrs)
    #
    #        job.newRunningInstance( run, self.db )


    ##########################################################################
    def loadJobsByTimestamp( self, more, less, runningAttrs=None, jobAttributes=None) :
        """
        retrieve job information from db for job
        whose running instance match attributes
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating running jobs
        if runningAttrs is None :
            run = RunningJob()
        else:
            run = RunningJob( runningAttrs )

        # creating jobs
        if jobAttributes is None :
            job = Job()
        else :
            job = Job( jobAttributes )

        # load job from db
        jMap = { 'jobId' : 'jobId',
                 'taskId' : 'taskId',
                 'submissionNumber' : 'submission' }

        runJobList = self.db.selectJoin( job, run, \
                                         jMap=jMap, \
                                         less=less, \
                                         more=more )

        # recall jobs
        for job, runningJob in runJobList :
            job.setRunningInstance( runningJob )

        # return
        return [key[0] for key in runJobList]


    ##########################################################################
    #def loadJobsByRunningAttr( self, runningAttrs=None, jobAttributes=None, all=False, strict=True, limit=None, offset=None, jobList=None ) :
    #    """
    #    retrieve job information from db for job
    #    whose running instance match attributes
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # creating running jobs
    #    if runningAttrs is None :
    #        run = RunningJob()
    #    else:
    #        run = RunningJob( runningAttrs )
    #
    #    # creating jobs
    #    if jobAttributes is None :
    #        job = Job()
    #    else :
    #        job = Job( jobAttributes )
    #
    #    # if requested, with a right join is possible to fill a running
    #    # instance where missing
    #    if all :
    #        jType = 'right'
    #    else :
    #        jType = ''
    #
    #    # load bunch of jobs?
    #    if jobList is None or jobList == [] :
    #        inList = None
    #    else :
    #        inList = {'jobId' : jobList}
    #
    #    # load job from db
    #    jMap = { 'jobId' : 'jobId',
    #             'taskId' : 'taskId',
    #             'submissionNumber' : 'submission' }
    #    options = { 'strict' : strict,
    #                'jType'  : jType,
    #                'limit'  : limit,
    #                'offset' : offset,
    #                'inList' : inList }
    #
    #    runJobList = self.db.selectJoin( job, run, \
    #                                     jMap=jMap , \
    #                                     options=options )
    #
    #    # recall jobs
    #    for job, runningJob in runJobList :
    #        job.setRunningInstance( runningJob )
    #
    #    # return
    #    return [key[0] for key in runJobList]


    ##########################################################################
    #def loadJobByName( self, jobName ) :
    #    """
    #    retrieve job information from db for jobs with name 'name'
    #    """
    #
    #    jobList = self.loadJobsByAttr( { 'name' : jobName } )
    #
    #    if jobList is None or jobList == [] :
    #        return None
    #
    #    if len (jobList) > 1 :
    #        raise JobError("Multiple job instances corresponds to the" + \
    #                 " name specified: %s" % jobName)
    #
    #    return jobList[0]


    ##########################################################################
    #  What is this supposed to do?
    #  name is a unique variable in the job table.  You can't select by jid given a jobName!
    #    -mnorman
    ##########################################################################

    
    #def loadLastJobByName( self, jobName ) :
    #    """
    #    retrieve job information from db for jobs with name 'name'
    #    """
    #
    #    jobList = self.loadJobsByRunningAttr(
    #        jobAttributes={ 'name' : jobName } )
    #
    #    if jobList is None or jobList == [] :
    #        return None
    #
    #    jid = 0
    #    retJob = None
    #    for job in jobList :
    #        if job['id'] > jid :
    #            retJob = job
    #        
    #    for job in jobList :
    #        if job['id'] != retJob['id'] and job.runningJob['closed'] == 'N' \
    #               and job.runningJob['processStatus'] == 'processed' :
    #            logging.warning(
    #                "WARNING: previous job %s.%s.%s not closed. Forcing closed='Y'" \
    #                % (job['taskId'], job['jobId'], job['submissionNumber'])
    #                )
    #            job.runningJob['closed'] = 'Y'
    #            self.updateDB(job.runningJob)
    #
    #    return retJob

    ##########################################################################
    #ef loadCreated( self, attributes = None, limit=None, offset=None ) :
    #   """
    #   retrieve information from db for jobs created but not submitted using:
    #   - range of tasks
    #   - range of jobs inside a task
    #
    #   Takes the highest submission number for each job
    #   """
    #
    #   jobList = []
    #   if attributes is None :
    #       attributes = { 'status' : 'W' }
    #   else :
    #       attributes['status'] = 'W'
    #
    #   # load W
    #   jobList = self.loadJobsByRunningAttr(
    #       attributes, limit=limit, offset=offset )
    #
    #   # load C
    #   attributes['status'] = 'C'
    #   jobList.extend( self.loadJobsByRunningAttr(
    #       attributes, limit=limit, offset=offset ) )
    #
    #   return jobList


    ##########################################################################
    #def loadSubmitted( self, attributes = None, limit=None, offset=None  ) :
    #    """
    #    retrieve information from db for jobs submitted using:
    #    - range of tasks
    #    - range of jobs inside a task
    #
    #    Takes the highest submission number for each job
    #    """
    #
    #    if attributes is None :
    #        attributes = { 'closed' : 'N' }
    #    else :
    #        attributes['closed'] = 'N'
    #
    #    return self.loadJobsByRunningAttr(
    #        attributes, limit=limit, offset=offset )


    ##########################################################################
    #ef loadEnded( self, attributes = None, limit=None, offset=None ) :
    #   """
    #   retrieve information from db for jobs successfully using:
    #   - range of tasks
    #   - range of jobs inside a task
    #
    #   Takes the highest submission number for each job
    #   """
    #
    #   if attributes is None :
    #       attributes = { 'status' : 'SD' }
    #   else :
    #       attributes['status'] = 'SD'
    #
    #   return self.loadJobsByRunningAttr(
    #       attributes, limit=limit, offset=offset )


    ##########################################################################
    #def loadFailed( self, attributes = None, limit=None, offset=None ) :
    #    """
    #    retrieve information from db for jobs aborted/killed using:
    #    - range of tasks
    #    - range of jobs inside a task
    #
    #    Takes the highest submission number for each job
    #    """
    #
    #    if attributes is None :
    #        attributes = { 'status' : 'A' }
    #    else :
    #        attributes['status'] = 'A'
    #
    #    # load aborted
    #    jobList = self.loadJobsByRunningAttr(
    #        attributes, limit=limit, offset=offset )
    #
    #    # load killed
    #    attributes['status'] = 'K'
    #    jobList.extend( self.loadJobsByRunningAttr(
    #        attributes, limit=limit, offset=offset ) )
    #
    #    return jobList


    ##########################################################################
    def loadJobDistinct( self, taskId, distinctAttr, jobAttributes=None ):
        """
        retrieve job templates with distinct job attribute
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating job
        if jobAttributes is None :
            job = Job()
        else :
            job = Job( jobAttributes )
        job['taskId'] =  taskId

        # load job from db
        jobList = self.db.selectDistinct(job, distinctAttr)

        return jobList


    ##########################################################################
    def loadRunJobDistinct( self, taskId, distinctAttr, jobAttributes=None ):
        """
        retrieve job templates with distinct job attribute
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating job
        if jobAttributes is None :
            job = RunningJob()
        else :
            job = RunningJob( jobAttributes )
        job['taskId'] =  taskId

        # load job from db
        jobList = self.db.selectDistinct(job, distinctAttr)

        return jobList


    ##########################################################################
    #def getTaskFromJob( self, job):
    #    """
    #    retrieve Task object from Job object and perform association
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # creating task
    #    task = self.loadTask(job['taskId'], None)
    #
    #    # perform association
    #    task.appendJob( job )
    #
    #    # operation validity checks
    #    if len( task.jobs ) != 1 :
    #        raise DbError( "ERROR: too many jobs loaded %s" % \
    #                             len( task.jobs ))
    #    if id( task.jobs[0] ) != id( job ) :
    #        raise DbError( "Fatal ERROR: mismatching job" )
    #
    #    # return task
    #    return task


    ##########################################################################
    ## DanieleS.
    def loadJobDist( self, taskId, value ) :
        """
        retrieve job distinct job attribute
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating job
        jobAttributes = { 'taskId' : taskId}
        job = Job( jobAttributes )

        # load job from db
        jobList = self.db.distinct(job, value)

        return jobList

    ## DanieleS. NOTE: ToBeRevisited
    def loadJobDistAttr( self, taskId, value_1, value_2, alist ) :
        """
        retrieve job distinct job attribute
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating job
        jobAttributes = { 'taskId' : taskId}
        job = Job( jobAttributes )

        # load job from db
        jobList = self.db.distinctAttr(job, value_1, value_2, alist )

        return jobList


    ##########################################################################
    #def archive( self, obj ):
    #    """
    #    set a flag/index to closed
    #    """
    #
    #    # db connect
    #    if self.db is None :
    #        self.connect()
    #
    #    # the object passed is a Job
    #    if type(obj) == Job :
    #        obj.runningJob['closed'] = 'Y'
    #        # obj.closeRunningInstance( self.db )
    #
    #    # the object passed is a Task
    #    elif type(obj) == Task :
    #        for job in obj.jobs:
    #            job.runningJob['closed'] = 'Y'
    #            # job.closeRunningInstance( self.db )
    #
    #    # update object
    #    obj.update(self.db)
    #    self.bossLiteDB.commit()


    ##########################################################################
    def deserialize( self, xmlFilePath ) :
        """
        obtain task object from XML object
        """

        from xml.dom import minidom
        doc = minidom.parse( xmlFilePath )

        # parse the task general attributes
        taskNode = doc.getElementsByTagName("TaskAttributes")[0]
        taskInfo =  {}
        for i in range(taskNode.attributes.length):

            key = str(taskNode.attributes.item(i).name)
            val = str(taskNode.attributes.item(i).value)
            if Task.defaults[key] == []:
                val = val.split(',')
            taskInfo[key] = val

        # run over the task jobs and parse the structure
        jnodes = doc.getElementsByTagName("Job")
        jobs = []
        runningJobsAttribs = {}
        for jobNode in jnodes:

            jobInfo = {}
            for i in range(jobNode.attributes.length):
                key = str(jobNode.attributes.item(i).name)
                val = str(jobNode.attributes.item(i).value)
                if Job.defaults[key] == []:
                    val = val.split(',')
                jobInfo[key] = val
            jobs.append(jobInfo)

            rjAttrs = {}
            rjl = jobNode.getElementsByTagName("RunningJob")
            if len(rjl) > 0:
                rjNode = rjl[0]
                for i in range(rjNode.attributes.length):
                    key = str(rjNode.attributes.item(i).name)
                    val = str(rjNode.attributes.item(i).value)
                    if RunningJob.defaults[key] == []:
                        val = val.split(',')
                    rjAttrs[key] = val
            runningJobsAttribs[ jobInfo['name'] ] = copy.deepcopy(rjAttrs)

        # return objects
        return taskInfo, jobs, runningJobsAttribs


    ##########################################################################
    def serialize( self, task ):
        """
        obtain XML object from task object
        """

        from xml.dom import minidom

        cfile = minidom.Document()
        root = cfile.createElement("Task")

        node = cfile.createElement("TaskAttributes")
        for key, value in task.data.iteritems():
            if key == 'id' or value == task.defaults[key] :
                continue
            if task.defaults[key] == []:
                value = str( ','.join(value))
            node.setAttribute( key, str(value) )
        root.appendChild(node)

        node = cfile.createElement("TaskJobs")
        for job in task.jobs:
            subNode = cfile.createElement("Job")
            for key, value in job.data.iteritems():
                if key == 'id' or value == job.defaults[key] :
                    continue
                if job.defaults[key] == []:
                    value = str( ','.join(value))
                subNode.setAttribute( key, str(value) )
            node.appendChild(subNode)

            if job.runningJob is not None:
                subSubNode = cfile.createElement("RunningJob")
                for key, value in job.runningJob.data.iteritems():
                    if key == 'id' or value == job.runningJob.defaults[key] :
                        continue
                    if job.runningJob.defaults[key] == []:
                        value = str( ','.join(value))
                    subSubNode.setAttribute(key, str(job.runningJob[key]) )
                subNode.appendChild(subSubNode)

        root.appendChild(node)
        cfile.appendChild(root)

        # return xml string
        return cfile.toprettyxml().replace('\&quot;','')


    ##########################################################################


