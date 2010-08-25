#!/usr/bin/env python
"""
_BossLiteAPI_

"""

__version__ = "$Id: BossLiteAPI.py,v 1.5 2010/05/14 11:21:50 spigafi Exp $"
__revision__ = "$Revision: 1.5 $"

import logging
import copy

# Task and job objects
from WMCore.BossLite.DbObjects.Job        import Job
from WMCore.BossLite.DbObjects.Task       import Task
from WMCore.BossLite.DbObjects.RunningJob import RunningJob
from WMCore.BossLite.Common.Exceptions    import TaskError, JobError, DbError

# database engine
from WMCore.BossLite.DbObjects.BossLiteDBWM    import BossLiteDBWM


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


class BossLiteAPI(object):
    """
    High level API class for DBObjets.
    It allows load/operate/update jobs and tasks using just id ranges
    """

    def __init__(self, database = "WMCore", dbConfig=None, pool=None, makePool=False):
        """
        initialize the API instance
        """
        
        # Select the database engine
        if database == "WMCore":
            self.db = BossLiteDBWM()
        else :
            raise NotImplementedError
        
        return
    
    ##########################################################################
    # General purpose Methods
    ##########################################################################
    
    def archive(self, obj ):
        """
        Close running jobs. Works for either task or job
        """

        # the object passed is a Job
        if type(obj) == Job :
            obj.runningJob['closed'] = 'Y'

        # the object passed is a Task
        elif type(obj) == Task :
            for job in obj.jobs:
                job.runningJob['closed'] = 'Y'

        # update object
        obj.update(self.db)
    
    
    ##########################################################################
    # Methods for Task
    ##########################################################################

    def saveTask( self, task ):
        """
        register task related informations in the db
        """

        # save task
        # Is this try/except necessary? Yes
         
        try :
            task.save(self.db)
        except Exception, err:
            
            # These statements reuire a check...
            #if str(err).find( 'column name is not unique') == -1 and \
            #       str(err).find( 'Duplicate entry') == -1 and \
            #       task['id'] is not None :
            #    self.removeTask( task )
            #    task = None
            
            # Raise... what???
            raise

        return task

    
    def loadTask( self, taskId, jobRange='all' ) :
        """
        Retrieve task information from db using task 'taskId' and
        - jobRange = None  : no jobs are loaded
        - jobRange = 'all' : all jobs are loaded
        - jobRange = range : only selected jobs are loaded - NOT IMPLEMENTED
        """
        
        # create template for task
        task = Task(parameters = {'id': int(taskId)})

        if jobRange == 'all':
            #the default: load all jobs 
            task.load(self.db)
        elif  jobRange is None :
            task.load(self.db, deep = False)
        else :
            raise NotImplementedError
        
        return task
    
    
    def loadTaskByName( self, name, jobRange='all' ) :
        """
        Retrieve task information from db using task 'name' and
        - jobRange = None  : no jobs are loaded
        - jobRange = 'all' : all jobs are loaded
        - jobRange = range : only selected jobs are loaded - NOT IMPLEMENTED
        """

        # create template for task and load
        task = Task(parameters = {'name': name})

        if jobRange == 'all':
            #the default: load all jobs 
            task.load(self.db)
        elif  jobRange is None :
            task.load(self.db, deep = False)
        else :
            raise NotImplementedError
        
        return task

    
    def loadTasksByAttr( self, binds ) :
        """
        Retrieve list of tasks from db matching the list of task attributes
        """
        
        if not type(binds) == dict :
            # 'binds' must be a dictionary!
            raise Exception
        
        tasks = []
        
        result = self.db.objAdvancedLoad(obj = Task(), binds = binds)
        
        for x in result : 
            tmp = Task()
            tmp.data.update(x)
            tmp.existsInDataBase = True
            tasks.append(tmp)
            
        return tasks

    def removeTask(self, task):
        """
        remove task, jobs and their running instances from db
        NOT SQLite safe (why? NdFilippo)
        """

        # remove task
        task.remove(self.db)

        return
    
    ##########################################################################
    # Methods for Job
    ##########################################################################

    def loadJob( self, taskId, jobId ) :
        """
        retrieve job information from db using task and job id
        """

        # creating job
        jobAttributes = { 'taskId' : taskId, "jobId" : jobId}
        job = Job(parameters = jobAttributes)

        # load job from db
        job.load(self.db)

        return job


    # this method is broken!!!! 
    def loadJobsByAttr( self, jobAttribute, value) :
        """
        retrieve job information from db for job matching attributes
        """
        
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
        """
        
        # creating jobs
        job = Job( jobAttribute )

        # load job from db
        jobList = self.db.select(job)

        return jobList
    

    def loadJobByName( self, jobName ) :
        """
        retrieve job information from db for jobs with name 'name'
        """

        params = {'name': jobName}
        job = Job(parameters = params)
        job.load(load.db)

        return job

  
    def getRunningInstance( self, job, runningAttrs = None ) :
        """
        retrieve RunningInstance where existing or create it
        """

        """
        # check whether the running instance is still loaded
        if job.runningJob is not None :
            return

        # load if exixts
        job.getRunningInstance(self.db)

        # create it otherwise
        if job.runningJob is None :
            job.newRunningInstance(self.db)
            if type(runningAttrs) == dict:
                job.runningJob.data.update(runningAttrs)
        """
           
        return NotImplementedError
    

    def getNewRunningInstance( self, job, runningAttrs = None ) :
        """
        create a new RunningInstance
        
        why a "get" method updates the fileds????
        """

        job.newRunningInstance(self.db)
        
        if type(runningAttrs) == dict:
            job.runningJob.data.update(runningAttrs)

        return
    

    def loadJobsByRunningAttr( self, attribute, value) :
        """
        retrieve job information from db for job
        whose running instance match attributes
        
        --> need a check!
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
    # Serialize Task in XML and viceversa
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

