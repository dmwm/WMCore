#!/usr/bin/env python
"""
_BossLiteAPI_

"""




#import logging
import copy

# Task and job objects
from WMCore.BossLite.DbObjects.Job        import Job
from WMCore.BossLite.DbObjects.Task       import Task
from WMCore.BossLite.DbObjects.RunningJob import RunningJob
from WMCore.BossLite.Common.Exceptions    import TaskError, JobError

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
    High level API class for DBObjets. It allows load/operate/update jobs 
    and tasks using just id ranges.
    """

    def __init__(self, database = "WMCore"):
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
        
        return task.save(self.db)

    
    def loadTask( self, taskId, jobRange='all' ) :
        """
        Retrieve task information from db using task 'taskId' and
        - jobRange = None  : no jobs are loaded
        - jobRange = 'all' : all jobs are loaded
        - jobRange = range : only selected jobs are loaded 
        """
        
        # create template for task
        task = Task(parameters = {'id': int(taskId)})

        if jobRange == 'all':
            #the default: load all jobs 
            task.load(self.db)
        elif  jobRange is None :
            task.load(self.db, deep = False)
        else :
            task.load(self.db, jobRange = parseRange(jobRange))
        
        return task
    
    
    def loadTaskByName( self, name, jobRange='all' ) :
        """
        Retrieve task information from db using task 'name' and
        - jobRange = None  : no jobs are loaded
        - jobRange = 'all' : all jobs are loaded
        - jobRange = range : only selected jobs are loaded 
        """

        # create template for task and load
        task = Task(parameters = {'name': name})

        if jobRange == 'all':
            #the default: load all jobs 
            task.load(self.db)
        elif  jobRange is None :
            task.load(self.db, deep = False)
        else :
            task.load(self.db, jobRange = parseRange(jobRange))
        
        return task

    
    def loadTasksByAttr( self, binds, deep = True ) :
        """
        Retrieve list of tasks from db matching the list of task attributes
        """
        
        if not type(binds) == dict :
            msg = "BossLiteAPI.loadTasksByAttr: "
            msg += "binds must be a dictionary."
            raise TaskError(msg)
        
        tasks = []
        
        result = self.db.objAdvancedLoad(obj = Task(), binds = binds)
        
        for x in result : 
            tmp = Task()
            tmp.data.update(x)
            tmp.existsInDataBase = True
            
            if deep :
                tmp.loadJobs(self.db)
                
            tasks.append(tmp)
            
        return tasks
    
    
    def removeTask(self, task):
        """
        remove task, jobs and their running instances from db
        """
        
        task.remove(self.db)

        return
    
    
    ##########################################################################
    # Methods for Job
    ##########################################################################

    def loadJob( self, taskId, jobId, deep = True ) :
        """
        retrieve job information from db using task and job id
        """

        jobAttributes = { 'taskId' : taskId, "jobId" : jobId}
        job = Job(parameters = jobAttributes)

        job.load(self.db, deep)
        
        return job

    
    def loadJobsByAttr( self, binds, deep = True) :
        """
        retrieve job information from db for job matching attributes
        """
        
        if not type(binds) == dict :
            msg = "BossLiteAPI.loadJobsByAttr: "
            msg += "binds must be a dictionary."
            raise JobError(msg)
        
        
        jobs = []
        
        result = self.db.objAdvancedLoad(obj = Job(), binds = binds)
        
        if not type(result) == list :
            result = [result]
        
        for x in result : 
            tmp = Job()
            tmp.data.update(x)
            
            if deep :
                tmp.getRunningInstance(self.db)
            
            tmp.existsInDataBase = True
            jobs.append(tmp)
            
        return jobs
    
    
    def loadJobByName( self, jobName, deep = True ) :
        """
        retrieve job information from db for jobs with name 'name'
        """

        params = {'name': jobName}
        job = Job(parameters = params)
        job.load(self.db, deep)

        return job


    def getTaskFromJob( self, job):
        """
        retrieve Task object from Job object and perform association
        """

        # creating/load task
        task = self.loadTask(taskId = job['taskId'], jobRange = None)

        # perform association
        task.appendJob(job)

        # operation validity checks -- are necessary?
        if len( task.jobs ) != 1 :
            msg = "BossLiteAPI.getTaskFromJob: "
            msg += "error, too many jobs loaded %s" % \
                                 len( task.jobs )
            raise TaskError(msg)
        
        if id( task.jobs[0] ) != id( job ) :
            msg = "BossLiteAPI.getTaskFromJob: "
            msg += "fatal error, mismatching job."
            raise TaskError(msg)
        
        return task
    
    
    def removeJob( self, job ):
        """
        remove job and its running instances from db
        """
        
        return job.remove(self.db)
    

    ##########################################################################
    # Methods for RunningJob
    ##########################################################################

    def getRunningInstance( self, job, runningAttrs = None ) :
        """
        retrieve RunningInstance where existing or create it
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
        
        return job.runningJob
    

    def getNewRunningInstance( self, job, runningAttrs = None ) :
        """
        create a new RunningInstance
        """     

        job.newRunningInstance(self.db)
        
        if type(runningAttrs) == dict:
            job.runningJob.data.update(runningAttrs)

        return job.runningJob
    
    
    def updateRunningInstances( self, task ) :
        """
        update runningInstances of a task in the DB
        """
        
        for job in task.jobs:

            # update only loaded acrive instances
            if job.runningJob is not None:
                job.updateRunningInstance(self.db)

        return
    
    
    ##########################################################################
    # Methods for Combined Job-RunningJob
    ##########################################################################

    def loadJobsByRunningAttr( self, binds, limit = None) :
        """
        retrieve job information from db for job
        whose running instance match attributes
        """
    
        jobList = []
        
        results  = self.db.jobLoadByRunningAttr(binds = binds, limit = limit)
        
        for entry in results:
            job = Job()
            job.data.update(entry)
            self.getRunningInstance(job = job )
            jobList.append(job)
                       
        return jobList
    
    
    def loadJobsByTimestamp( self, time_binds, standard_binds = None, 
                                                            limit = None ) :
        """
        retrieve job information from db for job whose running 
        instance match time attributes
        """
        
        binds = {}
        
        binds.update(time_binds)
        if standard_binds :
            binds.update(standard_binds)
        
        results  = self.loadJobsByRunningAttr(binds = binds, limit = limit)
        
        return results
    
    
    def loadCreated( self, limit = None ) :
        """
        retrieve information from db for jobs created but not submitted
        """
        
        return self.loadJobsByRunningAttr(binds = {'status' : 'W'}, 
                                                        limit = limit )
    
    
    def loadSubmitted( self, limit = None ) :
        """
        retrieve information from db for jobs submitted
        """

        return self.loadJobsByRunningAttr(binds = {'closed' : 'N'}, 
                                                        limit = limit )
    
    
    def loadEnded( self, limit = None ) :
        """
        retrieve information from db for jobs successfully
        """
        
        return self.loadJobsByRunningAttr(binds = {'status' : 'SD'}, 
                                                        limit = limit )
    
    
    def loadFailed( self, limit = None ) :
        """
        retrieve information from db for jobs aborted/killed
        """
        
        jobList = []
        jobList.extend(self.loadJobsByRunningAttr(binds = {'status' : 'A'},
                                                                limit = limit))
        jobList.extend(self.loadJobsByRunningAttr(binds = {'status' : 'K'},
                                                                limit = limit))
        
        return jobList
    
    
    ##########################################################################
    # Missing Methods - Not Implemented Yet
    ##########################################################################
    
    def loadJobDistinct( self ):
        """
        retrieve job templates with distinct job attribute
        """
        
        raise NotImplementedError

    
    def loadRunJobDistinct( self ):
        """
        retrieve job templates with distinct job attribute
        """
        
        raise NotImplementedError


    def loadJobDist( self ) :
        """
        retrieve job distinct job attribute
        """

        raise NotImplementedError


    def loadJobDistAttr( self ) :
        """
        retrieve job distinct job attribute
        """

        raise NotImplementedError


    ##########################################################################
    # Serialize Task in XML and vice-versa
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

    
    def serialize( self, task ) :
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
        put a description here
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
    
    
    def declareNewJobs( self, xml, task, proxyFile=None ) :
        """
        Register job related informations in the db
        """
        
        taskInfo, jobInfos, rjAttrs = self.deserialize( xml )
        jobRange= range(len(task.getJobs())+1)
        jobRange.remove(0)  
        
        # reconstruct jobs and fill the data
        jobs = []
        for jI in jobInfos:
            # check if jobs is new 
            if int(jI['jobId']) not in jobRange:
                job = Job( jI )
                job['taskId'] = task['id']  
                subn = int( job['submissionNumber'] )
                
                if subn > 0 :
                    job['submissionNumber'] = subn - 1
                else :
                    job['submissionNumber'] = subn
                #jobs.append(job)
                task.appendJob(job)
        
        for job in task.jobs:
            if int(job['jobId']) not in jobRange:
                attrs = rjAttrs[ str(job['name']) ]
                self.getRunningInstance( job, attrs )
                self.updateDB( job )
          
        # self.updateDB( task )

        # all done
        return task
    
    
    ##########################################################################
    # Methods for general DbObject
    ##########################################################################
    
    def updateDB( self, obj ) :
        """
        Update any object table in the DB. It works for tasks, j
        obs, runningJobs.
        """
        
        obj.update(self.db)
    
