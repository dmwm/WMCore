#!/usr/bin/env python
"""
_Job_

"""

__version__ = "$Id: Job.py,v 1.20 2010/06/02 16:02:27 spigafi Exp $"
__revision__ = "$Revision: 1.20 $"

from WMCore.Services.UUID import makeUUID

from WMCore.BossLite.DbObjects.DbObject   import DbObject, DbObjectDBFormatter
from WMCore.BossLite.DbObjects.RunningJob import RunningJob

from WMCore.BossLite.Common.Exceptions    import JobError
from WMCore.BossLite.Common.System import strToList, listToStr

class Job(DbObject):
    """
    Job object
    """

    # fields on the object and their names on database
    fields = { 'id' : 'id',
               'jobId' : 'job_id',
               'wmbsJobId' : 'wmbsJob_id',
               'taskId' : 'task_id',
               'name' : 'name',
               'executable' : 'executable',
               'events' : 'events',
               'arguments' : 'arguments',
               'standardInput' : 'stdin',
               'standardOutput' : 'stdout',
               'standardError' : 'stderr',
               'inputFiles' : 'input_files',
               'outputFiles' : 'output_files',
               'dlsDestination' : 'dls_destination',
               'submissionNumber' : 'submission_number',
               'closed' : 'closed'
              }

    # mapping between field names and database fields
    mapping = fields

    # private non mapped fields
    private = { 'fullPathOutputFiles' : [],
                'fullPathInputFiles' : [] }

    # default values for fields
    defaults = { 'id' : None,
                 'jobId' : None,
                 'wmbsJobId' : None,
                 'taskId' : None,
                 'name' : None,
                 'executable' : None,
                 'events' : 0,
                 'arguments' : "",
                 'standardInput' : "",
                 'standardOutput' : "",
                 'standardError' : "",
                 'inputFiles' : [],
                 'outputFiles' : [],
                 'dlsDestination' : [],
                 'submissionNumber' : 0,
                 'closed' : 'N'
              }
    
    # exception class
    exception = JobError
    
    
    def __init__(self, parameters = None):
        """
        initialize a Job instance
        """

        # call super class init method
        DbObject.__init__(self, parameters)

        # initialize running job
        self.runningJob = None

        # Init a bunch of variables to reasonable values
        if not self.data['name']:
            self.data['name'] = makeUUID()
        if not self.data['id']:
            self.data['id'] = -1
            
        # object not in database
        self.existsInDataBase = False
        
        return
    
    
    def create(self, db):
        """
        Create a new instance of a job
        """
        
        db.objCreate(self)
        
        self.existsInDataBase = True
        
        return
    

    def exists(self, db, noDB = False):
        """
        Check to see if the job exists
        """
        
        if noDB:
            if self.data['id'] < 0:
                return False
            else:
                return self.data['id']

        # Then use the database    
        tmpId = db.objExists(self)
        
        if tmpId:
            self.data['id'] = tmpId
        
        return tmpId
    
    
    def save(self, db, deep = True):
        """
        Save the object into the database
        """

        if not self.existsInDataBase:
            self.create(db)
        else:
            db.objSave(self)
            
        self.existsInDataBase = True
        
        # create entry for runningJob
        if deep and self.runningJob is not None:
            # consistency - needed everytime?
            self.runningJob['jobId']      = self.data['jobId']
            self.runningJob['taskId']     = self.data['taskId']
            self.runningJob['submission'] = self.data['submissionNumber']
            self.runningJob.save(db)
            
        return
    
    
    def load(self, db, deep = True):
        """
        Load the job info from the database
        """
        
        result = db.objLoad(self)
        
        if result == []:
            raise JobError("No job instances corresponds to the," + \
                     " template specified: %s" % self)
        
        if len(result) > 1 :
            raise JobError("Multiple job instances corresponds to the" + \
                     " template specified: %s" % self)

        self.data.update(result[0])
        
        if deep :
            self.getRunningInstance(db)
        
        self.existsInDataBase = True
        
        return
    
    
    def getRunningInstance(self, db):
        """
        get running job information
        """

        parameters = {'jobId': self.data['jobId'],
                      'taskId': self.data['taskId'],
                      'submission': self.data['submissionNumber']}
        runJob = RunningJob(parameters = parameters)
        
        runJob.load(db)

        if not runJob.existsInDataBase:  
            self.runningJob = None
        else:
            self.runningJob = runJob
        
        return
    
    
    def closeRunningInstance(self, db):
        """
        close the running instance.
        it should be only one but ignore if there are more than one...
        """
        
        # do not do anything if the job is not completely defined
        if not self.valid(['jobId', 'taskId']):
            # raise exception?
            return

        if not self.runningJob:
            self.getRunningInstance(db)
            if not self.runningJob:
                # Then we didn't have one and we couldn't load one
                return
            
        self.runningJob.data['closed'] = 'Y'
        self.runningJob.save(db)

        return
    
    
    def updateRunningInstance(self, db):
        """
        update current running job
        """

        if not self.runningJob:
            self.getRunningInstance(db)
            
        if not self.runningJob:
            # Then we couldn't load it either
            # maybe rise an exception?
            return

        # check consistency
        if self.runningJob['taskId'] != self.data['taskId'] or \
               self.runningJob['jobId'] != self.data['jobId'] or \
               self.runningJob['submission'] != self.data['submissionNumber'] :
            raise JobError( "Running instance of job %s.%s with invalid " \
                            + " submission number: %s instead of %s " \
                            % ( self.data['jobId'], self.data['taskId'], \
                                self.runningJob['submission'], \
                                self.data['submissionNumber'] ) )

        # update
        self.runningJob.save(db)

        return
    
    
    def newRunningInstance(self, db):
        """
        set currently running job
        """

        # close previous running instance (if any)
        self.closeRunningInstance(db)
        
        self.data['submissionNumber'] += 1
        parameters = {'jobId': self.data['jobId'],
                      'taskId': self.data['taskId'],
                      'submission': self.data['submissionNumber']}
        self.runningJob = RunningJob(parameters = parameters)

        return
    
    
    def update(self, db, deep = True):
        """
        update job information in database
        """
        
        return self.save(db, deep)
    
    
    def remove(self, db):
        """
        remove job object from database
        """

        if not self.existsInDataBase:
            raise JobError("The following job instance cannot be removed," + \
                     " since it is not completely specified: %s" % self) 
        
        
        db.objRemove(self)
        
        self.existsInDataBase = False

        # return number of entries removed
        return 1
    
    
    def setRunningInstance(self, runningJob):
        """
        set currently running job
        """
        
        # check if the running instance is plain
        if not runningJob.valid(['taskId']) :
            runningJob['taskId'] = self.data['taskId']
        if not runningJob.valid(['jobId']) :
            runningJob['jobId'] = self.data['jobId']
        if not runningJob.valid(['submission']) :
            runningJob['submission'] = self.data['submissionNumber']
        
        # check consistency
        if runningJob['taskId'] != self.data['taskId'] or \
               runningJob['jobId'] != self.data['jobId'] or \
               runningJob['submission'] != self.data['submissionNumber'] :
            raise JobError("Invalid running instance with keys %s.%s.%s " + \
                           " instead of %s.%s.%s" % ( \
            str(runningJob['taskId']), str(runningJob['jobId']),
            str(runningJob['submission']), str(self.data['taskId']), \
            str(self.data['jobId']), str(self.data['submissionNumber']) ) )
        
        # store instance
        self.runningJob = runningJob
        
        return
    

class JobDBFormatter(DbObjectDBFormatter):
    """
    JobDBFormatter
    """

    def preFormat(self, res):
        """
        It maps database fields with object dictionary and it translate python 
        List and timestamps in well formatted string. This is useful for any 
        kind of database engine!
        """
        
        result = {}
        
        # result['id']               = entry['id']
        result['jobId']            = res['jobId']
        result['taskId']           = res['taskId']
        result['name']             = res['name']
        result['executable']       = res['executable']
        result['events']           = res['events']
        result['arguments']        = res['arguments']
        result['standardInput']    = res['standardInput']
        result['standardOutput']   = res['standardOutput']
        result['standardError']    = res['standardError']
        result['inputFiles']       = listToStr(res['inputFiles'])
        result['outputFiles']      = listToStr(res['outputFiles'])
        result['dlsDestination']   = listToStr(res['dlsDestination'])
        result['submissionNumber'] = res['submissionNumber']
        result['closed']           = res['closed']
            
        return result
    
    
    def postFormat(self, res):
        """
        Format the results into the right output. This is useful for any 
        kind of database engine!
        """
        
        final = []
        for entry in res:
            result = {}
            result['id']               = entry['id']
            result['jobId']            = entry['jobid']
            result['taskId']           = entry['taskid']
            result['name']             = entry['name']
            result['executable']       = entry['executable']
            result['events']           = entry['events']
            result['arguments']        = entry['arguments']
            result['standardInput']    = entry['standardinput']
            result['standardOutput']   = entry['standardoutput']
            result['standardError']    = entry['standarderror']
            result['inputFiles']       = strToList(entry['inputfiles'])
            result['outputFiles']      = strToList(entry['outputfiles'])
            result['dlsDestination']   = strToList(entry['dlsdestination'])
            result['submissionNumber'] = entry['submissionnumber']
            result['closed']           = entry['closed']

            final.append(result)

        return final
