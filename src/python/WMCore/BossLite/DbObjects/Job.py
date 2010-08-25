#!/usr/bin/env python
"""
_Job_

"""

__version__ = "$Id: Job.py,v 1.11 2010/05/03 09:13:39 spigafi Exp $"
__revision__ = "$Revision: 1.11 $"


# imports
import logging

# WMCore objects
from WMCore.Services.UUID import makeUUID


from WMCore.BossLite.Common.Exceptions    import JobError # , DbError
from WMCore.BossLite.DbObjects.DbObject   import DbObject
from WMCore.BossLite.DbObjects.RunningJob import RunningJob

# from WMCore.DAOFactory   import DAOFactory

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
                 'standardInput' : "None",
                 'standardOutput' : "None",
                 'standardError' : "",
                 'inputFiles' : "",
                 'outputFiles' : "",
                 'dlsDestination' : "",
                 'submissionNumber' : 0,
                 'closed' : None
              }

    # database properties
    tableName = "bl_job"
    tableIndex = ["jobId", "taskId"]

    # exception class
    exception = JobError

    ##########################################################################

    def __init__(self, parameters = {}):
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

    ##########################################################################

    def create(self, db):
        """
        Create a new instance of a job
        """
        
        # is this 'if' necessary? This check is probably duplicated...
        #if not self.existsInDataBase:
        """
        action = self.daofactory(classname = "Job.New")
        action.execute(binds = self.data,
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction)
        """
        db.objCreate(self)
        
        # update ID & check... necessary call!
        if self.exists(db) : 
            self.existsInDataBase = True

    ####################################################################

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
        """
        action = self.daofactory(classname = "Job.Exists")
        tmpId = action.execute(name = self.data['name'],
                            conn = self.getDBConn(),
                            transaction = self.existingTransaction)
        """
        tmpId = db.objExists(self)
        
        if tmpId:
            self.data['id'] = tmpId
        
        return tmpId

    ###############################################################

    def save(self, db, deep = True):
        """
        Save the object into the database
        """

        if not self.existsInDataBase:
            # Then we don't have an entry yet
            self.create(db)
        else:
            """
            action = self.daofactory(classname = "Job.Save")
            action.execute(binds = self.data,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
            """
            db.objSave(self)
            
        # create entry for runningJob
        if deep and self.runningJob is not None:
            # consistency?
            self.runningJob['jobId']      = self.data['jobId']
            self.runningJob['taskId']     = self.data['taskId']
            self.runningJob['submission'] = self.data['submissionNumber']
            self.runningJob.save()
            
        return

    ########################################################################

    def load(self, db, deep = True):
        """
        Load the job info from the database
        """
        
        # consistency check
        if not self.valid(['jobId', 'taskId', 'name']):
            raise JobError("The following job instance cannot be loaded," + \
                     " since it is not completely specified: %s" % self)
        
        """
        # the select MUST be done taking care of these three fields...
        if self.data['id'] > 0:
            # Then load by ID
            value = self.data['id']
            column = 'id'

        elif self.data['jobId'] > 0:
            # Then use jobID
            value = self.data['jobId']
            column = 'job_id'

        elif self.data['name']:
            # Then use name
            value = self.data['name']
            column = 'name'

        else:
            # We have no identifiers.  We're screwed
            # this branch doesn't exist
            return

        # this DAO MUST be reviewed
        action = self.daofactory(classname = "Job.SelectJob")
        result = action.execute(value = value,
                                column = column,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction)
        """
        result = db.objLoad(self)
        
        if result == []:
            # Then the job doesn't exist!
            raise JobError("No job instances corresponds to the," + \
                     " template specified: %s" % self)
        
        if len(result) > 1 :
            # bad message, I would like to change it...
            raise JobError("Multiple job instances corresponds to the" + \
                     " template specified: %s" % self)

        # If it's internal, we only want the first job
        self.data.update(result[0])
        
        if deep :
            self.getRunningInstance()
        
        self.existsInDataBase = True
        
        return

    ########################################################################

    def getRunningInstance(self):
        """
        get running job information
        """

        parameters = {'jobId': self.data['jobId'],
                      'taskId': self.data['taskId'],
                      'submission': self.data['submissionNumber']}
        runJob = RunningJob(parameters = parameters)
        runJob.load()
        
        # Not happy with this call because it's slow.  Maybe use ID?
        # keep track with a boolean variable could be a valid solution (NdFilip)
        if not runJob.exists():  
            self.runningJob = None
        else:
            self.runningJob = runJob


    ########################################################################

    def closeRunningInstance(self):
        """
        close the running instance.
        it should be only one but ignore if there are more than one...
        """
        # do not do anything if the job is not completely defined
        if not self.valid(['jobId', 'taskId']):
            return

        if not self.runningJob:
            self.getRunningInstance()
            if not self.runningJob:
                # Then we didn't have one and we couldn't load one
                return
            
        self.runningJob.data['closed'] = 'Y'
        self.runningJob.save()

        return

    ###################################################################

    def updateRunningInstance(self):
        """
        update current running job
        """

        if not self.runningJob:
            self.getRunningInstance()
            
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
        self.runningJob.save()

        return

    ###########################################################################

    def newRunningInstance(self):
        """
        set currently running job
        """

        # close previous running instance (if any)
        self.closeRunningInstance()

        # WTF!!!!
        # We update the submission number when getting a new runningJob?
        # Not on some other update?
        self.data['submissionNumber'] += 1
        parameters = {'jobId': self.data['jobId'],
                      'taskId': self.data['taskId'],
                      'submission': self.data['submissionNumber']}
        self.runningJob = RunningJob(parameters = parameters)

        return

    ###########################################################################

    def update(self, db, deep = True):
        """
        update job information in database
        """
        
        """
        if deep and self.runningJob:
            self.updateRunningInstance()
            status += 1
        """
        
        return self.save(db, deep)

    ###########################################################################

    def remove(self, db):
        """
        remove job object from database
        """

        if not self.exists(db):
            raise JobError("The following job instance cannot be removed," + \
                     " since it is not completely specified: %s" % self) 
        
        """
        # verify data is complete
        if self.valid(['id']):
            value  = self.data['id']
            column = 'id'

        elif self.valid(['name']):
            value  = self.data['name']
            column = 'name'
            
            # TODO: Find some way to do job_id:task_id
        elif self.valid(['jobId']):
            value  = self.data['jobId']
            column = 'job_id'

        else:
        """ 
        db.objRemove(self)
        
        # update status
        self.existsInDataBase = False

        # return number of entries removed
        # I don't like this (NdFilippo)
        return 1


    ##########################################################################

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
            runningJob['taskId'], runningJob['jobId'],
            runningJob['submission'], self.data['taskId'], \
            self.data['jobId'], self.data['submissionNumber'] ) )
        
        # store instance
        self.runningJob = runningJob
