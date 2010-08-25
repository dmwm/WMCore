#!/usr/bin/env python
"""
_Job_

"""

__version__ = "$Id: Job.py,v 1.2 2010/03/30 15:31:33 mnorman Exp $"
__revision__ = "$Revision: 1.2 $"


# imports
import logging

# WMCore objects
from WMCore.Services.UUID import makeUUID


from WMCore.BossLite.Common.Exceptions    import JobError, DbError
from WMCore.BossLite.DbObjects.DbObject   import DbObject, dbTransaction
from WMCore.BossLite.DbObjects.RunningJob import RunningJob

from WMCore.DAOFactory   import DAOFactory

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




    ##########################################################################

    @dbTransaction
    def create(self):
        """
        Create a new instance of a job

        """
        if not self.exists():
            action = self.daofactory(classname = "Job.New")
            action.execute(binds = self.data,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)

        return


####################################################################

    @dbTransaction
    def exists(self, noDB = False):
        """
        Check to see if the job exists

        """

        

        if noDB:
            if self.data['id'] < 0:
                return False
            else:
                return self.data['id']

        # Then use the database    
        action = self.daofactory(classname = "Job.Exists")
        id = action.execute(name = self.data['name'],
                            conn = self.getDBConn(),
                            transaction = self.existingTransaction)
        if id:
            self.data['id'] = id
        self.existsInDataBase = True
        return id


###############################################################

    @dbTransaction
    def save(self):
        """
        Save the object into the database
        """

        if not self.exists():
            # Then we don't have an entry yet
            self.create()
        else:
            action = self.daofactory(classname = "Job.Save")
            action.execute(binds = self.data,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
            # create entry for runningJob
            if self.runningJob is not None:
                self.runningJob['jobId']      = self.data['jobId']
                self.runningJob['taskId']     = self.data['taskId']
                self.runningJob['submission'] = self.data['submissionNumber']
                self.runningJob.save()


        return

    ######################################################################

    @dbTransaction
    def load(self):
        """
        Load the job info from the database
        """

        if self.data['id'] > 0:
            # Then load by ID
            action = self.daofactory(classname = "Job.LoadByID")
            result = action.execute(id = self.data['id'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
        elif self.data['jobId'] > 0:
            # Then use jobID
            action = self.daofactory(classname = "Job.LoadByJobID")
            result = action.execute(jobId = self.data['jobId'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)


        elif self.data['name']:
            # Then use name
            action = self.daofactory(classname = "Job.LoadByName")
            result = action.execute(name = self.data['name'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)

        else:
            # We have no identifiers.  We're screwed
            return



        if result == []:
            # Then the job doesn't exist!
            logging.error('Attempted to load non-existant job!')
            return

        # If it's internal, we only want the first job
        self.data.update(result[0])
        self.existsInDataBase = True


        
        return

    #####################################################################

    def getRunningInstance(self):
        """
        get running job information
        """

        parameters = {'jobId': self.data['jobId'],
                      'taskId': self.data['taskId'],
                      'submission': self.data['submissionNumber']}
        runJob = RunningJob(parameters = parameters)
        runJob.load()

        if not runJob.exists():  # Not happy with this call because it's slow.  Maybe use ID?
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
                      'submission': self.data['submission']}
        self.runningJob = RunningJob(parameters = parameters)

        return

###############################################################################
    # ACHTUNG!
    # This is where I stopped doing anything
###############################################################################

#    def save(self, db):
#        """
#        save complete job object in database
#        """
#
#        # verify data is complete
#        if not self.valid(['jobId', 'taskId', 'name']):
#            raise JobError("The following job instance cannot be saved," + \
#                     " since it is not completely specified: %s" % self)
#
#        # insert job
#        try:
#
#            # create entry in database
#            status = db.insert(self)
#            if status != 1:
#                raise JobError("Cannot insert job %s" % self)
#
#            # create entry for runningJob
#            if self.runningJob is not None:
#                self.runningJob['jobId'] = self.data['jobId']
#                self.runningJob['taskId'] = self.data['taskId']
#                self.runningJob['submission'] = self.data['submissionNumber']
#                self.runningJob.save( db )
#
#        # database error
#        except DbError, msg:
#            raise JobError(str(msg))
#
#        # update status
#        self.existsInDataBase = True
#
#        return status

    ##########################################################################

    def remove(self, db):
        """
        remove job object from database
        """

        # verify data is complete
        if not self.valid(['jobId', 'taskId']):
            raise JobError("The following job instance cannot be removed," + \
                     " since it is not completely specified: %s" % self)

        # remove from database
        try:
            status = db.delete(self)
            if status < 1:
                raise JobError("Cannot remove job %s" % self)

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # update status
        self.existsInDataBase = False

        # return number of entries removed
        return status

    ##########################################################################

    def update(self, db, deep = True):
        """
        update job information in database
        """

        # verify if the object exists in database
        if not self.existsInDataBase:

            # no, use save instead of update
            return self.save(db)

        # verify data is complete
        if not self.valid(['jobId', 'taskId']):
            raise JobError("The following job instance cannot be updated," + \
                     " since it is not completely specified: %s" % self)

        # update it on database
        try:
            status = db.update(self)

            # update running job if associated
            if deep and self.runningJob is not None:
                if self.data['submissionNumber'] != \
                       self.runningJob['submission']:
                    raise JobError(
                        "Running instance of job %s.%s with invalid " \
                        + " submission number: %s instead of %s " \
                        % ( self.data['jobId'], self.data['taskId'], \
                            self.runningJob['submission'], \
                            self.data['submissionNumber'] ) )
                status += self.runningJob.update(db)

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # return number of entries updated.
        return status





   ##########################################################################

#    def load(self, db, deep = True):
#        """
#        load information from database
#        """
#
#        # verify data is complete
#        if not self.valid(['id']) and not self.valid(['name']) and \
#           not self.valid(['jobId']):
#            raise JobError("The following job instance cannot be loaded" + \
#                     " since it is not completely specified: %s" % self)
#
#        # get information from database based on template object
#        try:
#            objects = db.select(self)
#
#        # database error
#        except DbError, msg:
#            raise JobError(str(msg))
#
#        # since required data is a key, it should be a single object list
#        if len(objects) == 0:
#            raise JobError("No job instances corresponds to the," + \
#                     " template specified: %s" % self)
#
#        if len(objects) > 1:
#            raise JobError("Multiple job instances corresponds to the" + \
#                     " template specified: %s" % self)
#
#        # copy fields
#        for key in self.fields:
#            self.data[key] = objects[0][key]
#
#        # get associated running job if it exists
#        if deep:
#            self.getRunningInstance(db)
#
#        # update status
#        self.existsInDataBase = True

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

    ##########################################################################

#    def newRunningInstance(self, runningJob, db):
#        """
#        set currently running job
#        """
#
#        # close previous running instance (if any)
#        self.closeRunningInstance(db)
#
#        # update job id and submission counter
#        runningJob['jobId'] = self.data['jobId']
#        runningJob['taskId'] = self.data['taskId']
#        self.data['submissionNumber'] += 1
#        runningJob['submission'] = self.data['submissionNumber']
#        runningJob.existsInDataBase = False
#        
#        # store instance
#        self.runningJob = runningJob

    ##########################################################################
#
#    def getRunningInstance(self, db):
#        """
#        get running job information
#        """
#
#        # create template
#        template = RunningJob()
#        template['jobId'] = self['jobId']
#        template['taskId'] = self['taskId']
#        template['submission'] = self['submissionNumber']
#        # template['closed'] = "N"
#
#        # get running job
#        runningJobs = db.select(template)
#
#        # no running instance
#        if runningJobs == []:
#            self.runningJob = None
#
#        # one running instance
#        elif len(runningJobs) == 1:
#            self.runningJob = runningJobs[0]
#
#        # oops, more than one!
#        else:
#            raise JobError("Multiple running instances of job %s : %s" % \
#                           (self['jobId'], len(runningJobs)))
#
    ##########################################################################
#
#    def updateRunningInstance(self, db, notSkipClosed = True):
#        """
#        update current running job
#        """
#
#        # check consistency
#        if self.runningJob['taskId'] != self.data['taskId'] or \
#               self.runningJob['jobId'] != self.data['jobId'] or \
#               self.runningJob['submission'] != self.data['submissionNumber'] :
#            raise JobError( "Running instance of job %s.%s with invalid " \
#                            + " submission number: %s instead of %s " \
#                            % ( self.data['jobId'], self.data['taskId'], \
#                                self.runningJob['submission'], \
#                                self.data['submissionNumber'] ) )
#
#        # update
#        self.runningJob.update(db, notSkipClosed)
        
    ##########################################################################

#    def closeRunningInstance(self, db):
#        """
#        close the running instance.
#        it should be only one but ignore if there are more than one...
#        """
#
#        # do not do anything if the job is not completely defined
#        if not self.valid(['jobId', 'taskId']):
#            return
#
#        # create template
#        template = RunningJob()
#        template['jobId'] = self['jobId']
#        template['taskId'] = self['taskId']
#        template['closed'] = "N"
#
#        # get running job
#        runningJobs = db.select(template)
#
#        # do for all of them (should be one...)
#        for job in runningJobs:
#
#            job["closed"] = "Y"
#            job.update(db)






