#!/usr/bin/env python

"""
The JobAccountant algorithm
"""
__all__ = []
__revision__ = "$Id: JobAccountantPoller.py,v 1.2 2009/09/28 19:54:53 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import threading
import logging
import os
import string
import time

from subprocess import Popen, PIPE

import inspect

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMFactory      import WMFactory
from WMCore.DAOFactory     import DAOFactory

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Job          import Job


from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMCore.FwkJobReport.FJRParser import readJobReport

"""
Usual structure here:

__init__   (starts up)
setup      (Creates DBSBuffer instance for passing completed jobs to)
algorithm  (Actually runs the accountant function)
accountant:
  -polls jobs using getJobs()
  -checks the job states by looking at the FJRs using parseJob()
    - decides which state jobs are in (Success, Failed, or Exhausted)
  -passes job off to appropriate handler
    - closeOutJob(): Job is done, write to DBS and WMBS and go home
      - tarUp(): tar everything
    - exhaustJob():  Job is tired and we just want to put it to sleep
    - failJob(): Job failed.  Pass to Error Handler.
"""

#Note that this is a bit awkward.
#Right now I'm assuming that I might want, in the future, to parse information from exhausted jobs
#Otherwise, we would handle them completely seperately


class JobAccountantPoller(BaseWorkerThread):
    """
    Handles poll-based Job Accounting (after they've run)

    """


    def __init__(self, config, dbsBuffer = None, dbsConfig = None):
        """
        __init__

        """

        myThread = threading.currentThread()
        myThread.dialect = os.getenv('DIALECT')
        BaseWorkerThread.__init__(self)
        self.config = config

        if dbsConfig == None:
            self.dbsConfig = config

            
        self.buffer = dbsBuffer


        
    def setup(self, parameters):
        """
        Load required objects
        """
        myThread = threading.currentThread()

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        #uniqueCouchDbName = 'jsm_test'
        self.changeState = ChangeState(self.config)

        self.buffer = DBSBuffer(self.dbsConfig)
        #print myThread.dbi.processData("SELECT * FROM ms_process")[0].fetchall()
        self.buffer.prepareToStart()

        return
    


    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)





    def algorithm(self, parameters):
        """
        This is the master function -> It runs everything else
        """
        logging.debug("Running subscription / fileset matching algorithm")
        myThread = threading.currentThread()

        try:
            myThread.transaction.begin()
            self.accountant()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise

        return


    def accountant(self):
        """
        _accountant_

        Polls jobs, then does everything else.
        """

        myThread = threading.currentThread()

        #get jobs
        jobList = self.getJobs()

        failedList    = []
        exhaustedList = []
        successList   = []

        #Parse jobs
        for job in jobList:
            value = self.parseJob(job)
            if value == 'Success':
                successList.append(job)
            elif value == 'Exhausted':
                exhaustedList.append(job)
            else:
                failedList.append(job)

        self.changeState.propagate(successList, 'success', 'complete')
        self.changeState.propagate(failedList,  'JobFailed', 'complete')


        for job in successList:
            self.closeOutJob(job)
        for job in failedList:
            self.failJob(job)
        for job in exhaustedList:
            self.exhaustJob(job)


        self.changeState.propagate(successList,    'closeout', 'success')
        self.changeState.propagate(exhaustedList,  'closeout', 'exhausted')

        return


    def getJobs(self):
        """
        _getJobs_

        Get jobs in state 'Complete'
        """

        myThread = threading.currentThread()

        jobList = []
        idList  = []


        getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        idList = getJobs.execute(state = 'complete')

        idList.extend(getJobs.execute(state = 'exhausted'))

        FJRDict = {}
        couchList = self.changeState.getCouchByJobIDAndState(idList, 'complete')
        for entry in couchList:
            job = entry['job']
            FJRDict[job['id']] = job['FJR_Path']

        for id in idList:
            job = Job(id = id)
            job.loadData()
            job['FJR_Path'] = FJRDict[id]
            jobList.append(job)

        return jobList


    def parseJob(self, job):
        """
        _parseJob_

        Parses the FJR and evaluates the state of the job
        """


        value = 'Failed'

        vitalKeys = ['LFN', 'Size', 'TotalEvents', 'Checksum']
        datasetKeys = ["PrimaryDataset", "ProcessedDataset", "DataTier", "ApplicationName", "ApplicationVersion", "ApplicationFamily", "PSetHash"]

        failure = False

        #First, dump any exhausted jobs
        if job.getState().lower() == 'exhausted':
            return 'Exhausted'

        if not 'FJR_Path' in job.keys():
            logging.info("Job %i failed because it has no FJR_Path" %(job['id']))
            return 'Failed'

        if not job['FJR_Path']:
            logging.info("Job %i failed because it has no FJR_Path" %(job['id']))
            return 'Failed'
        
        if not os.path.isfile(job['FJR_Path']):
            logging.info("Job %i failed because the FJR could not be found" %(job['id']))
            return 'Failed'

        #get the jobReport
        jobReport = self.readJobReportInfo(job['FJR_Path'])

        if not jobReport or jobReport == []:
            logging.info("Job %i failed because the FJR could not be parsed at all" %(job['id']))
            return 'Failed'

        FJR  = jobReport[-1]
        #Check if the job failed obviously
        if not FJR.exitCode == 0:
            logging.info("Job %i failed with error code %i" %(job['id'], FJR.exitCode))
            return 'Failed'
        file = FJR.files[0]

        #Somehow get FJR
        #Then assure that we have all necessary keys
        for key in vitalKeys:
            if not key in file.keys():
                failure = True
                logging.error('Could not find value %s in FJR for job %i' %(key, job['id']))

        if hasattr(file, 'dataset'):
            for key in datasetKeys:
                if not key in file.dataset[0].keys():
                    failure = True
                    logging.error('Could not find key %s in FJR.dataset[0] for job %i' %(key, job['id']))

        else:
            failure = True
            logging.error('Job %s did not have dataset attribute' %(job['id']))


        #Now if we have failed
        if failure:
            return 'Failed'
        else:
            #Otherwise
            value = "Success"


        return value


    def closeOutJob(self, job):
        """
        _closeOurJob_

        Handles the result of a successful job
        """

        #insert the Job
        self.buffer.handleMessage('JobSuccess', job['FJR_Path'])

        #tar up jobs
        self.tarUp(job)

        return



    def exhaustJob(self, job):
        """
        _exhaustJob_

        Handles the result of an exhausted job
        """

        return




    def failJob(self, job):
        """
        _failJob_

        Handles the result of a failed job
        """
        
        

        return


    def tarUp(self, job):
        """
        _tarUp_

        Tars and stores the logfiles, etc. for passed job
        """

        if not "cache_dir" in job.keys():
            logging.error("Could not find cache_dir for job %i" %(job['id']))
            return

        if not job["cache_dir"]:
            logging.error("cache_dir of type None for job %i" %(job['id']))
            return

        if not os.path.isdir(job["cache_dir"]):
            logging.error("cache_dir not valid directory for job %i" %(job['id']))
            return

        if os.listdir(job["cache_dir"]) == []:
            logging.info("cache_dir empty for job %i" %(job['id']))
            return

        #Well, if we got here, we should tar everything up.
        tarString = ["tar"]
        tarString.append("-cvf")
        tarString.append('%s/Job_%i.tar ' %(job["cache_dir"], job['id']))
        for file in os.listdir(job["cache_dir"]):
            tarString.append('%s' %(os.path.join(job["cache_dir"], file)))

        #Now we should have all the files together.  Tar them up
        pipe = Popen(tarString, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.wait()

        #This should wait for the result, and then return     
        return
    

    def readJobReportInfo(self, jobReportFile):
        """
        _readJobReportInfo_

        Read the info from jobReport file

        """
        jobReportFile = string.replace(jobReportFile, "file://", "")
        if not os.path.exists(jobReportFile):
            logging.error("JobReport Not Found: %s" %jobReportFile)
            raise InvalidJobReport(jobReportFile)
        try:
            jobreports = readJobReport(jobReportFile)
        except:
            logging.debug("Invalid JobReport File: %s" %jobReportFile)
            raise InvalidJobReport(jobReportFile)

        return jobreports
