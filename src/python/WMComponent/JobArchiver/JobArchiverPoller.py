#!/usr/bin/env python
#pylint: disable-msg=W6501
# W6501: pass information to logging using string arguments
"""
The actual jobArchiver algorithm
"""
__all__ = []



import threading
import json
import logging
import os
import os.path
import shutil
import tarfile
import traceback
import time

from WMComponent.TaskArchiver.TaskArchiverPoller import uploadPublishWorkflow
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.JobStateMachine.ChangeState    import ChangeState

from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueNoMatchingElements

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMException       import WMException
from WMCore.WMBS.Workflow     import Workflow


class JobArchiverPollerException(WMException):
    """
    _JobArchiverPollerException_

    The Exception handler for the job archiver.
    """

class JobArchiverPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config      = config
        self.changeState = ChangeState(self.config)

        myThread        = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.loadAction = self.daoFactory(classname = "Jobs.LoadFromIDWithWorkflow")


        # Variables
        self.numberOfJobsToCluster = getattr(self.config.JobArchiver,
                                             "numberOfJobsToCluster", 1000)

        # initialize the alert framework (if available)
        self.initAlerts(compName = "JobArchiver")

        try:
            self.uploadPublishDir = getattr(self.config.JobArchiver, 'uploadPublishDir',
                                  os.path.join(config.JobArchiver.componentDir, 'publishDir'))
            self.logDir = getattr(config.JobArchiver, 'logDir',
                                  os.path.join(config.JobArchiver.componentDir, 'logDir'))
            if not os.path.isdir(self.logDir):
                os.makedirs(self.logDir)
            if not os.path.isdir(self.uploadPublishDir):
                os.makedirs(self.uploadPublishDir)
        except Exception, ex:
            msg =  "Unhandled exception while setting up logDir and/or uploadPublishDir!\n"
            msg += str(ex)
            logging.error(msg)
            self.sendAlert(6, msg = msg)
            try:
                logging.debug("Directory: %s" % self.logDir)
                logging.debug("Config: %s" % config)
            except:
                pass
            raise JobArchiverPollerException(msg)

        try:
            from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
            self.workQueue = queueFromConfig(self.config)
        except Exception, ex:
            msg =  "Could not load workQueue"
            msg += str(ex)
            logging.error(msg)
            #raise JobArchiverPollerException(msg)

        self.uploadPublishInfo = getattr(self.config.JobArchiver, 'uploadPublishInfo', False)
        self.userFileCacheURL = getattr(self.config.JobArchiver, 'userFileCacheURL', None)
        self.handleWorkflowInjection = getattr(self.config.JobArchiver,
                                               'handleInjected', True)

        return



    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        return


    def terminate(self, params):
        """
        _terminate_

        This function terminates the job after a final pass
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return


    def algorithm(self, parameters = None):
        """
        Performs the archiveJobs method, looking for each type of failure
        And deal with it as desired.
        """
        try:
            self.archiveJobs()
            self.pollForClosable()
            self.markInjected()
        except WMException:
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', None) != None\
                   and getattr(myThread.transaction, 'transaction', None) != None:
                myThread.transaction.rollback()
            raise
        except Exception, ex:
            myThread = threading.currentThread()
            msg = "Caught exception in JobArchiver\n"
            msg += str(ex)
            msg += "\n\n"
            if getattr(myThread, 'transaction', None) != None\
                   and getattr(myThread.transaction, 'transaction', None) != None:
                myThread.transaction.rollback()
            raise JobArchiverPollerException(msg)


        return


    def archiveJobs(self):
        """
        _archiveJobs_

        archiveJobs will handle the master task of looking for finished jobs,
        and running the code that cleans them out.
        """
        myThread = threading.currentThread()

        doneList  = self.findFinishedJobs()
        logging.info("Found %i finished jobs to archive" % len(doneList))

        self.cleanWorkArea(doneList)

        successList = []
        failList    = []
        killList    = []

        for job in doneList:
            if job["outcome"] == "success":
                successList.append(job)
            elif job["outcome"] == "killed":
                killList.append(job)
            else:
                failList.append(job)

        myThread.transaction.begin()

        if self.uploadPublishInfo:
            self.createAndUploadPublish(successList)

        self.changeState.propagate(successList, "cleanout", "success")
        self.changeState.propagate(failList, "cleanout", "exhausted")
        self.changeState.propagate(killList, "cleanout", "killed")
        myThread.transaction.commit()


    def findFinishedJobs(self):
        """
        _findFinishedJobs_

        Will actually, surprisingly, find finished jobs (i.e., jobs either exhausted or successful)
        """
        jobList = []

        jobListAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList1 = jobListAction.execute(state = "success")
        jobList2 = jobListAction.execute(state = "exhausted")
        jobList3 = jobListAction.execute(state = "killed")

        jobList.extend(jobList1)
        jobList.extend(jobList2)
        jobList.extend(jobList3)

        if len(jobList) == 0:
            # Then nothing is ready
            return []

        # Put together a list of job IDs
        binds = []
        for jobID in jobList:
            binds.append({"jobid": jobID})


        results = self.loadAction.execute(jobID = binds)

        if not type(results) == list:
            results = [results]

        doneList = []

        for entry in results:
            # One job per entry
            tmpJob = Job(id = entry['id'])
            tmpJob.update(entry)
            doneList.append(tmpJob)


        return doneList


    def cleanWorkArea(self, doneList):
        """
        _cleanWorkArea_

        Upon workQueue realizing that a subscriptions is done, everything
        regarding those jobs is cleaned up.
        """

        for job in doneList:
            #print "About to clean cache for job %i" % (job['id'])
            self.cleanJobCache(job)

        return

    def cleanJobCache(self, job):
        """
        _cleanJobCache_

        Clears out any files still sticking around in the jobCache,
        tars up the contents and sends them off
        """

        cacheDir = job['cache_dir']

        if not cacheDir or not os.path.isdir(cacheDir):
            msg = "Could not find jobCacheDir %s" % (cacheDir)
            logging.error(msg)
            self.sendAlert(1, msg = msg)
            return

        cacheDirList = os.listdir(cacheDir)

        if cacheDirList == []:
            os.rmdir(cacheDir)
            return

        # Now we need to set up a final destination
        try:
            # Label all directories by workflow
            # Workflow better have a first character
            workflow       = job['workflow']
            firstCharacter = workflow[0]
            jobFolder = 'JobCluster_%i' \
                        % (int(job['id']/self.numberOfJobsToCluster))
            logDir =  os.path.join(self.logDir, firstCharacter,
                                   workflow, jobFolder)
            if not os.path.exists(logDir):
                os.makedirs(logDir)
        except Exception, ex:
            msg =  "Exception while trying to make output logDir\n"
            msg += str("logDir: %s\n" % (logDir))
            msg += str(ex)
            logging.error(msg)
            self.sendAlert(6, msg = msg)
            raise JobArchiverPollerException(msg)

        # Otherwise we have something in there
        try:
            tarName = 'Job_%i.tar.bz2' % (job['id'])
            tarball = tarfile.open(name = os.path.join(logDir, tarName),
                                   mode = 'w:bz2')
            for fileName in cacheDirList:
                fullFile = os.path.join(cacheDir, fileName)
                try:
                    tarball.add(name = fullFile,
                                arcname = 'Job_%i/%s' %(job['id'], fileName))
                except IOError:
                    logging.error('Cannot read %s, skipping' % fullFile)
            tarball.close()
        except Exception, ex:
            msg =  "Exception while opening and adding to a tarfile\n"
            msg += "Tarfile: %s\n" % os.path.join(logDir, tarName)
            msg += str(ex)
            logging.error(msg)
            self.sendAlert(6, msg = msg)
            logging.debug("cacheDirList: %s" % (cacheDirList))
            raise JobArchiverPollerException(msg)

        try:
            shutil.rmtree('%s' % (cacheDir), ignore_errors=True)
        except Exception, ex:
            msg =  "Error while removing the old cache dir.\n"
            msg += "CacheDir: %s\n" % cacheDir
            msg += str(ex)
            logging.error(msg)
            self.sendAlert(6, msg = msg)
            raise JobArchiverPollerException(msg)

        return


    def markInjected(self):
        """
        _markInjected_

        Mark any workflows that have been fully injected as injected
        """

        if not self.handleWorkflowInjection:
            logging.debug("Component will not check workflows for injection status")
            return

        myThread = threading.currentThread()
        getAction  = self.daoFactory(classname = "Workflow.GetInjectedWorkflows")
        markAction = self.daoFactory(classname = "Workflow.MarkInjectedWorkflows")
        result = getAction.execute()

        # Check each result to see if it is injected:
        injected = []
        for name in result:
            try:
                if self.workQueue.getWMBSInjectionStatus(workflowName = name):
                    injected.append(name)
            except WorkQueueNoMatchingElements:
                # workflow not known - free to cleanup
                injected.append(name)
            except Exception, ex:
                logging.error("Injection status checking failed, investigate: %s" % str(ex))
                logging.debug("Traceback: %s" % traceback.format_exc())

        # Now, mark as injected those that returned True
        if len(injected) > 0:
            myThread.transaction.begin()
            markAction.execute(names = injected, injected = True)
            myThread.transaction.commit()
        return

    def pollForClosable(self):
        """
        _pollForClosable_

        Search WMBS for filesets that can be closed and mark them as closed.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        closableFilesetDAO = self.daoFactory(classname = "Fileset.ListClosable")
        closableFilesets = closableFilesetDAO.execute()

        for closableFileset in closableFilesets:
            openFileset = Fileset(id = closableFileset)
            openFileset.load()

            logging.debug("Closing fileset %s" % openFileset.name)
            openFileset.markOpen(False)

        myThread.transaction.commit()

    def createAndUploadPublish(self, jobList):
        taskList = {}
        for job in jobList:
            taskList[(job['workflow'], job['task'])] = job['cache_dir']

        for (workflow, task) in taskList.keys():
            wf = Workflow(name=workflow, task=task)
            wf.load()
            try:
                uploadPublishWorkflow(self.config, wf, ufcEndpoint=self.userFileCacheURL, workDir=self.uploadPublishDir)
            except Exception, ex:
                logging.error('Upload failed for workflow, task: %s, %s' % (workflow, task))
                logging.error(str(ex))

        return
