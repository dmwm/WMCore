#!/usr/bin/env python
#pylint: disable-msg=W6501, W0142
# W6501: pass information to logging using string arguments
# W0142: Some people like ** magic
"""
The actual taskArchiver algorithm

Procedure:
a) Takes as input all finished subscriptions
      This is defined by the Subscriptions.GetFinishedSubscriptions DAO
b) Calls the WMBS.Subscription.DeleteEverything() method on them.

This should be a simple process.  Because of the long time between
the submission of subscriptions projected and the short time to run
this class, it should be run irregularly.
"""
__all__ = []

import os.path
import shutil
import threading
import logging
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Subscription   import Subscription
from WMCore.WMBS.Fileset        import Fileset
from WMCore.DAOFactory          import DAOFactory
from WMCore.WorkQueue.WorkQueue import localQueue
from WMCore.WMException         import WMException
from WMCore.Database.CMSCouch   import CouchServer
from WMCore.DataStructs.Run     import Run
from WMCore.DataStructs.Mask    import Mask

from WMComponent.JobCreator.CreateWorkArea   import getMasterName
from WMComponent.JobCreator.JobCreatorPoller import retrieveWMSpec


class TaskArchiverPollerException(WMException):
    """
    _TaskArchiverPollerException_

    This is the class that serves as the customized
    Exception class for the TaskArchiverPoller

    As if you couldn't tell that already
    """

class TaskArchiverPoller(BaseWorkerThread):
    """
    Polls for Ended jobs

    List of attributes
    
    requireCouch:  raise an exception on couch failure instead of ignoring
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        self.config      = config
        self.jobCacheDir = self.config.JobCreator.jobCacheDir
        
        if getattr(self.config.TaskArchiver, "useWorkQueue", False) != False:
            wqp = self.config.TaskArchiver.WorkQueueParams
            self.workQueue = localQueue(**wqp)
        else:
            self.workQueue = None

        self.timeout = getattr(self.config.TaskArchiver, "timeOut", 0)

        # Start a couch server for getting job info
        # from the FWJRs for committal to archive
        try:
            self.dbname       = getattr(self.config.JobStateMachine, "couchDBName")
            self.couchdb      = CouchServer(self.config.JobStateMachine.couchurl)
            self.jobsdatabase = self.couchdb.connectDatabase("%s/jobs" % self.dbname)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.dbname)
            self.workdatabase = self.couchdb.connectDatabase(self.dbname)
            logging.debug("Using url %s" % self.config.JobStateMachine.couchurl)
            logging.debug("Writing to %s" % self.dbname)
            self.requireCouch = getattr(self.config.TaskArchiver, 'requireCouch', False)
        except Exception, ex:
            msg =  "Error in connecting to couch.\n"
            msg += str(ex)
            logging.error(msg)
            self.jobsdatabase = None
            self.fwjrdatabase = None
            if self.requireCouch:
                raise TaskArchiverPollerException(msg)
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
        logging.debug("Running algorithm for finding finished subscriptions")
        try:
            self.archiveTasks()
        except WMException:
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise
        except Exception, ex:
            myThread = threading.currentThread()
            msg = "Caught exception in TaskArchiver\n"
            msg += str(ex)
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise TaskArchiverPollerException(msg)

        return


    def archiveTasks(self):
        """
        _archiveTasks_
        
        archiveTasks will handle the master task of looking for finished subscriptions,
        checking to see if they've finished, and then notifying the workQueue and
        finishing things up.
        """


        subList = self.findFinishedSubscriptions()
        if len(subList) == 0:
            return

        if self.workQueue != None:
            doneList = self.notifyWorkQueue(subList)
            self.killSubscriptions(doneList)
        else:
            self.killSubscriptions(subList)
            
        return

    def findFinishedSubscriptions(self):
        """
        _findFinishedSubscriptions_
        
        Figures out which one of the subscriptions is actually finished.
        """
        subList = []

        myThread = threading.currentThread()

        myThread.transaction.begin()

        subscriptionList = self.daoFactory(classname = "Subscriptions.GetFinishedSubscriptions")
        subscriptions    = subscriptionList.execute(timeOut = self.timeout)

        for subscription in subscriptions:
            wmbsSubscription = Subscription(id = subscription['id'])
            subList.append(wmbsSubscription)
            logging.info("Found subscription %i" %subscription['id'])

        myThread.transaction.commit()

        return subList


    def notifyWorkQueue(self, subList):
        """
        _notifyWorkQueue_
        
        Tells the workQueue component that a particular subscription,
        or set of subscriptions, is done.  Receives confirmation
        """
        subIDs = []
        
        for sub in subList:
            subIDs.append(sub['id'])        
        
        try:
            self.workQueue.doneWork(subIDs, id_type = "subscription_id")
            return subList
        except Exception, ex:
            logging.error("Error talking to workqueue: %s" % str(ex))
            logging.error("Tried to complete the following: %s" % subIDs)

        return []

    def killSubscriptions(self, doneList):
        """
        _killSubscriptions_
        
        Actually dump the subscriptions
        """

        for sub in doneList:
            logging.info("Deleting subscription %i" % sub['id'])
            try:
                sub.deleteEverything()
                workflow = sub['workflow']
                if not workflow.exists():
                    # Then we deleted the workflow
                    # First pull its info from couch and archive it
                    self.archiveCouchSummary(workflow = workflow)
                    # Now we have to delete the task area.
                    workDir, taskDir = getMasterName(startDir = self.jobCacheDir,
                                                     workflow = workflow)
                    logging.info("About to delete work directory %s" % taskDir)
                    if os.path.isdir(taskDir):
                        # Remove the taskDir, because we're done
                        shutil.rmtree(taskDir)
                    else:
                        logging.error("Attempted to delete work directory but it was already gone: %s" % taskDir)
                    # Remove the sandbox dir
                    logging.debug("Loading spec to delete sandbox dir")
                    spec     = retrieveWMSpec(workflow = workflow)
                    wmTask   = spec.getTaskByPath(workflow.task)
                    sandbox  = getattr(wmTask.data.input, 'sandbox', None)
                    if sandbox:
                        sandboxDir = os.path.dirname(sandbox)
                        if os.path.isdir(sandboxDir):
                            shutil.rmtree(sandboxDir)
                            logging.debug("Sandbox dir deleted")
                        else:
                            logging.error("Attempted to delete sandbox dir but it was already gone: %s" % sandboxDir)
            except Exception, ex:
                msg =  "Critical error while deleting subscription %i\n" % sub['id']
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                raise TaskArchiverPollerException(msg)

        return


    def archiveCouchSummary(self, workflow):
        """
        _archiveCouchSummary_

        For each workflow pull its information from couch and turn it into
        a summary for archiving
        """

        failedJobs = []
        jobErrors  = []
        outputLFNs = []

        workflowFailures = {}

        # Get a list of failed job IDs
        failedCouch = self.jobsdatabase.loadView("JobDump", "failedJobsByWorkflowName",
                                                 options = {"startkey": [workflow.task.split('/')[1], workflow.task],
                                                            "endkey": [workflow.task.split('/')[1], workflow.task]})['rows']

        output = self.fwjrdatabase.loadView("FWJRDump", "outputByWorkflowName",
                                            options = {"group_level": 2,
                                                       "startkey": [workflow.task.split('/')[1]],
                                                       "endkey": [workflow.task.split('/')[1], {}]})['rows']
        
        for entry in failedCouch:
            failedJobs.append(entry['value'])


        workflowFailures["_id"] = workflow.task.split('/')[1]

        # Attach output
        workflowFailures['output'] = {}
        for e in output:
            entry   = e['value']
            dataset = entry['dataset']
            workflowFailures['output'][dataset] = {}
            workflowFailures['output'][dataset]['nFiles'] = entry['count']
            workflowFailures['output'][dataset]['size']   = entry['size']
            workflowFailures['output'][dataset]['events'] = entry['events']
            
        
        for jobid in failedJobs:
            errorCouch = self.fwjrdatabase.loadView("FWJRDump", "errorsByJobID",
                                                    options = {"startkey": [jobid, 0],
                                                               "endkey": [jobid, {}]})['rows']
            
            job    = self.jobsdatabase.document(id = str(jobid))
            inputs = [x['lfn'] for x in job['inputfiles']]
            runsA  = [x['runs'][0] for x in job['inputfiles']]
            maskA  = job['mask']
            # Have to transform this because JSON is too stupid to understand ints
            for key in maskA['runAndLumis'].keys():
                maskA['runAndLumis'][int(key)] = maskA['runAndLumis'][key]
                del maskA['runAndLumis'][key]
            mask   = Mask()
            mask.update(maskA)
            runs   = []
            # Turn arbitrary format into real runs
            for r in runsA:
                run = Run(runNumber = r['run_number'])
                run.lumis = r.get('lumis', [])
                runs.append(run)
            # Get rid of runs that aren't in the mask
            runs = mask.filterRunLumisByMask(runs = runs)
            for err in errorCouch:
                task   = err['value']['task']
                step   = err['value']['step']
                errors = err['value']['error']
                if not task in workflowFailures.keys():
                    workflowFailures[task] = {}
                if not step in workflowFailures[task].keys():
                    workflowFailures[task][step] = {}
                stepFailures = workflowFailures[task][step]
                for error in errors:
                    exitCode = str(error['exitCode'])
                    if not exitCode in stepFailures.keys():
                        stepFailures[exitCode] = {"errors": [],
                                                  "jobs":   0,
                                                  "input":  [],
                                                  "runs":   {}}
                    if jobid in stepFailures[exitCode]['jobs']:
                        # We're repeating this error, and I don't know why
                        continue
                    stepFailures[exitCode]['jobs'] += 1 # Increment job counter
                    if len(stepFailures[exitCode]['errors']) == 0 or \
                           exitCode == '99999':
                        # Only record the first error for an exit code
                        # unless exit code is 99999 (general panic)
                        stepFailures[exitCode]['errors'].append(error)
                    # Add input LFNs to structure
                    for input in inputs:
                        if not input in stepFailures[exitCode]['input']:
                            stepFailures[exitCode]['input'].append(input)
                    # Add runs to structure
                    for run in runs:
                        if not str(run.run) in stepFailures[exitCode]['runs'].keys():
                            stepFailures[exitCode]['runs'][str(run.run)] = []
                        for l in run.lumis:
                            if not l in stepFailures[exitCode]['runs'][str(run.run)]:
                                stepFailures[exitCode]['runs'][str(run.run)].append(l)
                                                         
        # Now we have the workflowFailures in the right format
        # Time to send them on
        logging.debug("About to commit workflow summary to couch")
        self.workdatabase.commitOne(workflowFailures)
        logging.debug("Finished committing workflow summary to couch")
        return
