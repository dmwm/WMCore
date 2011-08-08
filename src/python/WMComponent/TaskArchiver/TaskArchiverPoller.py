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


Config options
histogramKeys: Allows you to report values in histogram form in the
  workloadSummary - i.e., as a list of bins
histogramBins: Bin size for all histograms
histogramLimit: Limit in terms of number of standard deviations from the
  average at which you cut the histogram off.  All points outside of that
  go into overflow and underflow.
"""
__all__ = []

import os.path
import shutil
import threading
import logging
import traceback
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Subscription   import Subscription
from WMCore.WMBS.Fileset        import Fileset
from WMCore.DAOFactory          import DAOFactory
from WMCore.WorkQueue.WorkQueue import localQueue
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueNoMatchingElements
from WMCore.WMException         import WMException
from WMCore.Database.CMSCouch   import CouchServer
from WMCore.DataStructs.Run     import Run
from WMCore.DataStructs.Mask    import Mask
from WMCore.Algorithms          import MathAlgos
from WMCore.Alerts.ZMQ.Sender   import Sender
from WMCore.Lexicon             import sanitizeURL

from WMComponent.JobCreator.CreateWorkArea   import getMasterName
from WMComponent.JobCreator.JobCreatorPoller import retrieveWMSpec

from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sender import Sender


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
            # Get workqueue setup from config unless overridden
            if hasattr(self.config.TaskArchiver, 'WorkQueueParams'):
                self.workQueue = localQueue(**self.config.TaskArchiver.WorkQueueParams)
            else:
                from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
                self.workQueue = queueFromConfig(self.config)
        else:
            self.workQueue = None

        self.timeout = getattr(self.config.TaskArchiver, "timeOut", 0)

        # Set up optional histograms
        self.histogramKeys  = getattr(self.config.TaskArchiver, "histogramKeys", [])
        self.histogramBins  = getattr(self.config.TaskArchiver, "histogramBins", 10)
        self.histogramLimit = getattr(self.config.TaskArchiver, "histogramLimit", 5.0)

        # Start a couch server for getting job info
        # from the FWJRs for committal to archive
        try:
            self.dbname       = getattr(self.config.JobStateMachine, "couchDBName")
            self.couchdb      = CouchServer(self.config.JobStateMachine.couchurl)
            self.summarydb    = getattr(self.config.TaskArchiver, "summaryDBName", self.dbname)
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
            if getattr(self.config.TaskArchiver, 'requireCouch', False):
                raise TaskArchiverPollerException(msg)
            
        # initialize the alert framework (if available)
        # instance of Alert messages Sender 
        self.sender = None
        # pre-defined values for Alert instances generated from this class
        self.preAlert = None
        self._setUpAlertsMessaging()
        return
    

    def _setUpAlertsMessaging(self):
        """
        Set up Alerts Sender instance, etc.
        Depends on provided configuration (general section 'Alert').
        Should not break anything if such config is not provided.
        
        TODO:
        This kind of method will be similar if not the same for all
            other components sending alerts and shall be factored out into
            an appropriate base class - to discuss.
        
        """
        if hasattr(self.config, "Alert"):
            # pre-defined values for Alert instances generated from this class
            self.preAlert = Alert(Type = "WMAgentComponent",
                                  Workload = "n/a",
                                  Component = "TaskArchiver",
                                  Source = self.__class__.__name__)
            # create sender instance (sending alert messages)
            self.sender = Sender(self.config.Alert.address,
                                 self.__class__.__name__,
                                 self.config.Alert.controlAddr)
            self.sender.register()
    

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
            doneIds = self.notifyWorkQueue(subList)
            # Only kill subscriptions updated in workqueue
            doneSubs = [x for x in subList if x['id'] in doneIds]
            self.killSubscriptions(doneSubs)
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

        if len(subList) < 1:
            return []
        
        subIDs = []
        
        for sub in subList:
            subIDs.append(sub['id'])        
            try:
                self.workQueue.doneWork(SubscriptionId = sub['id'])
                subIDs.append(sub['id'])
            except WorkQueueNoMatchingElements:
                # subscription wasn't known to workqueue, feel free to clean up
                subIDs.append(sub['id'])
            except Exception, ex:
                msg =  "Error talking to workqueue: %s\n" % str(ex)
                msg += "Tried to complete the following: %s\n" % subIDs
                logging.error(msg)
                self._sendAlert(1, msg = msg)

        return []
    
    def _sendAlert(self, level, **args):
        """
        Common method taking care of sending Alert messages.
        It is silent should not the Alert framework be set up.
        Level of the Alert messages is defined by level. 
        
        TODO:
        similarly to _setUpAlertsMessaging, adept for factoring higher.
        
        """
        if self.sender:
            alert = Alert(**self.preAlert)
            alert["Timestamp"] = time.time()
            alert["Level"] = level
            alert["Details"] = args
            self.sender(alert)
            

    def killSubscriptions(self, doneList):
        """
        _killSubscriptions_
        
        Actually dump the subscriptions
        """

        for sub in doneList:
            logging.info("Deleting subscription %i" % sub['id'])
            try:
                sub.load()
                sub['workflow'].load()
                wf = sub['workflow']
                if self.workQueue != None and not \
                       self.workQueue.getWMBSInjectStatus(wf.name):
                        # Then there are still files to be put in.
                        continue
                sub.deleteEverything()
                workflow = sub['workflow']

                if not workflow.exists():
                    # Then we deleted the workflow

                    # First load the WMSpec
                    logging.debug("Loading spec to delete sandbox dir")
                    spec     = retrieveWMSpec(workflow = workflow)
                    wmTask   = spec.getTaskByPath(workflow.task)
                                        
                    # Then pull its info from couch and archive it
                    self.archiveCouchSummary(workflow = workflow, spec = spec)
                    # Now we have to delete the task area.
                    workDir, taskDir = getMasterName(startDir = self.jobCacheDir,
                                                     workflow = workflow)
                    logging.info("About to delete work directory %s" % taskDir)
                    if os.path.isdir(taskDir):
                        # Remove the taskDir, because we're done
                        shutil.rmtree(taskDir)
                    else:
                        msg = "Attempted to delete work directory but it was already gone: %s" % taskDir
                        logging.error(msg)
                        self._sendAlert(1, msg = msg)
                    # Remove the sandbox dir
                    if not workflow.countWorkflowsBySpec() == 0:
                        continue
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
                self._sendAlert(2, msg = msg)
                # Matt's patch had this following raising commented out too ...
                #raise TaskArchiverPollerException(msg)

        return


    def archiveCouchSummary(self, workflow, spec):
        """
        _archiveCouchSummary_

        For each workflow pull its information from couch and turn it into
        a summary for archiving
        """

        failedJobs = []
        jobErrors  = []
        outputLFNs = []

        workflowData = {}
        workflowName     = workflow.task.split('/')[1]

        # Set campaign
        workflowData['campaign'] = spec.getCampaign()

        # Get a list of failed job IDs
        failedCouch = self.jobsdatabase.loadView("JobDump", "failedJobsByWorkflowName",
                                                 options = {"startkey": [workflowName, workflow.task],
                                                            "endkey": [workflowName, workflow.task]})['rows']

        output = self.fwjrdatabase.loadView("FWJRDump", "outputByWorkflowName",
                                            options = {"group_level": 2,
                                                       "startkey": [workflowName],
                                                       "endkey": [workflowName, {}],
                                                       "group": True,
                                                       "group_level": 1})['rows']

        perf = self.handleCouchPerformance(workflowName = workflowName)
        workflowData['performance'] = {}
        for key in perf:
            workflowData['performance'][key] = {}
            for attr in perf[key].keys():
                workflowData['performance'][key][attr] = perf[key][attr]

        for entry in failedCouch:
            failedJobs.append(entry['value'])


        workflowData["_id"]          = workflow.task.split('/')[1]
        try:
            workflowData["ACDCServer"]   = sanitizeURL(self.config.ACDC.couchurl)['url']
            workflowData["ACDCDatabase"] = self.config.ACDC.database
        except AttributeError, ex:
            # We're missing the ACDC info.
            # Keep going
            logging.error("ACDC info missing from config.  Skipping this step in the workflow summary.")
            logging.debug("Error: %s" % str(ex))
    
            

        # Attach output
        workflowData['output'] = {}
        for e in output:
            entry   = e['value']
            dataset = entry['dataset']
            workflowData['output'][dataset] = {}
            workflowData['output'][dataset]['nFiles'] = entry['count']
            workflowData['output'][dataset]['size']   = entry['size']
            workflowData['output'][dataset]['events'] = entry['events']


        # Loop over all failed jobs
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
                logs   = err['value']['logs']
                start  = err['value']['start']
                stop   = err['value']['stop']
                if not task in workflowData.keys():
                    workflowData[task] = {'failureTime': 0}
                if not step in workflowData[task].keys():
                    workflowData[task][step] = {}
                workflowData[task]['failureTime'] += (stop - start)
                stepFailures = workflowData[task][step]
                for error in errors:
                    exitCode = str(error['exitCode'])
                    if not exitCode in stepFailures.keys():
                        stepFailures[exitCode] = {"errors": [],
                                                  "jobs":   0,
                                                  "input":  [],
                                                  "runs":   {},
                                                  "logs":   []}
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
                    for log in logs:
                        if not log in stepFailures[exitCode]["logs"]:
                            stepFailures[exitCode]["logs"].append(log)

        # Now we have the workflowData in the right format
        # Time to send them on
        logging.debug("About to commit workflow summary to couch")
        try:
            self.workdatabase.commitOne(workflowData)
        except Exception, ex:
            msg = "Error while attempting to commit to couch: %s" % str(ex)
            self._sendAlert(3, msg = msg)
            raise
        logging.debug("Finished committing workflow summary to couch")

        return


    def handleCouchPerformance(self, workflowName):
        """
        _handleCouchPerformance_

        The couch performance stuff is convoluted enough I think I want to handle it separately.
        """
        output = {'jobTime': []}
        final  = {}
        
        perf = self.fwjrdatabase.loadView("FWJRDump", "performanceByWorkflowName",
                                          options = {"startkey": [workflowName],
                                                     "endkey": [workflowName]})['rows']

        for row in perf:
            for key in row['value'].keys():
                if key in ['startTime', 'stopTime']:
                    continue
                if not key in output.keys():
                    output[key] = []
                output[key].append(float(row['value'][key]))
            try:
                jobTime = row['value'].get('stopTime', None) - row['value'].get('startTime', None)
                output['jobTime'].append(jobTime)
            except TypeError:
                # One of those didn't have a real value
                pass

        for key in output.keys():
            final[key] = {}
            if key in self.histogramKeys:
                histogram = MathAlgos.createHistogram(numList = output[key],
                                                      nBins = self.histogramBins,
                                                      limit = self.histogramLimit)
                final[key]['histogram'] = histogram
            else:
                average, stdDev = MathAlgos.getAverageStdDev(numList = output[key])
                final[key]['average'] = average
                final[key]['stdDev']  = stdDev
            
        return final


        
