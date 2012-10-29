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

import json
import logging
import math
import os.path
import pickle
import shutil
import tarfile
import threading
import traceback
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache

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
from WMCore.Lexicon             import sanitizeURL
from WMCore.WMSpec.WMWorkload   import WMWorkloadHelper

from WMComponent.JobCreator.CreateWorkArea   import getMasterName
from WMComponent.JobCreator.JobCreatorPoller import retrieveWMSpec
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter

class TaskArchiverPollerException(WMException):
    """
    _TaskArchiverPollerException_

    This is the class that serves as the customized
    Exception class for the TaskArchiverPoller

    As if you couldn't tell that already
    """



class FileEncoder(json.JSONEncoder):
    """
    JSON encoder to transform sets to lists and handle any object with a __to_json__ method
    """

    def default(self, obj):
        """
        Redefine JSONEncoder default method
        """
        if hasattr(obj, '__to_json__'):
            return obj.__to_json__()
        elif isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def uploadPublishWorkflow(workflow, ufcEndpoint, workDir):
    """
    Write out and upload to the UFC a JSON file
    with all the info needed to publish this dataset later
    """

    ufc = UserFileCache({'endpoint': ufcEndpoint})

    # Skip tasks ending in LogCollect, they have nothing interesting.
    taskNameParts = workflow.task.split('/')
    if taskNameParts.pop() in ['LogCollect']:
        logging.info('Skipping LogCollect task')
        return False
    logging.info('Generating JSON for publication of %s of type %s' % (workflow.name, workflow.wfType))

    myThread = threading.currentThread()

    dbsDaoFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                logger = myThread.logger, dbinterface = myThread.dbi)
    findFiles = dbsDaoFactory(classname = "LoadFilesByWorkflow")

    # Fetch and filter the files to the ones we actually need
    uploadDatasets = {}
    uploadFiles = findFiles.execute(workflowName = workflow.name)
    for file in uploadFiles:
        datasetName = file['datasetPath']
        if not uploadDatasets.has_key(datasetName):
            uploadDatasets[datasetName] = []
        uploadDatasets[datasetName].append(file)

    if not uploadDatasets:
        logging.info('No datasets found to upload.')
        return False

    # Write JSON file and then create tarball with it
    baseName = '%s_publish.tgz'  % workflow.name
    jsonName = os.path.join(workDir, '%s_publish.json' % workflow.name)
    tgzName = os.path.join(workDir, baseName)
    with open(jsonName, 'w') as jsonFile:
        json.dump(uploadDatasets, fp=jsonFile, cls=FileEncoder, indent=2)

    # Only in 2.7 does tarfile become usable as context manager
    tgzFile = tarfile.open(name=tgzName, mode='w:gz')
    tgzFile.add(jsonName)
    tgzFile.close()

    result = ufc.upload(fileName=tgzName, name=baseName)
    logging.debug('Upload result %s' % result)
    # If this doesn't work, exception will propogate up and block archiving the task
    logging.info('Uploaded with name %s and hashkey %s' % (result['name'], result['hashkey']))
    return




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

        self.timeout           = getattr(self.config.TaskArchiver, "timeOut", 0)
        self.nOffenders        = getattr(self.config.TaskArchiver, 'nOffenders', 3)
        self.deleteCouchData   = getattr(self.config.TaskArchiver, 'deleteCouchData', True)
        self.uploadPublishInfo = getattr(self.config.TaskArchiver, 'uploadPublishInfo', False)
        self.uploadPublishDir  = getattr(self.config.TaskArchiver, 'uploadPublishDir', None)
        self.userFileCacheURL  = getattr(self.config.TaskArchiver, 'userFileCacheURL', None)

        # Set up optional histograms
        self.histogramKeys  = getattr(self.config.TaskArchiver, "histogramKeys", [])
        self.histogramBins  = getattr(self.config.TaskArchiver, "histogramBins", 10)
        self.histogramLimit = getattr(self.config.TaskArchiver, "histogramLimit", 5.0)
        self.centralCouchDBWriter = WMStatsWriter(self.config.TaskArchiver.centralWMStatsURL);
        
        # Start a couch server for getting job info
        # from the FWJRs for committal to archive
        try:
            workDBName       = getattr(self.config.TaskArchiver, 'workloadSummaryCouchDBName',
                                        'workloadsummary')
            workDBurl        = getattr(self.config.TaskArchiver, 'workloadSummaryCouchURL')
            jobDBurl         = sanitizeURL(self.config.JobStateMachine.couchurl)['url']
            jobDBName        = self.config.JobStateMachine.couchDBName
            self.jobCouchdb  = CouchServer(jobDBurl)
            self.workCouchdb = CouchServer(workDBurl)

            self.jobsdatabase = self.jobCouchdb.connectDatabase("%s/jobs" % jobDBName)
            self.fwjrdatabase = self.jobCouchdb.connectDatabase("%s/fwjrs" % jobDBName)
            self.workdatabase = self.workCouchdb.connectDatabase(workDBName)

            logging.debug("Using url %s/%s for job" % (jobDBurl, jobDBName))
            logging.debug("Writing to  %s/%s for workloadSummary" % (sanitizeURL(workDBurl)['url'], workDBName))
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
        self.initAlerts(compName = "TaskArchiver")
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
        logging.info("Found %i finished subscriptions" % len(subList))
        if len(subList) == 0:
            return

        if self.workQueue != None:
            doneIDs  = self.notifyWorkQueue(subList)
            doneSubs = [x for x in subList if x['id'] in doneIDs]
            # Only kill subscriptions updated in workqueue
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
                self.sendAlert(1, msg = msg)

        return subIDs


    def killSubscriptions(self, doneList):
        """
        _killSubscriptions_

        Actually dump the subscriptions
        """
        abortedWorkflows = self.centralCouchDBWriter.workflowsByStatus(["aborted"], format = "dict");
        for sub in doneList:
            logging.info("Deleting subscription %i" % sub['id'])
            try:
                sub.load()
                sub['workflow'].load()
                wf = sub['workflow']
                if self.uploadPublishInfo:
                    self.createAndUploadPublish(wf)
                sub.deleteEverything()
                workflow = sub['workflow']

                if workflow.exists():
                    # Then there are other subscriptions attached
                    # to the workflow
                    continue

                # If we deleted the workflow, it's time to delete
                # the work directories

                # Now we have to delete the task area.
                workDir, taskDir = getMasterName(startDir = self.jobCacheDir,
                                                 workflow = workflow)
                logging.info("About to delete work directory %s" % taskDir)
                if os.path.exists(taskDir):
                    if os.path.isdir(taskDir):
                        shutil.rmtree(taskDir)
                    else:
                        # What we think of as a working directory is not a directory
                        # This should never happen and there is no way we can recover
                        # from this here. Bail out now and have someone look at things. 
                        msg = "Work directory is not a directory, this should never happen: %s" % taskDir
                        raise TaskArchiverPollerException(msg)
                else:
                    msg = "Attempted to delete work directory but it was already gone: %s" % taskDir
                    logging.debug(msg)

                # Now check if the workflow is done
                if not workflow.countWorkflowsBySpec() == 0:
                    continue

                # If the WMSpec is done, then we have to delete
                # the sandbox, and send off the couch summary

                # First load the WMSpec
                try:
                    logging.debug("Loading spec to delete sandbox dir for task %s" % workflow.task)
                    spec     = retrieveWMSpec(workflow = workflow)
                    wmTask   = spec.getTaskByPath(workflow.task)
                except Exception, ex:
                    # If this happens, we're well and truly screwed.
                    # We've passed the deletion point.  We can't recover
                    # Abort this.  There will be no couch summary
                    msg =  "Critical error in opening spec after workflow deletion"
                    msg += "Task: %s" % workflow.task
                    msg += str(ex)
                    msg += "There will be NO workflow summary for this task"
                    raise TaskArchiverPollerException(msg)

                # Then pull its info from couch and archive it
                self.archiveCouchSummary(workflow = workflow, spec = spec)
                workflowName = workflow.task.split('/')[1]
                self.deleteWorkflowFromCouch(workflowName = workflowName)
                if workflowName in abortedWorkflows:
                    self.centralCouchDBWriter.updateRequestStatus(workflowName, "aborted-completed")
                    logging.info("status updated to aborted-completed %s" % workflowName)

                # Now take care of the sandbox
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
                self.sendAlert(2, msg = msg)
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

        workflowData = {'retryData': {}}
        workflowName     = workflow.task.split('/')[1]

        # Set campaign
        workflowData['campaign'] = spec.getCampaign()

        # Get a list of failed job IDs
        # Make sure you get it for ALL tasks in the spec
        for taskName in spec.listAllTaskPathNames():
            failedTmp = self.jobsdatabase.loadView("JobDump", "failedJobsByWorkflowName",
                                                   options = {"startkey": [workflowName, taskName],
                                                              "endkey": [workflowName, taskName]})['rows']
            for entry in failedTmp:
                failedJobs.append(entry['value'])

        retryData = self.jobsdatabase.loadView("JobDump", "retriesByTask",
                                               options = {'group_level': 3,
                                                          'startkey': [workflowName],
                                                          'endkey': [workflowName, {}]})['rows']
        for row in retryData:
            taskName = row['key'][2]
            count    = str(row['key'][1])
            if not taskName in workflowData['retryData'].keys():
                workflowData['retryData'][taskName] = {}
            workflowData['retryData'][taskName][count] = row['value']

        output = self.fwjrdatabase.loadView("FWJRDump", "outputByWorkflowName",
                                            options = {"group_level": 2,
                                                       "startkey": [workflowName],
                                                       "endkey": [workflowName, {}],
                                                       "group": True})['rows']

        perf = self.handleCouchPerformance(workflowName = workflowName)
        workflowData['performance'] = {}
        for key in perf:
            workflowData['performance'][key] = {}
            for attr in perf[key].keys():
                workflowData['performance'][key][attr] = perf[key][attr]


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
        workflowData['errors'] = {}
        for jobid in failedJobs:
            errorCouch = self.fwjrdatabase.loadView("FWJRDump", "errorsByJobID",
                                                    options = {"startkey": [jobid, 0],
                                                               "endkey": [jobid, {}]})['rows']
            job    = self.jobsdatabase.document(id = str(jobid))
            inputs = [x['lfn'] for x in job['inputfiles']]
            runsA  = []
            for x in job['inputfiles']:
                try:
                    runsA.append(x['runs'][0])
                except IndexError:
                    # No runs in this input file
                    # Ignore it
                    pass
            maskA  = job['mask']

            # Have to transform this because JSON is too stupid to understand ints
            # Also for some reason we're getting a strange problem where the mask
            # isn't being loaded at all.  I'm not sure what to do there except drop it.
            try:
                for key in maskA['runAndLumis'].keys():
                    maskA['runAndLumis'][int(key)] = maskA['runAndLumis'][key]
                    del maskA['runAndLumis'][key]
            except KeyError:
                # We don't have a mask.  Not much we can do about this
                maskA = Mask()
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
                if not task in workflowData['errors'].keys():
                    workflowData['errors'][task] = {'failureTime': 0}
                if not step in workflowData['errors'][task].keys():
                    workflowData['errors'][task][step] = {}
                workflowData['errors'][task]['failureTime'] += (stop - start)
                stepFailures = workflowData['errors'][task][step]
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
            self.sendAlert(3, msg = msg)
            raise
        logging.debug("Finished committing workflow summary to couch")

        return


    def handleCouchPerformance(self, workflowName):
        """
        _handleCouchPerformance_

        The couch performance stuff is convoluted enough I think I want to handle it separately.
        """
        perf = self.fwjrdatabase.loadView("FWJRDump", "performanceByWorkflowName",
                                          options = {"startkey": [workflowName],
                                                     "endkey": [workflowName]})['rows']

        taskList   = {}
        finalTask  = {}

        for row in perf:
            taskName = row['value']['taskName']
            stepName = row['value']['stepName']
            if not taskName in taskList.keys():
                taskList[taskName] = {}
            if not stepName in taskList[taskName].keys():
                taskList[taskName][stepName] = []
            value = row['value']
            taskList[taskName][stepName].append(value)

        for taskName in taskList.keys():
            final = {}
            for stepName in taskList[taskName].keys():
                output = {'jobTime': []}
                final[stepName] = {}
                masterList = []

                # For each step put the data into a dictionary called output
                # keyed by the name of the value
                for row in taskList[taskName][stepName]:
                    masterList.append(row)
                    for key in row.keys():
                        if key in ['startTime', 'stopTime', 'taskName', 'stepName', 'jobID']:
                            continue
                        if not key in output.keys():
                            output[key] = []
                        try:
                            output[key].append(float(row[key]))
                        except TypeError:
                            # Why do we get None values here?
                            # We may want to look into it
                            logging.debug("Got a None performance value for key %s" % key)
                            if row[key] == None:
                                output[key].append(0.0)
                            else:
                                raise
                    try:
                        jobTime = row.get('stopTime', None) - row.get('startTime', None)
                        output['jobTime'].append(jobTime)
                        row['jobTime'] = jobTime
                    except TypeError:
                        # One of those didn't have a real value
                        pass

                # Now that we've sorted the data, we process it one key at a time
                for key in output.keys():
                    final[stepName][key] = {}
                    # Assemble the 'worstOffenders'
                    # These are the top [self.nOffenders] in that particular category
                    # i.e., those with the highest values
                    offenders = MathAlgos.getLargestValues(dictList = masterList, key = key,
                                                           n = self.nOffenders)
                    for x in offenders:
                        try:
                            logArchive = self.fwjrdatabase.loadView("FWJRDump", "logArchivesByJobID",
                                                                    options = {"startkey": [x['jobID']],
                                                                               "endkey": [x['jobID'],
                                                                                          x['retry_count']]})['rows'][0]['value']['lfn']
                            logCollectID = self.jobsdatabase.loadView("JobDump", "jobsByInputLFN",
                                                                      options = {"startkey": [workflowName, logArchive],
                                                                                 "endkey": [workflowName, logArchive]})['rows'][0]['value']
                            logCollect = self.fwjrdatabase.loadView("FWJRDump", "outputByJobID",
                                                                    options = {"startkey": logCollectID,
                                                                               "endkey": logCollectID})['rows'][0]['value']['lfn']
                            x['logArchive'] = logArchive.split('/')[-1]
                            x['logCollect'] = logCollect
                        except IndexError, ex:
                            logging.debug("Unable to find final logArchive tarball for %i" % x['jobID'])
                            logging.debug(str(ex))
                        except KeyError, ex:
                            logging.debug("Unable to find final logArchive tarball for %i" % x['jobID'])
                            logging.debug(str(ex))


                    if key in self.histogramKeys:
                        histogram = MathAlgos.createHistogram(numList = output[key],
                                                              nBins = self.histogramBins,
                                                              limit = self.histogramLimit)
                        final[stepName][key]['histogram'] = histogram
                    else:
                        average, stdDev = MathAlgos.getAverageStdDev(numList = output[key])
                        final[stepName][key]['average'] = average
                        final[stepName][key]['stdDev']  = stdDev

                    final[stepName][key]['worstOffenders'] = [{'jobID': x['jobID'], 'value': x.get(key, 0.0),
                                                               'log': x.get('logArchive', None),
                                                               'logCollect': x.get('logCollect', None)} for x in offenders]


            finalTask[taskName] = final
        return finalTask


    def createAndUploadPublish(self, workflow):
        """
        Upload to the UFC a JSON file with all the info needed to publish this dataset later
        """

        if self.uploadPublishDir:
            workDir = self.uploadPublishDir
        else:
            workDir, taskDir = getMasterName(startDir=self.jobCacheDir, workflow=workflow)

        try:
            return uploadPublishWorkflow(workflow, ufcEndpoint=self.userFileCacheURL, workDir=workDir)
        except Exception, ex:
            logging.error('Upload failed for workflow: %s' % (workflow))
            logging.exception(ex) #Let's print the stacktrace with generic Exception
            return False


    def deleteWorkflowFromCouch(self, workflowName):
        """
        _deleteWorkflowFromCouch_

        If we are asked to delete the workflow from couch, delete it
        to clear up some space.

        Load the document IDs and revisions out of couch by workflowName,
        then order a delete on them.
        """

        if not self.deleteCouchData:
            return


        jobs = self.jobsdatabase.loadView("JobDump", "jobsByWorkflowName",
                                          options = {"startkey": [workflowName],
                                                     "endkey": [workflowName, {}]})['rows']
        for j in jobs:
            id  = j['value']['id']
            rev = j['value']['rev']
            self.jobsdatabase.delete_doc(id = id, rev = rev)

        jobs = self.fwjrdatabase.loadView("FWJRDump", "fwjrsByWorkflowName",
                                          options = {"startkey": [workflowName],
                                                     "endkey": [workflowName, {}]})['rows']

        for j in jobs:
            id  = j['value']['id']
            rev = j['value']['rev']
            self.fwjrdatabase.delete_doc(id = id, rev = rev)

        return

