#!/usr/bin/env python
#pylint: disable=W1201
# W1201: Specify string format arguments as logging function parameters

"""
_PyCondorPlugin_

Example of Condor plugin
For glide-in use.
"""

import os
import time
import Queue
import os.path
import logging
import threading
import traceback
import subprocess
import multiprocessing
import glob
import fnmatch

import WMCore.Algorithms.BasicAlgos as BasicAlgos

from WMCore.Credential.Proxy           import Proxy
from WMCore.DAOFactory                 import DAOFactory
from WMCore.WMException                import WMException
from WMCore.WMInit                     import getWMBASE
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException
from WMCore.FwkJobReport.Report        import Report
from WMCore.Algorithms                 import SubprocessAlgos

##  python-condor stuff
import htcondor as condor
import classad


def submitWorker(input, results, timeout = None):
    """
    _outputWorker_

    Runs a subprocessed command.

    This takes whatever you send it (a single ID)
    executes the command
    and then returns the stdout result

    I planned this to do a glite-job-output command
    in massive parallel, possibly using the bulkID
    instead of the gridID.  Either way, all you have
    to change is the command here, and what is send in
    in the complete() function.
    """

    # Get this started
    while True:
        try:
            work = input.get()
        except (EOFError, IOError) as ex:
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            crashMessage += str(ex)
            logging.error(crashMessage)
            break
        except Exception as ex:
            msg =  "Hit unidentified exception getting work\n"
            msg += str(ex)
            msg += "Assuming everything's totally hosed.  Killing process.\n"
            logging.error(msg)
            break

        if work == 'STOP':
            # Put the brakes on
            logging.info("submitWorker multiprocess issued STOP command!")
            break

        command = work.get('command', None)
        idList  = work.get('idList', [])
        if not command:
            results.put({'stdout': '', 'stderr': '999100\n Got no command!', 'idList': idList})
            continue

        try:
            stdout, stderr, returnCode = SubprocessAlgos.runCommand(cmd = command, shell = True, timeout = timeout)
            if returnCode == 0:
                results.put({'stdout': stdout, 'stderr': stderr, 'idList': idList, 'exitCode': returnCode})
            else:
                results.put({'stdout': stdout,
                             'stderr': 'Non-zero exit code: %s\n stderr: %s' % (returnCode, stderr),
                             'exitCode': returnCode,
                             'idList': idList})
        except Exception as ex:
            msg =  "Critical error in subprocess while submitting to condor"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            results.put({'stdout': '', 'stderr': '999101\n %s' % msg, 'idList': idList, 'exitCode': 999101})

    return 0


def parseError(error):
    """
    Do some basic condor error parsing

    """

    errorCondition = True
    errorMsg       = error

    if 'ERROR: proxy has expired\n' in error:
        errorCondition = True
        errorMsg = 'CRITICAL ERROR: Your proxy has expired!\n'
    elif '999100\n' in error:
        errorCondition = True
        errorMsg = "CRITICAL ERROR: Failed to build submit command!\n"
    elif 'Failed to open command file' in error:
        errorCondition = True
        errorMsg = "CONDOR ERROR: jdl file not found by submitted jobs!\n"
    elif 'It appears that the value of pthread_mutex_init' in error:
        # glexec insists on spitting out to stderr
        lines = error.split('\n')
        if len(lines) == 2 and not lines[1]:
            errorCondition = False
            errorMsg = error

    return errorCondition, errorMsg





class PyCondorPlugin(BasePlugin):
    """
    _PyCondorPlugin_

    Condor plugin for glide-in submissions
    """

    @staticmethod
    def stateMap():
        """
        For a given name, return a global state


        """

        stateDict = {'New': 'Pending',
                     'Idle': 'Pending',
                     'Running': 'Running',
                     'Held': 'Error',
                     'Complete': 'Complete',
                     'Error': 'Error',
                     'Timeout': 'Error',
                     'Removed': 'Running',
                     'Unknown': 'Error'}

        # This call is optional but needs to for testing
        #BasePlugin.verifyState(stateDict)

        return stateDict

    @staticmethod
    def exitCodeMap():
        """
        Exit Codes and their meaing
        https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
        """
        exitCodeMap = { 0 : "Unknown", 
                        1 : "Idle",
                        2 : "Running",
                        3 : "Removed",
                        4 : "Complete",
                        5 : "Held",
                        6 : "Running" }

        return exitCodeMap


    def __init__(self, config):

        self.config = config

        BasePlugin.__init__(self, config)

        self.locationDict = {}

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)
        self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")


        self.packageDir = None

        if os.path.exists(os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')):
            self.unpacker = os.path.join(getWMBASE(),
                                         'src/python/WMCore/WMRuntime/Unpacker.py')
        else:
            self.unpacker = os.path.join(getWMBASE(),
                                         'WMCore/WMRuntime/Unpacker.py')

        self.agent         = getattr(config.Agent, 'agentName', 'WMAgent')
        self.sandbox       = None
        self.scriptFile    = None
        self.submitDir     = None
        self.removeTime    = getattr(config.BossAir, 'removeTime', 60)
        self.useGSite      = getattr(config.BossAir, 'useGLIDEINSites', False)
        self.submitWMSMode = getattr(config.BossAir, 'submitWMSMode', False)
        self.errorThreshold= getattr(config.BossAir, 'submitErrorThreshold', 10)
        self.errorCount    = 0
        self.defaultTaskPriority = getattr(config.BossAir, 'defaultTaskPriority', 0)
        self.maxTaskPriority     = getattr(config.BossAir, 'maxTaskPriority', 1e7)

        # Required for global pool accounting
        self.acctGroup = getattr(config.BossAir, 'acctGroup', "production")
        self.acctGroupUser = getattr(config.BossAir, 'acctGroupUser', "cmsdataops")

        # Build ourselves a pool
        self.pool     = []
        self.input    = None
        self.result   = None
        self.nProcess = getattr(self.config.BossAir, 'nCondorProcesses', 4)

        # Set up my proxy and glexec stuff
        self.setupScript = getattr(config.BossAir, 'UISetupScript', None)
        self.proxy       = None
        self.serverCert  = getattr(config.BossAir, 'delegatedServerCert', None)
        self.serverKey   = getattr(config.BossAir, 'delegatedServerKey', None)
        self.myproxySrv  = getattr(config.BossAir, 'myproxyServer', None)
        self.proxyDir    = getattr(config.BossAir, 'proxyDir', '/tmp/')
        self.serverHash  = getattr(config.BossAir, 'delegatedServerHash', None)
        self.glexecPath  = getattr(config.BossAir, 'glexecPath', None)
        self.glexecWrapScript = getattr(config.BossAir, 'glexecWrapScript', None)
        self.glexecUnwrapScript = getattr(config.BossAir, 'glexecUnwrapScript', None)
        self.jdlProxyFile    = None # Proxy name to put in JDL (owned by submit user)
        self.glexecProxyFile = None # Copy of same file owned by submit user

        if self.glexecPath:
            if not (self.myproxySrv and self.proxyDir):
                raise WMException('glexec requires myproxyServer and proxyDir to be set.')
        if self.myproxySrv:
            if not (self.serverCert and self.serverKey):
                raise WMException('MyProxy server requires serverCert and serverKey to be set.')

        # Make the directory for the proxies
        if self.proxyDir and not os.path.exists(self.proxyDir):
            logging.debug("proxyDir not found: creating it.")
            try:
                os.makedirs(self.proxyDir, 0o1777)
            except Exception as ex:
                msg = "Error: problem when creating proxyDir directory - '%s'" % str(ex)
                raise BossAirPluginException(msg)
        elif not os.path.isdir(self.proxyDir):
            msg = "Error: proxyDir '%s' is not a directory" % self.proxyDir
            raise BossAirPluginException(msg)

        if self.serverCert and self.serverKey and self.myproxySrv:
            self.proxy = self.setupMyProxy()

        # Build a request string
        self.reqStr = "(Memory >= 1 && OpSys == \"LINUX\" ) && (Arch == \"INTEL\" || Arch == \"X86_64\") && stringListMember(GLIDEIN_CMSSite, DESIRED_Sites) && ((REQUIRED_OS==\"any\") || (GLIDEIN_REQUIRED_OS==REQUIRED_OS))"
        if hasattr(config.BossAir, 'condorRequirementsString'):
            self.reqStr = config.BossAir.condorRequirementsString

        return


    def __del__(self):
        """
        __del__

        Trigger a close of connections if necessary
        """
        self.close()


    def setupMyProxy(self):
        """
        _setupMyProxy_

        Setup a WMCore.Credential.Proxy object with which to retrieve
        proxies from myproxy using the server Cert
        """

        args = {}
        if self.setupScript:
            args['uisource'] = self.setupScript
        args['server_cert'] = self.serverCert
        args['server_key']  = self.serverKey
        args['myProxySvr']  = self.myproxySrv
        args['credServerPath'] = self.proxyDir
        args['logger'] = logging
        return Proxy(args = args)


    def close(self):
        """
        _close_

        Kill all connections and terminate
        """
        terminate = False
        for x in self.pool:
            try:
                self.input.put('STOP')
            except Exception as ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                logging.error(msg)
                terminate = True
        try:
            self.input.close()
            self.result.close()
        except:
            # There's really not much we can do about this
            pass
        for proc in self.pool:
            if terminate:
                try:
                    proc.terminate()
                except Exception as ex:
                    logging.error("Failure while attempting to terminate process")
                    logging.error(str(ex))
                    continue
            else:
                try:
                    proc.join()
                except Exception as ex:
                    try:
                        proc.terminate()
                    except Exception as ex2:
                        logging.error("Failure to join or terminate process")
                        logging.error(str(ex))
                        logging.error(str(ex2))
                        continue
        # At the end, clean the pool and the queues
        self.pool   = []
        self.input  = None
        self.result = None
        return



    def submit(self, jobs, info=None):
        """
        _submit_


        Submit jobs for one subscription
        """

        # If we're here, then we have submitter components
        self.scriptFile = self.config.JobSubmitter.submitScript
        self.submitDir  = self.config.JobSubmitter.submitDir
        timeout         = getattr(self.config.JobSubmitter, 'getTimeout', 400)

        successfulJobs = []
        failedJobs     = []
        jdlFiles       = []

        if len(jobs) == 0:
            # Then was have nothing to do
            return successfulJobs, failedJobs

        if len(self.pool) == 0:
            # Starting things up
            # This is obviously a submit API
            logging.info("Starting up PyCondorPlugin worker pool")
            self.input    = multiprocessing.Queue()
            self.result   = multiprocessing.Queue()
            for x in range(self.nProcess):
                p = multiprocessing.Process(target = submitWorker,
                                            args = (self.input, self.result, timeout))
                p.start()
                self.pool.append(p)

        if not os.path.exists(self.submitDir):
            os.makedirs(self.submitDir)


        # Now assume that what we get is the following; a mostly
        # unordered list of jobs with random sandboxes.
        # We intend to sort them by sandbox.

        submitDict = {}
        nSubmits   = 0
        for job in jobs:
            sandbox = job['sandbox']
            if not sandbox in submitDict.keys():
                submitDict[sandbox] = []
            submitDict[sandbox].append(job)


        # Now submit the bastards
        queueError = False
        for sandbox in submitDict.keys():
            jobList = submitDict.get(sandbox, [])
            idList = [x['jobid'] for x in jobList]
            if queueError:
                # If the queue has failed, then we must not process
                # any more jobs this cycle.
                continue
            while len(jobList) > 0:
                jobsReady = jobList[:self.config.JobSubmitter.jobsPerWorker]
                jobList   = jobList[self.config.JobSubmitter.jobsPerWorker:]
                idList    = [x['id'] for x in jobsReady]
                jdlList = self.makeSubmit(jobList = jobsReady)
                if not jdlList or jdlList == []:
                    # Then we got nothing
                    logging.error("No JDL file made!")
                    return {'NoResult': [0]}
                jdlFile = "%s/submit_%i_%i.jdl" % (self.submitDir, os.getpid(), idList[0])
                handle = open(jdlFile, 'w')
                handle.writelines(jdlList)
                handle.close()
                jdlFiles.append(jdlFile)

                # Now submit them
                logging.info("About to submit %i jobs" %(len(jobsReady)))
                if self.glexecPath:
                    command = 'CS=`which condor_submit`; '
                    if self.glexecWrapScript:
                        command += 'export GLEXEC_ENV=`%s 2>/dev/null`; ' % self.glexecWrapScript
                    command += 'export GLEXEC_CLIENT_CERT=%s; ' % self.glexecProxyFile
                    command += 'export GLEXEC_SOURCE_PROXY=%s; ' % self.glexecProxyFile
                    command += 'export X509_USER_PROXY=%s; ' % self.glexecProxyFile
                    command += 'export GLEXEC_TARGET_PROXY=%s; ' % self.jdlProxyFile
                    if self.glexecUnwrapScript:
                        command += '%s %s -- $CS %s' % (self.glexecPath, self.glexecUnwrapScript, jdlFile)
                    else:
                        command += '%s $CS %s' % (self.glexecPath, jdlFile)
                else:
                    command = "condor_submit %s" % jdlFile

                try:
                    self.input.put({'command': command, 'idList': idList})
                except AssertionError as ex:
                    msg =  "Critical error: input pipeline probably closed.\n"
                    msg += str(ex)
                    msg += "Error Procedure: Something critical has happened in the worker process\n"
                    msg += "We will now proceed to pull all useful data from the queue (if it exists)\n"
                    msg += "Then refresh the worker pool\n"
                    logging.error(msg)
                    queueError = True
                    break
                nSubmits += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        for n in range(nSubmits):
            try:
                res = self.result.get(block = True, timeout = timeout)
            except Queue.Empty:
                # If the queue was empty go to the next submit
                # Those jobs have vanished
                logging.error("Queue.Empty error received!")
                logging.error("This could indicate a critical condor error!")
                logging.error("However, no information of any use was obtained due to process failure.")
                logging.error("Either process failed, or process timed out after %s seconds." % timeout)
                queueError = True
                continue
            except AssertionError as ex:
                msg =  "Found Assertion error while retrieving output from worker process.\n"
                msg += str(ex)
                msg += "This indicates something critical happened to a worker process"
                msg += "We will recover what jobs we know were submitted, and resubmit the rest"
                msg += "Refreshing worker pool at end of loop"
                logging.error(msg)
                queueError = True
                continue

            try:
                output   = res['stdout']
                error    = res['stderr']
                idList   = res['idList']
                exitCode = res['exitCode']
            except KeyError as ex:
                msg =  "Error in finding key from result pipe\n"
                msg += "Something has gone crticially wrong in the worker\n"
                try:
                    msg += "Result: %s\n" % str(res)
                except:
                    pass
                msg += str(ex)
                logging.error(msg)
                queueError = True
                continue

            if not exitCode == 0:
                logging.error("Condor returned non-zero.  Printing out command stderr")
                logging.error(error)
                errorCheck, errorMsg = parseError(error = error)
                logging.error("Processing failed jobs and proceeding to the next jobs.")
                logging.error("Do not restart component.")
            else:
                errorCheck = None

            if errorCheck:
                self.errorCount += 1
                condorErrorReport = Report()
                condorErrorReport.addError("JobSubmit", 61202, "CondorError", errorMsg)
                for jobID in idList:
                    for job in jobs:
                        if job.get('id', None) == jobID:
                            job['fwjr'] = condorErrorReport
                            failedJobs.append(job)
                            break
            else:
                if self.errorCount > 0:
                    self.errorCount -= 1
                for jobID in idList:
                    for job in jobs:
                        if job.get('id', None) == jobID:
                            successfulJobs.append(job)
                            break

            # If we get a lot of errors in a row it's probably time to
            # report this to the operators.
            if self.errorCount > self.errorThreshold:
                try:
                    msg = "Exceeded errorThreshold while submitting to condor. Check condor status."
                    logging.error(msg)
                    logging.error("Reporting to Alert system and continuing to process jobs")
                    from WMCore.Alerts import API as alertAPI
                    preAlert, sender = alertAPI.setUpAlertsMessaging(self,
                                                                     compName = "BossAirPyCondorPlugin")
                    sendAlert = alertAPI.getSendAlert(sender = sender,
                                                      preAlert = preAlert)
                    sendAlert(6, msg = msg)
                    sender.unregister()
                    self.errorCount = 0
                except:
                    # There's nothing we can really do here
                    pass

        # Remove JDL files unless commanded otherwise
        if getattr(self.config.JobSubmitter, 'deleteJDLFiles', True):
            for f in jdlFiles:
                os.remove(f)

        # When we're finished, clean up the queue workers in order
        # to free up memory (in the midst of the process, the forked
        # memory space shouldn't be touched, so it should still be
        # shared, but after this point any action by the Submitter will
        # result in memory duplication).
        logging.info("Purging worker pool to clean up memory")
        self.close()


        # We must return a list of jobs successfully submitted,
        # and a list of jobs failed
        logging.info("Done submitting jobs for this cycle in PyCondorPlugin")
        return successfulJobs, failedJobs




    def track(self, jobs, info=None):
        """
        _track_

        Track the jobs while in condor
        This returns a three-way ntuple
        First, the total number of jobs still running
        Second, the jobs that need to be changed
        Third, the jobs that need to be completed
        """

        changeList   = []
        completeList = []
        runningList  = []
        noInfoFlag   = False

        # Get the job
        logging.debug("PyCondor is going to track %s jobs" % (len(jobs)))
        jobInfo, sd = self.getClassAds()
        if jobInfo is None :
            return runningList, changeList, completeList
        else:
            logging.debug("PyCondor retrieved %s classAds from condor schedd" % (len(jobInfo)))

        if len(jobInfo.keys()) == 0:
            noInfoFlag = True

        # Now go over the jobs from WMBS and see what we have
        for job in jobs:
            if not job['jobid'] in jobInfo.keys():
                if noInfoFlag:
                    self.procJobNoInfo(job, changeList, completeList)
                else:
                    self.procCondorLog(job, changeList, completeList, runningList)
            else:
                self.procClassAd(job, jobInfo, changeList, completeList, runningList)

        logging.debug("PyCondorPlugin tracking : %i/%i/%i (Running/Changing/Complete)" %( len(runningList), 
                                                                                          len(changeList), 
                                                                                          len(completeList)))

        return runningList, changeList, completeList


    def complete(self, jobs):
        """
        Do any completion work required

        In this case, look for a returned logfile
        """

        for job in jobs:
            if job.get('cache_dir', None) == None or job.get('retry_count', None) == None:
                # Then we can't do anything
                logging.error("Can't find this job's cache_dir in PyCondorPlugin.complete")
                logging.error("cache_dir: %s" % job.get('cache_dir', 'Missing'))
                logging.error("retry_count: %s" % job.get('retry_count', 'Missing'))
                continue
            reportName = os.path.join(job['cache_dir'], 'Report.%i.pkl' % job['retry_count'])
            if os.path.isfile(reportName) and os.path.getsize(reportName) > 0:
                # Then we have a real report.
                # Do nothing
                continue
            if os.path.isdir(reportName):
                # Then something weird has happened.
                # File error, do nothing
                logging.error("Went to check on error report for job %i.  Found a directory instead.\n" % job['id'])
                logging.error("Ignoring this, but this is very strange.\n")

            # If we're still here, we must not have a real error report
            logOutput = 'Could not find jobReport\n'
            #But we don't know exactly the condor id, so it will append
            #the last lines of the latest condor log in cache_dir
            genLogPath = os.path.join(job['cache_dir'], 'condor.*.*.log')
            logPaths = glob.glob(genLogPath)
            errLog = None
            if len(logPaths):
                errLog = max(logPaths, key = lambda path :
                                                    os.stat(path).st_mtime)
            if errLog != None and os.path.isfile(errLog):
                logTail = BasicAlgos.tail(errLog, 50)
                logOutput += 'Adding end of condor.log to error message:\n'
                logOutput += '\n'.join(logTail)
            if not os.path.isdir(job['cache_dir']):
                msg =  "Serious Error in Completing condor job with id %s!\n" % job.get('id', 'unknown')
                msg += "Could not find jobCache directory - directory deleted under job: %s\n" % job['cache_dir']
                msg += "Creating artificial cache_dir for failed job report\n"
                logging.error(msg)
                os.makedirs(job['cache_dir'])
                logOutput += msg
                condorReport = Report()
                condorReport.addError("NoJobReport", 99304, "NoCacheDir", logOutput)
                condorReport.save(filename = reportName)
                continue
            condorReport = Report()
            condorReport.addError("NoJobReport", 99303, "NoJobReport", logOutput)
            if os.path.isfile(reportName):
                # Then we have a file already there.  It should be zero size due
                # to the if statements above, but we should remove it.
                if os.path.getsize(reportName) > 0:
                    # This should never happen.  If it does, ignore it
                    msg =  "Critical strange problem.  FWJR changed size while being processed."
                    logging.error(msg)
                else:
                    try:
                        os.remove(reportName)
                        condorReport.save(filename = reportName)
                    except Exception as ex:
                        logging.error("Cannot remove and replace empty report %s" % reportName)
                        logging.error("Report continuing without error!")
            else:
                condorReport.save(filename = reportName)

            # Debug message to end loop
            logging.debug("No returning job report for job %i" % job['id'])


        return


    def updateSiteInformation(self, jobs, siteName, excludeSite):
        """
        _updateSiteInformation_

        Modify condor classAd for all Idle jobs for a site if it has gone Down, Draining or Aborted.
        Kill all jobs if the site is the only site for the job.
        This expects:    excludeSite = False when moving to Normal
                         excludeSite = True when moving to Down, Draining or Aborted
        """
        jobInfo, sd = self.getClassAds()
        jobtokill=[]
        for job in jobs:
            jobID = job['id']
            jobAd = jobInfo.get(jobID)
            if not jobAd:
                logging.debug("No jobAd received for jobID %i"%jobID)
            else:
                desiredSites = jobAd.get('DESIRED_Sites').split(', ')
                extDesiredSites = jobAd.get('ExtDESIRED_Sites').split(', ')
                if excludeSite:
                    #Remove siteName from DESIRED_Sites if job has it
                    if siteName in desiredSites and siteName in extDesiredSites:
                        usi = desiredSites
                        if len(usi) > 1:
                            usi.remove(siteName)
                            usi = ','.join(map(str, usi))
                            sd.edit('WMAgent_JobID == %i'%jobID, "DESIRED_Sites", classad.ExprTree('"%s"'% usi))
                        else:
                            jobtokill.append(job)
                    else:
                        #If job doesn't have the siteName in the siteList, just ignore it
                        logging.debug("Cannot find siteName %s in the sitelist" % siteName)
                else:
                    #Add siteName to DESIRED_Sites if ExtDESIRED_Sites has it (moving back to Normal)
                    if siteName not in desiredSites and siteName in extDesiredSites:
                        usi = desiredSites
                        usi.append(siteName)
                        usi = ','.join(map(str, usi))
                        sd.edit('WMAgent_JobID == %i'%jobID, "DESIRED_Sites", classad.ExprTree('"%s"'% usi))
                    else:
                        #If job doesn't have the siteName in the siteList, just ignore it
                        logging.debug("Cannot find siteName %s in the sitelist" % siteName)

        return jobtokill



    def kill(self, jobs, info=None):
        """
        _kill_

        Kill a list of jobs based on the WMBS job names.
        Kill can happen for schedd running on localhost... TBC.
        """
        sd = condor.Schedd()
        for job in jobs:
            logging.debug("Going to remove jobid=%i from the queue" % job['jobid'])
            sd.act(condor.JobAction.Remove, 'WMAgent_JobID == %i' % job['jobid'])
            logging.debug("Removed jobid=%i from the queue" % job['jobid'])

        return

    def killWorkflowJobs(self, workflow):
        """
        _killWorkflowJobs_

        Kill all the jobs belonging to a specif workflow.
        """
        sd = condor.Schedd()
        logging.debug("Going to remove all the jobs for workflow %s" % workflow)
        sd.act(condor.JobAction.Remove, 'WMAgent_RequestName == %s' % classad.quote(str(workflow)))

        return

    def updateJobInformation(self, workflow, task, **kwargs):
        """
        _updateJobInformation_

        Update job information for all jobs in the workflow and task,
        the change will take effect if the job is Idle or becomes idle.

        The currently supported changes are only priority for which both the task (taskPriority)
        and workflow priority (requestPriority) must be provided.
        """
        sd = condor.Schedd()
        if 'taskPriority' in kwargs and 'requestPriority' in kwargs:
            # Do a priority update
            priority = (int(kwargs['requestPriority']) + int(kwargs['taskPriority'] * self.maxTaskPriority))
            try:
                sd.edit('WMAgent_JobID =!= "UNDEFINED" && WMAgent_SubTaskName == %s && WMAgent_RequestName == %s && JobPrio != %d' %
                        (classad.quote(str(task)),classad.quote(str(workflow)),classad.Literal(int(priority))), "JobPrio", classad.Literal(int(priority)))
            except:
                msg = "Couldn\'t edit classAd to change job Priority for WMAgent_SubTaskName=%s, WMAgent_RequestName=%s " % (classad.quote(str(task)), classad.quote(str(workflow)))
                logging.debug(msg)

        return

    # Start with submit functions


    def initSubmit(self, jobList=None):
        """
        _makeConfig_

        Make common JDL header
        """
        jdl = []


        # -- scriptFile & Output/Error/Log filenames shortened to
        #    avoid condorg submission errors from > 256 character pathnames

        jdl.append("universe = vanilla\n")
        jdl.append("requirements = %s\n" % self.reqStr)

        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Executable = %s\n" % self.scriptFile)
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")

        jdl.append("+WMAgent_AgentName = \"%s\"\n" %(self.agent))
        jdl.append("+JOBGLIDEIN_CMSSite= \"$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\\"Unknown\\\", GLIDEIN_CMSSite)])\"\n")

        # Required for global pool accounting
        jdl.append("+AcctGroup = \"%s\"\n" % (self.acctGroup))
        jdl.append("+AcctGroupUser = \"%s\"\n" %(self.acctGroupUser))
        jdl.append("+AccountingGroup = \"%s.%s\"\n" %(self.acctGroup, self.acctGroupUser))
        
        jdl.extend(self.customizeCommon(jobList))

        if self.proxy:
            # Then we have to retrieve a proxy for this user
            job0   = jobList[0]
            userDN = job0.get('userdn', None)
            if not userDN:
                # Then we can't build ourselves a proxy
                logging.error("Asked to build myProxy plugin, but no userDN available!")
                logging.error("Checked job %i" % job0['id'])
                return jdl
            logging.info("Fetching proxy for %s" % userDN)
            # Build the proxy
            # First set the userDN of the Proxy object
            self.proxy.userDN = userDN
            # Second, get the actual proxy
            if self.serverHash:
                # If we built our own serverHash, we have to be able to send it in
                filename = self.proxy.logonRenewMyProxy(credServerName = self.serverHash)
            else:
                # Else, build the serverHash from the proxy sha1
                filename = self.proxy.logonRenewMyProxy()
            logging.info("Proxy stored in %s" % filename)
            if self.glexecPath:
                self.jdlProxyFile = '%s.user' % filename
                self.glexecProxyFile = filename
                command = 'export GLEXEC_CLIENT_CERT=%s; export GLEXEC_SOURCE_PROXY=%s; export X509_USER_PROXY=%s; ' % \
                          (self.glexecProxyFile, self.glexecProxyFile, self.glexecProxyFile) + \
                          'export GLEXEC_TARGET_PROXY=%s; %s /usr/bin/id' % \
                          (self.jdlProxyFile, self.glexecPath)
                proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                        stdout = subprocess.PIPE, shell = True)
                out, err = proc.communicate()
                logging.info("Created new user proxy with glexec %s" % self.jdlProxyFile)
            else:
                self.jdlProxyFile = filename
            jdl.append("x509userproxy = %s\n" % self.jdlProxyFile)

        return jdl

    def customizeCommon(self, jobList):
        """
        JDL additions just for this implementation. Over-ridden in sub-classes
        These are the Glide-in specific bits
        """
        jdl = []
        jdl.append('+DESIRED_Archs = \"INTEL,X86_64\"\n')
        jdl.append('+REQUIRES_LOCAL_DATA = True\n')

        return jdl

    def makeSubmit(self, jobList):
        """
        _makeSubmit_

        For a given job/cache/spec make a JDL fragment to submit the job

        """

        if len(jobList) < 1:
            #I don't know how we got here, but we did
            logging.error("No jobs passed to plugin")
            return None

        jdl = self.initSubmit(jobList)


        # For each script we have to do queue a separate directory, etc.
        for job in jobList:
            if job == {}:
                # Then I don't know how we got here either
                logging.error("Was passed a nonexistant job.  Ignoring")
                continue
            jdl.append("initialdir = %s\n" % job['cache_dir'])
            jdl.append("transfer_input_files = %s, %s/%s, %s\n" \
                       % (job['sandbox'], job['packageDir'],
                          'JobPackage.pkl', self.unpacker))
            argString = "arguments = %s %i\n" \
                        % (os.path.basename(job['sandbox']), job['id'])
            jdl.append(argString)

            jdl.extend(self.customizePerJob(job))

            # Transfer the output files
            jdl.append("transfer_output_files = Report.%i.pkl\n" % (job["retry_count"]))

            # Add priority if necessary
            task_priority = job.get("taskPriority", self.defaultTaskPriority)
            try:
                task_priority = int(task_priority)
            except:
                logging.error("Priority for task not castable to an int")
                logging.error("Not setting priority")
                logging.debug("Priority: %s" % task_priority)
                task_priority = 0

            prio = 0
            if job.get('priority', None) != None:
                try:
                    prio = int(job['priority'])
                except ValueError:
                    logging.error("Priority for job %i not castable to an int\n" % job['id'])
                    logging.error("Not setting priority")
                    logging.debug("Priority: %s" % job['priority'])
                except Exception as ex:
                    logging.error("Got unhandled exception while setting priority for job %i\n" % job['id'])
                    logging.error(str(ex))
                    logging.error("Not setting priority")

            jdl.append("priority = %i\n" % (task_priority + prio*self.maxTaskPriority))

            jdl.append("+PostJobPrio1 = -%d\n" % len(job.get('potentialSites', [])))
            jdl.append("+PostJobPrio2 = -%d\n" % job['taskID'])

            jdl.append("+WMAgent_JobID = %s\n" % job['jobid'])
            jdl.append("job_machine_attrs = GLIDEIN_CMSSite\n")

            ### print all the variables needed for us to rely on condor userlog
            jdl.append("job_ad_information_attrs = JobStatus,QDate,EnteredCurrentStatus,JobStartDate,DESIRED_Sites,ExtDESIRED_Sites,WMAgent_JobID,MachineAttrGLIDEIN_CMSSite0\n")

            jdl.append("Queue 1\n")

        return jdl

    def customizePerJob(self, job):
        """
        JDL additions just for this implementation. Over-ridden in sub-classes
        These are the Glide-in specific bits
        """
        jdl = []
        jobCE = job['location']
        if not jobCE:
            # Then we ended up with a site that doesn't exist?
            logging.error("Job for non-existant site %s" \
                            % (job['location']))
            return jdl

        if self.useGSite:
            jdl.append('+GLIDEIN_CMSSite = \"%s\"\n' % (jobCE))
        if self.submitWMSMode and len(job.get('possibleSites', [])) > 0:
            strg = ','.join(map(str, job.get('possibleSites')))
            jdl.append('+DESIRED_Sites = \"%s\"\n' % strg)
        else:
            jdl.append('+DESIRED_Sites = \"%s\"\n' %(jobCE))

        if self.submitWMSMode and len(job.get('potentialSites', [])) > 0:
            strg = ','.join(map(str, job.get('potentialSites')))
            jdl.append('+ExtDESIRED_Sites = \"%s\"\n' % strg)
        else:
            jdl.append('+ExtDESIRED_Sites = \"%s\"\n' %(jobCE))

        if job.get('proxyPath', None):
            jdl.append('x509userproxy = %s\n' % job['proxyPath'])

        if job.get('requestName', None):
            jdl.append('+WMAgent_RequestName = "%s"\n' % job['requestName'])

        if job.get('taskName', None):
            jdl.append('+WMAgent_SubTaskName = "%s"\n' % job['taskName'])

        if job.get('taskType', None):
            jdl.append('+CMS_JobType = "%s"\n' % job['taskType'])

        # dataset info
        if job.get('inputDataset', None):
            jdl.append('+DESIRED_CMSDataset = "%s"\n' % job['inputDataset'])
        if job.get('inputDatasetLocations', None):
            jdl.append('+DESIRED_CMSDatasetLocations = "%s"\n' % ','.join(job['inputDatasetLocations']))

        # Performance estimates
        if job.get('estimatedJobTime', None):
            jdl.append('+MaxWallTimeMins = %d\n' % int(job['estimatedJobTime']/60.0))
        if job.get('estimatedMemoryUsage', None):
            jdl.append('request_memory = %d\n' % int(job['estimatedMemoryUsage']))
        if job.get('estimatedDiskUsage', None):
            jdl.append('request_disk = %d\n' % int(job['estimatedDiskUsage']))

        # Set up JDL for multithreaded jobs
        if job.get('numberOfCores', 1) > 1:
            jdl.append('machine_count = 1\n')
            jdl.append('request_cpus = %s\n' % job.get('numberOfCores', 1))

        #Add OS requirements for jobs
        if job.get('scramArch') is not None and job.get('scramArch').startswith("slc6_") :
            jdl.append('+REQUIRED_OS = "rhel6"\n')
        else:
            jdl.append('+REQUIRED_OS = "any"\n')

        return jdl

    def getCEName(self, jobSite):
        """
        _getCEName_

        This is how you get the name of a CE for a job
        """

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName = jobSite)
            self.locationDict[jobSite] = siteInfo[0].get('ce_name', None)
        return self.locationDict[jobSite]

    def getClassAds(self):
        """
        _getClassAds_

        Grab CONDOR classAds using CONDOR-PYTHON

        This looks at the schedd running on the
        Submit-Host and edit/remove jobs
        """

        jobInfo = {}
        schedd = condor.Schedd()
        results=[]
        
        try :
            logging.debug("Start: Retrieving classAds using Condor Python XQuery")
            itobj = schedd.xquery('WMAgent_JobID =!= "UNDEFINED" && WMAgent_AgentName == %s' % classad.quote(str(self.agent)),
                                  ["JobStatus", "EnteredCurrentStatus", "JobStartDate", "QDate", "DESIRED_Sites",
                                   "ExtDESIRED_Sites", "MachineAttrGLIDEIN_CMSSite0", "WMAgent_JobID"]
                                  )
            results = list(itobj)
            logging.debug("Finish: Retrieving classAds using Condor Python XQuery")
        except :
            msg = "Query to condor schedd failed in PyCondorPlugin"
            logging.debug(msg)
            return None, None
        else:
            for i in range(0, len(results)):
                
                ### This condition ignores jobs that are Removed, but stay in the X state
                ### For manual condor_rm removal, job wont be in the queue \
                ### and status of the jobs will be read from condor log
                if results[i].get("JobStatus",0)==3:
                    continue
                else :
                    ## For some strange race condition, schedd sometimes does not publish StatDate for a Running Job
                    ## Get the entire classad for such a job
                    ## Do not crash WMA, wait for next polling cycle to get all the info.
                    if results[i].get("JobStatus",0)==2 and results[i].get("JobStartDate") is None :
                        logging.debug("THIS SHOULD NOT HAPPEN. JobStartDate is MISSING from the CLASSAD.")
                        logging.debug("Could be caused by some race condition. Wait for the next Polling Cycle")
                        logging.debug("%s" % str(results[i]))
                
                    tmpDict={}
                    tmpDict["JobStatus"]=int(results[i].get("JobStatus",100))
                    tmpDict["stateTime"]=int(results[i].get("EnteredCurrentStatus",0))
                    tmpDict["runningTime"]=int(results[i].get("JobStartDate",0))
                    tmpDict["submitTime"]=int(results[i].get("QDate",0))
                    tmpDict["DESIRED_Sites"]=results[i].get("DESIRED_Sites")
                    tmpDict["ExtDESIRED_Sites"]=results[i].get("ExtDESIRED_Sites")
                    tmpDict["runningCMSSite"]=results[i].get("MachineAttrGLIDEIN_CMSSite0")
                    tmpDict["WMAgentID"]=int(results[i].get("WMAgent_JobID",0))
                    _tmpID = tmpDict["WMAgentID"]
                    jobInfo[_tmpID] = tmpDict
                
            logging.info("Retrieved %i classAds" % len(jobInfo))
            
        return jobInfo, schedd


    def readCondorLog(self, job):
        """
        __readCondorLog
        
        If schedd fails to give information about a job
        Check the condor log file for ths job
        Extract Exit status

        """
        def LogToScheddExitCodeMap(x):
            ### JobStatus shows the last status of the job
            ### Get TriggerEventTypeNumber which is the current status of the job
            ### Map it back to Schedd Status
            ### Mapping done using the exit codes from condor website,
            ### https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
            LogExitCode={0:1,1:1,2:0,3:2,4:3,5:4,6:2,7:0,8:0,9:4,10:0,11:1,12:5,13:2}
            n = LogExitCode.get(x) if LogExitCode.get(x) is not None else 100
            return n


        ### This should select the latest log file in the cache_dir
        fmtime=0
        logFile=None
        for joblog in os.listdir(job['cache_dir']):
            if fnmatch.fnmatch(joblog, 'condor.*.*.log'):
                _tmplogFile=os.path.join(job['cache_dir'],joblog)
                _tmpfmtime=int(os.path.getmtime(_tmplogFile))
                if _tmpfmtime > fmtime:
                    fmtime = _tmpfmtime
                    logFile = _tmplogFile


        jobLogInfo={}
        try :
            logging.debug("Opening condor job log file: %s" % logFile)
            logfileobj=open(logFile,"r")
        except :
            logging.debug('Cannot open condor job log file %s'% logFile)
        else :
            tmpDict={}
            cres=condor.read_events(logfileobj,1)
            ulog=list(cres)
            if len(ulog) > 0:
                _tmpStat = int(ulog[-1]["TriggerEventTypeNumber"])
                tmpDict["JobStatus"]=LogToScheddExitCodeMap(_tmpStat)
                tmpDict["submitTime"]=int(ulog[-1]["QDate"])
                tmpDict["runningTime"]=int(ulog[-1]["JobStartDate"]) if ulog[-1]["JobStartDate"] is not None else 0
                tmpDict["stateTime"]=int(ulog[-1]["EnteredCurrentStatus"])
                tmpDict["runningCMSSite"]=ulog[-1]["MachineAttrGLIDEIN_CMSSite0"]
                tmpDict["WMAgentID"]=int(ulog[-1]["WMAgent_JobID"])
                _tmpID = tmpDict["WMAgentID"]
                jobLogInfo[_tmpID] = tmpDict
            else :
                logging.debug('%s is EMPTY' % str(logFile))

        logging.info("Retrieved %i Info from Condor Job Log file %s" % (len(jobLogInfo), logFile))
                    
        return jobLogInfo

        

    def procJobNoInfo(self, job, changeList, completeList):
        """
        Process jobs where No ClassAd info is received from schedd
        """
        if not job['status'] == 'Removed':
            logging.debug("noInfoFlag is True and JobStatus for jobid=%i is %s" % (job['jobid'], job['status']))
            # If the job is not in removed, move it to removed
            job['status']      = 'Removed'
            job['status_time'] = int(time.time())
            changeList.append(job)
        elif time.time() - float(job['status_time']) > self.removeTime:
            logging.debug("noInfoFlag is True and JobStatus for jobid=%i is %s" % (job['jobid'], job['status']))
            # If the job is in removed, and it's been missing for more
            # then self.removeTime, remove it.
            completeList.append(job)
    
            
    def procCondorLog(self, job, changeList, completeList, runningList):
        """
        Process jobs where classad for jobs are not received
        """
        ### There could be multiple condor log files under the same cache_dir
        ### Get the one that corresponds to [jobid] ==> WMAgent_JobID
        jobLogInfo = self.readCondorLog(job)
        jobAd = jobLogInfo.get(job['jobid'])
        if jobAd is None :
            ## If neither jobAd and no jobLog, assume job is complete
            logging.debug("No job log Info for jobid=%i. Assume it is Complete. Check DB." % job['jobid'])
            completeList.append(job)
            return

        jobStatus = int(jobAd.get('JobStatus',100))
        
        statName  = 'Unknown'
        if jobStatus in PyCondorPlugin.exitCodeMap().keys():
            statName = PyCondorPlugin.exitCodeMap()[jobStatus]
            
        if statName == "Unknown" :
            logging.info("JobLogInfo: jobid=%i in unknown state %i" % (job['jobid'], jobStatus))
            
        # Get the global state
        job['globalState'] = PyCondorPlugin.stateMap()[statName]
        logging.debug("JobLogInfo: JobStatus for jobid=%i is %s" % (job['jobid'],job['status']))
        if statName != job['status']:
            job['status']      = statName
            job['status_time'] = 0
            logging.debug("JobLogInfo: JobStatus for jobid=%i changed to %s" % (job['jobid'],job['status']))
                        
        #Check if we have a valid status time
        #Do not catch exception here, Wait for next polling cycle
        if not job['status_time']:
            if job['status'] == 'Running':
                job['status_time'] = int(jobAd.get('runningTime', 0))

                # Check location for THIS running Job
                job['location'] = jobAd.get('runningCMSSite', None)
                if job['location'] is None:
                    logging.debug('JobLogInfo: Something IS NOT right here, a job (%s) is running with no CMS site' % str(jobAd))

            elif job['status'] == 'Idle':
                job['status_time'] = int(jobAd.get('submitTime', 0))
            else:
                job['status_time'] = int(jobAd.get('stateTime', 0))
                
            changeList.append(job)

        ## Add the job to Complete list if the status is Removed
        if job['status'] == "Complete" or job['status'] == "Removed" :
            completeList.append(job)
            return
            
        runningList.append(job) 

    
    def procClassAd(self, job, jobInfo, changeList, completeList, runningList):
        """
        Process jobs that have classAd info from schedd
        """
        jobAd = jobInfo.get(job['jobid'])
        jobStatus = int(jobAd.get('JobStatus',100))

        statName  = 'Unknown'
        if jobStatus in PyCondorPlugin.exitCodeMap().keys():
            statName = PyCondorPlugin.exitCodeMap()[jobStatus]
            
        if statName == "Unknown" :
            logging.info("JobAdInfo: jobid=%i in unknown state %i" % (job['jobid'], jobStatus))

        # Get the global state
        job['globalState'] = PyCondorPlugin.stateMap()[statName]

        logging.debug("JobAdInfo: JobStatus for jobid=%i is %s" % (job['jobid'],job['status']))
        if statName != job['status']:
            # Then the status has changed
            job['status']      = statName
            job['status_time'] = 0
            logging.debug("JobAdInfo: JobStatus for jobid=%i changed to %s" % (job['jobid'],job['status']))

        #Check if we have a valid status time
        #Do not catch exception here, wait for the next polling cycle
        if not job['status_time']:
            if job['status'] == 'Running':
                job['status_time'] = int(jobAd.get('runningTime', 0))
                
                # Check location for THIS running Job
                job['location'] = jobAd.get('runningCMSSite', None)
                if job['location'] is None:
                    logging.debug('JobAdInfo: Something IS NOT right here, a job (%s) is running with no CMS site' % str(jobAd))

            elif job['status'] == 'Idle':
                job['status_time'] = int(jobAd.get('submitTime', 0))
            else:
                job['status_time'] = int(jobAd.get('stateTime', 0))
                    
            changeList.append(job)

        ## Add the job to Complete list if the status is Removed
        if job['status'] == "Complete" or job['status'] == "Removed" :
            completeList.append(job)
            return

        runningList.append(job)

