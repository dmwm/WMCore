#!/usr/bin/env python
#pylint: disable=W1201
# W1201: Specify string format arguments as logging function parameters

"""
_CondorPlugin_

Example of Condor plugin
For glide-in use.
"""

import os
import re
import time
import Queue
import os.path
import logging
import threading
import traceback
import subprocess
import multiprocessing
import glob
import shlex

import WMCore.Algorithms.BasicAlgos as BasicAlgos

from WMCore.Credential.Proxy           import Proxy
from WMCore.DAOFactory                 import DAOFactory
from WMCore.WMException                import WMException
from WMCore.WMInit                     import getWMBASE
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException
from WMCore.FwkJobReport.Report        import Report
from WMCore.Algorithms                 import SubprocessAlgos

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





class CondorPlugin(BasePlugin):
    """
    _CondorPlugin_

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
        except Exception as ex:
            logging.error(str(ex))
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
            logging.info("Starting up CondorPlugin worker pool")
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
                msg += "Something has gone critically wrong in the worker\n"
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
                                                                     compName = "BossAirCondorPlugin")
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
        logging.info("Done submitting jobs for this cycle in CondorPlugin")
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
        jobInfo = self.getClassAds()
        if jobInfo == None:
            return runningList, changeList, completeList
        if len(jobInfo.keys()) == 0:
            noInfoFlag = True

        for job in jobs:
            # Now go over the jobs from WMBS and see what we have
            if not job['jobid'] in jobInfo.keys():
                # Two options here, either put in removed, or not
                # Only cycle through Removed if condor_q is sending
                # us no information
                if noInfoFlag:
                    if not job['status'] == 'Removed':
                        # If the job is not in removed, move it to removed
                        job['status']      = 'Removed'
                        job['status_time'] = int(time.time())
                        changeList.append(job)
                    elif time.time() - float(job['status_time']) > self.removeTime:
                        # If the job is in removed, and it's been missing for more
                        # then self.removeTime, remove it.
                        completeList.append(job)
                else:
                    completeList.append(job)
            else:
                jobAd     = jobInfo.get(job['jobid'])
                try:
                    # sometimes it returns 'undefined' (probably over high load)
                    jobStatus = int(jobAd.get('JobStatus', 0))
                except ValueError as ex:
                    jobStatus = 0  # unknown
                statName  = 'Unknown'
                if jobStatus == 1:
                    # Job is Idle, waiting for something to happen
                    statName = 'Idle'
                elif jobStatus == 5:
                    # Job is Held; experienced an error
                    statName = 'Held'
                elif jobStatus == 2 or jobStatus == 6:
                    # Job is Running, doing what it was supposed to
                    # NOTE: Status 6 is transferring output
                    # I'm going to list this as running for now because it fits.
                    statName = 'Running'
                elif jobStatus == 3:
                    # Job is in X-state: List as error
                    statName = 'Error'
                elif jobStatus == 4:
                    # Job is completed
                    statName = 'Complete'
                else:
                    # What state are we in?
                    logging.info("Job in unknown state %i" % jobStatus)

                # Get the global state
                job['globalState'] = CondorPlugin.stateMap()[statName]

                if statName != job['status']:
                    # Then the status has changed
                    job['status']      = statName
                    job['status_time'] = 0

                #Check if we have a valid status time
                if not job['status_time']:
                    if job['status'] == 'Running':
                        try:
                            job['status_time'] = int(jobAd.get('runningTime', 0))
                        except ValueError as ex:
                            job['status_time'] = 0
                        # If we transitioned to running then check the site we are running at
                        job['location'] = jobAd.get('runningCMSSite', None)
                        if job['location'] is None:
                            logging.debug('Something is not right here, a job (%s) is running with no CMS site' % str(jobAd))
                    elif job['status'] == 'Idle':
                        try:
                            job['status_time'] = int(jobAd.get('submitTime', 0))
                        except ValueError as ex:
                            job['status_time'] = 0
                    else:
                        try:
                            job['status_time'] = int(jobAd.get('stateTime', 0))
                        except ValueError as ex:
                            job['status_time'] = 0
                    changeList.append(job)

                runningList.append(job)

        return runningList, changeList, completeList


    def complete(self, jobs):
        """
        Do any completion work required

        In this case, look for a returned logfile
        """

        for job in jobs:
            if job.get('cache_dir', None) == None or job.get('retry_count', None) == None:
                # Then we can't do anything
                logging.error("Can't find this job's cache_dir in CondorPlugin.complete")
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
        jobInfo = self.getClassAds()
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
                            command = 'condor_qedit  -constraint \'WMAgent_JobID==%i\' DESIRED_Sites \'"%s"\'' %(jobID, usi)
                            proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                                    stdout = subprocess.PIPE, shell = True)
                            out, err = proc.communicate()
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
                        command = 'condor_qedit  -constraint \'WMAgent_JobID==%i\' DESIRED_Sites \'"%s"\'' %(jobID, usi)
                        proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                                stdout = subprocess.PIPE, shell = True)
                        out, err = proc.communicate()
                    else :
                        #If job doesn't have the siteName in the siteList, just ignore it
                        logging.debug("Cannot find siteName %s in the sitelist" % siteName)

        return jobtokill


    def kill(self, jobs, info=None):
        """
        _kill_

        Kill a list of jobs based on the WMBS job names.
        """
        for job in jobs:
            jobID = job['jobid']
            # This is a very long and painful command to run
            command = 'condor_rm -constraint \"WMAgent_JobID =?= %i\"' % (jobID)
            proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                    stdout = subprocess.PIPE, shell = True)
            out, err = proc.communicate()

        return

    def killWorkflowJobs(self, workflow):
        """
        _killWorkflowJobs_

        Kill all the jobs belonging to a specif workflow.
        """
        command = 'condor_rm -constraint \'WMAgent_RequestName == "%s"\'' % workflow
        proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                stdout = subprocess.PIPE, shell = True)
        out, err = proc.communicate()

        return

    def updateJobInformation(self, workflow, task, **kwargs):
        """
        _updateJobInformation_

        Update job information for all jobs in the workflow and task,
        the change will take effect if the job is Idle or becomes idle.

        The currently supported changes are only priority for which both the task (taskPriority)
        and workflow priority (requestPriority) must be provided.
        """
        if 'taskPriority' in kwargs and 'requestPriority' in kwargs:
            # Do a priority update
            priority = (int(kwargs['requestPriority']) + int(kwargs['taskPriority'] * self.maxTaskPriority))
            command = 'condor_qedit -constraint \'WMAgent_SubTaskName == "%s" && WMAgent_RequestName == "%s" ' %(task, workflow)
            command += '&& (JobPrio != %d)\' JobPrio %d' % (priority, priority)
            command = shlex.split(command)
            proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                    stdout = subprocess.PIPE)
            _, stderr = proc.communicate()
            if proc.returncode != 0:
                # Check if there are actually jobs to update
                command = 'condor_q -constraint \'WMAgent_SubTaskName == "%s" && WMAgent_RequestName == "%s"' %(task, workflow)
                command += ' && (JobPrio != %d)\'' % priority
                command += ' -format \'WMAgentID:\%d:::\' WMAgent_JobID'               
                command = shlex.split(command)
                proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                        stdout = subprocess.PIPE)
                stdout, _ = proc.communicate()
                if stdout != '':
                    msg = 'HTCondor edit failed with exit code %d\n'% proc.returncode
                    msg += 'Error was: %s' % stderr
                    raise BossAirPluginException(msg)

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

        Grab classAds from condor_q using xml parsing
        """

        jobInfo = {}

        command = ['condor_q', '-constraint', 'WMAgent_JobID =!= UNDEFINED',
                   '-constraint', 'WMAgent_AgentName == \"%s\"' % (self.agent),
                   '-format', '(JobStatus:\%s)  ', 'JobStatus',
                   '-format', '(stateTime:\%s)  ', 'EnteredCurrentStatus',
                   '-format', '(runningTime:\%s)  ', 'JobStartDate',
                   '-format', '(submitTime:\%s)  ', 'QDate',
                   '-format', '(DESIRED_Sites:\%s)  ', 'DESIRED_Sites',
                   '-format', '(ExtDESIRED_Sites:\%s)  ', 'ExtDESIRED_Sites',
                   '-format', '(runningCMSSite:\%s)  ', 'MATCH_EXP_JOBGLIDEIN_CMSSite',
                   '-format', '(WMAgentID:\%d):::',  'WMAgent_JobID']

        pipe = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
        stdout, _ = pipe.communicate()
        classAdsRaw = stdout.split(':::')

        if not pipe.returncode == 0:
            # Then things have gotten bad - condor_q is not responding
            logging.error("condor_q returned non-zero value %s" % str(pipe.returncode))
            logging.error("Skipping classAd processing this round")
            return None

        if classAdsRaw == '':
            # We have no jobs
            return jobInfo

        for ad in classAdsRaw:
            # There should be one for every job
            if not re.search("\(", ad):
                # There is no ad.
                # Don't know what happened here
                continue
            statements = ad.split('(')
            tmpDict = {}
            for statement in statements:
                # One for each value
                if not re.search(':', statement):
                    # Then we have an empty statement
                    continue
                key = str(statement.split(':')[0])
                value = statement.split(':')[1].split(')')[0]
                tmpDict[key] = value
            if not 'WMAgentID' in tmpDict.keys():
                # Then we have an invalid job somehow
                logging.error("Invalid job discovered in condor_q")
                logging.error(tmpDict)
                continue
            else:
                jobInfo[int(tmpDict['WMAgentID'])] = tmpDict

        logging.info("Retrieved %i classAds" % len(jobInfo))

        return jobInfo
