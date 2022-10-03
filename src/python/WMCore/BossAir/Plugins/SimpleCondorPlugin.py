#!/usr/bin/env python
"""
_SimpleCondorPlugin_

"""
import logging
import os
import os.path
import re
import threading
import time
import classad
import htcondor

from Utils import FileTools
from Utils.IteratorTools import grouper
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin
from WMCore.Credential.Proxy import Proxy
from WMCore.DAOFactory import DAOFactory
from WMCore.FwkJobReport.Report import Report
from WMCore.WMInit import getWMBASE
from WMCore.Lexicon import getIterMatchObjectOnRegexp, WMEXCEPTION_REGEXP, CONDOR_LOG_FILTER_REGEXP


def activityToType(jobActivity):
    """
    Function to map a workflow activity to a generic CMS job type.
    :param jobActivity: a workflow activity string
    :return: string which it maps to

    NOTE: this map is based on the Lexicon.activity list
    """
    activityMap = {"reprocessing": "production",
                   "production": "production",
                   "relval": "production",
                   "harvesting": "production",
                   "storeresults": "production",
                   "tier0": "tier0",
                   "t0": "tier0",
                   "integration": "test",
                   "test": "test"}
    return activityMap.get(jobActivity, "unknown")


class SimpleCondorPlugin(BasePlugin):
    """
    _SimpleCondorPlugin_

    Condor plugin for glide-in submissions
    """

    @staticmethod
    def stateMap():
        """
        For a given condor status mapped, return a global state used by the agent
        NOTE: these keys are populated into bl_status table.
        NOTE2: Timeout must be present in all future plugins (New is recommended too)
        """
        stateMap = {'New': 'Pending',
                    'Idle': 'Pending',
                    'Running': 'Running',
                    'Removed': 'Error',
                    'Completed': 'Complete',
                    'Held': 'Error',
                    'TransferOutput': 'Running',
                    'Suspended': 'Error',
                    'Timeout': 'Error',
                    'Unknown': 'Error'}

        return stateMap

    @staticmethod
    def exitCodeMap():
        """
        HTCondor mapping from the numerical value to its english meaning.
        https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
        """
        exitCodeMap = {0: "Unknown",
                       1: "Idle",
                       2: "Running",
                       3: "Removed",
                       4: "Completed",
                       5: "Held",
                       6: "TransferOutput",
                       7: "Suspended"}

        return exitCodeMap

    def __init__(self, config):
        BasePlugin.__init__(self, config)

        self.locationDict = {}

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        self.locationAction = daoFactory(classname="Locations.GetSiteInfo")

        self.packageDir = None

        # if agent is running in a container, Unpacker.py must come from a directory
        # on the host so the condor schedd can see it
        # config.General.workDir should always be bind mounted to the container
        if getattr(config.Agent, "isDocker", False):
            unpackerPath = os.path.join(config.General.workDir + "/Docker/WMRuntime/Unpacker.py")
        else:
            unpackerPath = os.path.join(getWMBASE(), 'src/python/WMCore/WMRuntime/Unpacker.py')

        if os.path.exists(unpackerPath):
            self.unpacker = unpackerPath
        else:
            self.unpacker = os.path.join(getWMBASE(),
                                         'WMCore/WMRuntime/Unpacker.py')

        self.agent = getattr(config.Agent, 'agentName', 'WMAgent')
        self.sandbox = None

        self.scriptFile = config.JobSubmitter.submitScript

        self.defaultTaskPriority = getattr(config.BossAir, 'defaultTaskPriority', 0)
        self.maxTaskPriority = getattr(config.BossAir, 'maxTaskPriority', 1e7)
        self.jobsPerSubmit = getattr(config.JobSubmitter, 'jobsPerSubmit', 200)
        self.extraMem = getattr(config.JobSubmitter, 'extraMemoryPerCore', 500)

        # Required for global pool accounting
        self.acctGroup = getattr(config.BossAir, 'acctGroup', "production")
        self.acctGroupUser = getattr(config.BossAir, 'acctGroupUser', "cmsdataops")
 
        if hasattr(config.BossAir, 'condorRequirementsString'):
            self.reqStr = config.BossAir.condorRequirementsString
        else:
            self.reqStr = None

        # x509 proxy handling
        proxy = Proxy({'logger': myThread.logger})
        self.x509userproxy = proxy.getProxyFilename()

        # These are added now by the condor client
        #self.x509userproxysubject = proxy.getSubject()
        #self.x509userproxyfqan = proxy.getAttributeFromProxy(self.x509userproxy)

        return

    def submit(self, jobs, info=None):
        """
        _submit_

        Submits jobs to the condor queue
        """
        successfulJobs = []
        failedJobs = []

        if len(jobs) == 0:
            # Then was have nothing to do
            return successfulJobs, failedJobs

        schedd = htcondor.Schedd()

        # Submit the jobs
        for jobsReady in grouper(jobs, self.jobsPerSubmit):

            (sub, jobParams) = self.createSubmitRequest(jobsReady)

            logging.debug("Start: Submitting %d jobs using Condor Python Submit", len(jobParams))
            try:
                with schedd.transaction() as txn:
                    submitRes = sub.queue_with_itemdata(txn, 1, iter(jobParams))
                    clusterId = submitRes.cluster()
            except Exception as ex:
                logging.error("SimpleCondorPlugin job submission failed.")
                logging.exception(str(ex))
                logging.error("Moving on the the next batch of jobs and/or cycle....")

                condorErrorReport = Report()
                condorErrorReport.addError("JobSubmit", 61202, "CondorError", str(ex))
                for job in jobsReady:
                    job['fwjr'] = condorErrorReport
                    failedJobs.append(job)
            else:
                logging.debug("Job submission to condor succeeded, clusterId is %s", clusterId)
                for index, job in enumerate(jobsReady):
                    job['gridid'] = "%s.%s" % (clusterId, index)
                    job['status'] = 'Idle'
                    successfulJobs.append(job)

        # We must return a list of jobs successfully submitted and a list of jobs failed
        logging.info("Done submitting jobs for this cycle in SimpleCondorPlugin")
        return successfulJobs, failedJobs

    def track(self, jobs):
        """
        _track_

        Track the jobs while in condor
        This returns a three-way ntuple
        First, the total number of jobs still running
        Second, the jobs that need to be changed
        Third, the jobs that need to be completed
        """
        jobInfo = {}
        changeList = []
        completeList = []
        runningList = []

        # get info about all active and recent jobs
        logging.debug("SimpleCondorPlugin is going to track %s jobs", len(jobs))

        schedd = htcondor.Schedd()

        logging.debug("Start: Retrieving classAds using Condor Python XQuery")
        try:
            itobj = schedd.xquery("WMAgent_AgentName == %s" % classad.quote(self.agent),
                                  ['ClusterId', 'ProcId', 'JobStatus', 'MachineAttrGLIDEIN_CMSSite0'])
            for jobAd in itobj:
                gridId = "%s.%s" % (jobAd['ClusterId'], jobAd['ProcId'])
                jobStatus = SimpleCondorPlugin.exitCodeMap().get(jobAd.get('JobStatus'), 'Unknown')
                location = jobAd.get('MachineAttrGLIDEIN_CMSSite0', None)
                jobInfo[gridId] = (jobStatus, location)
        except Exception as ex:
            logging.error("Query to condor schedd failed in SimpleCondorPlugin.")
            logging.error("Returning empty lists for all job types...")
            logging.exception(ex)
            return runningList, changeList, completeList

        logging.debug("Finished retrieving %d classAds from Condor", len(jobInfo))

        # now go over the jobs and see what we have
        for job in jobs:

            # if the schedd doesn't know a job, consider it complete
            # doing any further checks is not cost effective
            if job['gridid'] not in jobInfo:
                (newStatus, location) = ('Completed', None)
            else:
                (newStatus, location) = jobInfo[job['gridid']]

            # check for status changes
            if newStatus != job['status']:

                # update location info for Idle->Running transition
                if newStatus == 'Running' and job['status'] == 'Idle':
                    if location:
                        job['location'] = location
                        logging.debug("JobAdInfo: Job location for jobid=%i gridid=%s changed to %s", job['jobid'],
                                      job['gridid'], location)

                job['status'] = newStatus
                job['status_time'] = int(time.time())
                logging.debug("JobAdInfo: Job status for jobid=%i gridid=%s changed to %s", job['jobid'], job['gridid'],
                              job['status'])
                changeList.append(job)

            job['globalState'] = SimpleCondorPlugin.stateMap().get(newStatus)

            # stop tracking finished jobs
            if job['globalState'] in ['Complete', 'Error']:
                completeList.append(job)
            else:
                runningList.append(job)

        logging.debug("SimpleCondorPlugin tracking : %i/%i/%i (Executing/Changing/Complete)",
                      len(runningList), len(changeList), len(completeList))

        return runningList, changeList, completeList

    def complete(self, jobs):
        """
        Do any completion work required

        In this case, look for a returned logfile
        """

        for job in jobs:

            if job.get('cache_dir', None) is None or job.get('retry_count', None) is None:
                # Then we can't do anything
                logging.error("Can't find this job's cache_dir or retry count: %s", job)
                continue

            reportName = os.path.join(job['cache_dir'], 'Report.%i.pkl' % job['retry_count'])
            if os.path.isfile(reportName) and os.path.getsize(reportName) > 0:
                # everything in order, move on
                continue
            elif os.path.isdir(reportName):
                # Then something weird has happened. Report error, do nothing
                logging.error("The job report for job with id %s and gridid %s is a directory", job['id'],
                              job['gridid'])
                logging.error("Ignoring this, but this is very strange")
            else:
                logging.error("No job report for job with id %s and gridid %s", job['id'], job['gridid'])

                if os.path.isfile(reportName):
                    os.remove(reportName)

                # create a report from scratch
                condorReport = Report()
                logOutput = 'Could not find jobReport\n'

                if os.path.isdir(job['cache_dir']):
                    condorErr = "condor.%s.err" % job['gridid']
                    condorOut = "condor.%s.out" % job['gridid']
                    condorLog = "condor.%s.log" % job['gridid']
                    exitCode = 99303
                    exitType = "NoJobReport"
                    for condorFile in [condorErr, condorOut, condorLog]:
                        condorFilePath = os.path.join(job['cache_dir'], condorFile)
                        logOutput += "\n========== %s ==========\n" % condorFile
                        if os.path.isfile(condorFilePath):
                            logTail = FileTools.tail(condorFilePath, 50)
                            logOutput += 'Adding end of %s to error message:\n\n' % condorFile
                            logOutput += logTail
                            logOutput += '\n\n'

                            if condorFile == condorLog:
                                # for condor log, search for the information
                                for matchObj in getIterMatchObjectOnRegexp(condorFilePath, CONDOR_LOG_FILTER_REGEXP):
                                    condorReason = matchObj.group("Reason")
                                    if condorReason:
                                        logOutput += condorReason
                                        if "SYSTEM_PERIODIC_REMOVE" in condorReason or "via condor_rm" in condorReason:
                                            exitCode = 99400
                                            exitType = "RemovedByGlideinOrPeriodicExpr"
                                        else:
                                            exitCode = 99401

                                    siteName = matchObj.group("Site")
                                    if siteName:
                                        condorReport.data.siteName = siteName
                                    else:
                                        condorReport.data.siteName = "NoReportedSite"
                            else:
                                for matchObj in getIterMatchObjectOnRegexp(condorFilePath, WMEXCEPTION_REGEXP):
                                    errMsg = matchObj.group('WMException')
                                    if errMsg:
                                        logOutput += "\n\n%s\n" % errMsg

                                    errMsg = matchObj.group('ERROR')
                                    if errMsg:
                                        logOutput += "\n\n%s\n" % errMsg

                    logOutput += '\n\n'
                    condorReport.addError(exitType, exitCode, exitType, logOutput)
                else:
                    msg = "Serious Error in Completing condor job with id %s!\n" % job['id']
                    msg += "Could not find jobCache directory %s\n" % job['cache_dir']
                    msg += "Creating a new cache_dir for failed job report\n"
                    logging.error(msg)
                    os.makedirs(job['cache_dir'])
                    condorReport.addError("NoJobReport", 99304, "NoCacheDir", logOutput)

                condorReport.save(filename=reportName)

                logging.debug("Created failed job report for job with id %s and gridid %s", job['id'], job['gridid'])

        return

    def updateSiteInformation(self, jobs, siteName, excludeSite):
        """
        _updateSiteInformation_

        Allow or disallow jobs to run at a site.
        Called externally by Ops scripts if a site enters or leaves Down, Draining or Aborted.

        Kill job if after removing site from allowed sites it has nowhere to run.

        Parameters:    excludeSite = False when moving to Normal
                       excludeSite = True when moving to Down, Draining or Aborted
        """
        sd = htcondor.Schedd()
        jobIdToKill = []
        jobtokill = []
        origSiteLists = set()

        try:
            itobj = sd.xquery('WMAgent_AgentName =?= %s && JobStatus =?= 1' % classad.quote(self.agent),
                              ['WMAgent_JobID', 'DESIRED_Sites', 'ExtDESIRED_Sites'])

            for jobAd in itobj:
                jobAdId = jobAd.get('WMAgent_JobID')
                desiredSites = jobAd.get('DESIRED_Sites')
                extDesiredSites = jobAd.get('ExtDESIRED_Sites')
                if excludeSite and siteName == desiredSites:
                    jobIdToKill.append(jobAdId)
                else:
                    origSiteLists.add((desiredSites, extDesiredSites))
            logging.info("Set of %d site list condor combinations", len(origSiteLists))
        except Exception as ex:
            msg = "Failed to query condor schedd: %s" % str(ex)
            logging.exception(msg)
            return jobtokill

        with sd.transaction() as dummyTxn:
            for siteStrings in origSiteLists:
                desiredList = set([site.strip() for site in siteStrings[0].split(",")])
                extDesiredList = set([site.strip() for site in siteStrings[1].split(",")])

                if excludeSite and siteName not in desiredList:
                    continue
                elif not excludeSite and (siteName in desiredList or siteName not in extDesiredList):
                    continue
                elif excludeSite:
                    desiredList.remove(siteName)
                    extDesiredList.add(siteName)
                else:  # well, then include
                    desiredList.add(siteName)
                    extDesiredList.remove(siteName)

                # now put it back in the string format expected by condor
                desiredListStr = ",".join(desiredList)
                extDesiredListStr = ",".join(extDesiredList)

                try:
                    sd.edit('DESIRED_Sites =?= %s && ExtDESIRED_Sites =?= %s' % (classad.quote(siteStrings[0]),
                                                                                 classad.quote(siteStrings[1])),
                            "DESIRED_Sites", classad.quote(str(desiredListStr)))
                    sd.edit('DESIRED_Sites =?= %s && ExtDESIRED_Sites =?= %s' % (classad.quote(siteStrings[0]),
                                                                                 classad.quote(siteStrings[1])),
                            "ExtDESIRED_Sites", classad.quote(str(extDesiredListStr)))
                except RuntimeError as ex:
                    msg = 'Failed to condor edit job sites. Could be that no jobs were in condor anymore: %s' % str(ex)
                    logging.warning(msg)

        # now update the list of jobs to be killed
        jobtokill = [job for job in jobs if job['id'] in jobIdToKill]

        return jobtokill

    def kill(self, jobs, raiseEx=False):
        """
        _kill_

        Kill a list of jobs based on the WMBS job names.
        Kill can happen for schedd running on localhost... TBC.
        """
        logging.info("Killing %i jobs from the queue", len(jobs))

        schedd = htcondor.Schedd()
        gridIds = [job['gridid'] for job in jobs]
        try:
            schedd.act(htcondor.JobAction.Remove, gridIds)
        except RuntimeError:
            logging.warn("Error while killing jobs on the schedd: %s", gridIds)
            if raiseEx:
                raise

        return

    def killWorkflowJobs(self, workflow):
        """
        _killWorkflowJobs_

        Kill all the jobs belonging to a specific workflow.
        """
        logging.info("Going to remove all the jobs for workflow %s", workflow)

        schedd = htcondor.Schedd()

        try:
            schedd.act(htcondor.JobAction.Remove, "WMAgent_RequestName == %s" % classad.quote(str(workflow)))
        except RuntimeError:
            logging.warn("Error while killing jobs on the schedd: WMAgent_RequestName=%s", workflow)

        return

    def updateJobInformation(self, workflow, **kwargs):
        """
        _updateJobInformation_

        Update job information for all jobs in the workflow and task,
        the change will take effect if the job is Idle or becomes idle.

        The currently supported changes are only priority for which both the task (taskPriority)
        and workflow priority (requestPriority) must be provided.

        Since the default priority is very high, we only need to adjust new priorities
        for processing/production task types (which have a task priority of 0)
        """
        schedd = htcondor.Schedd()

        if 'requestPriority' in kwargs:
            newPriority = int(kwargs['requestPriority'])
            try:
                constraint = "WMAgent_RequestName =?= %s" % classad.quote(str(workflow))
                constraint += " && JobPrio =!= %d" % newPriority
                constraint += " && stringListMember(CMS_JobType, %s) " % classad.quote(str("Production, Processing"))
                schedd.edit(constraint, 'JobPrio', classad.Literal(newPriority))
            except Exception as ex:
                logging.error("Failed to update JobPrio for WMAgent_RequestName=%s", str(workflow))
                logging.exception(ex)

        return


    def getJobParameters(self, jobList):
        """
        _getJobParameters_

        Return a list of dictionaries with submit parameters per job.
        """

        undefined = 'UNDEFINED'
        jobParameters = []

        for job in jobList:
            ad = {}

            ad['initial_Dir'] = job['cache_dir']
            ad['transfer_input_files'] = "%s,%s/%s,%s" % (job['sandbox'], job['packageDir'],
                                                   'JobPackage.pkl', self.unpacker)
            ad['Arguments'] = "%s %i %s" % (os.path.basename(job['sandbox']), job['id'], job["retry_count"])
            ad['transfer_output_files'] = "Report.%i.pkl,wmagentJob.log" % job["retry_count"]

            # Dictionary keys need to be consistent across all jobs within the same 
            # clusterId when working with queue_with_itemdata()
            # Initialize 'Requirements' to an empty string for all jobs.
            # See issue: https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=7715 
            ad['Requirements'] = ''
            # Do not define custom Requirements for Volunteer resources
            if self.reqStr is not None:
                ad['Requirements'] = self.reqStr

            ad['My.x509userproxy'] = classad.quote(self.x509userproxy)
            sites = ','.join(sorted(job.get('possibleSites')))
            ad['My.DESIRED_Sites'] = classad.quote(str(sites))
            sites = ','.join(sorted(job.get('potentialSites')))
            ad['My.ExtDESIRED_Sites'] = classad.quote(str(sites))
            ad['My.CMS_JobRetryCount'] = str(job['retry_count'])
            ad['My.WMAgent_RequestName'] = classad.quote(job['request_name'])
            match = re.compile("^[a-zA-Z0-9_]+_([a-zA-Z0-9]+)-").match(job['request_name'])
            if match:
                ad['My.CMSGroups'] = classad.quote(match.groups()[0])
            else:
                ad['My.CMSGroups'] = undefined
            ad['My.WMAgent_JobID'] = str(job['jobid'])
            ad['My.WMAgent_SubTaskName'] = classad.quote(job['task_name'])
            ad['My.CMS_JobType'] = classad.quote(job['task_type'])
            ad['My.CMS_Type'] = classad.quote(activityToType(job['activity']))
            ad['My.CMS_RequestType'] = classad.quote(job['requestType'])

            # Handling for AWS, cloud and opportunistic resources
            ad['My.AllowOpportunistic'] = str(job.get('allowOpportunistic', False))
            if job.get('inputDataset'):
                ad['My.DESIRED_CMSDataset'] = classad.quote(job['inputDataset'])
            else:
                ad['My.DESIRED_CMSDataset'] = undefined
            if job.get('inputDatasetLocations'):
                sites = ','.join(sorted(job['inputDatasetLocations']))
                ad['My.DESIRED_CMSDataLocations'] = classad.quote(str(sites))
            else:
                ad['My.DESIRED_CMSDataLocations'] = undefined
            if job.get('inputPileup'):
                cmsPileups=','.join(sorted(job['inputPileup']))
                ad['My.DESIRED_CMSPileups'] = classad.quote(str(cmsPileups))
            else:
                ad['My.DESIRED_CMSPileups'] = undefined
            # HighIO
            ad['My.Requestioslots'] = str(1 if job['task_type'] in ["Merge", "Cleanup", "LogCollect"] else 0)
            # GPU resource handling
            # while we do not support a third option for RequiresGPU, make a binary decision
            if job['requiresGPU'] == "required":
                ad['My.RequiresGPU'] = "1"
                ad['request_GPUs'] = "1"
            else:
                ad['My.RequiresGPU'] = "0"
                ad['request_GPUs'] = "0"
            if job.get('gpuRequirements', None):
                ad['My.GPUMemoryMB'] = str(job['gpuRequirements']['GPUMemoryMB'])
                cudaCapabilities = ','.join(sorted(job['gpuRequirements']['CUDACapabilities']))
                ad['My.CUDACapability'] = classad.quote(str(cudaCapabilities))
                ad['My.CUDARuntime'] = classad.quote(job['gpuRequirements']['CUDARuntime'])
            else:
                ad['My.GPUMemoryMB'] = undefined
                ad['My.CUDACapability'] = undefined
                ad['My.CUDARuntime'] = undefined
            # Performance and resource estimates (including JDL magic tweaks)
            origCores = job.get('numberOfCores', 1)
            estimatedMins = int(job['estimatedJobTime'] / 60.0) if job.get('estimatedJobTime') else 12 * 60
            estimatedMinsSingleCore = estimatedMins * origCores
            # For now, assume a 15 minute job startup overhead -- condor will round this up further
            ad['My.EstimatedSingleCoreMins'] = str(estimatedMinsSingleCore)
            ad['My.OriginalMaxWallTimeMins'] = str(estimatedMins)
            ad['My.MaxWallTimeMins'] = 'WMCore_ResizeJob ? (EstimatedSingleCoreMins/RequestCpus + 15) : OriginalMaxWallTimeMins'
            requestMemory = int(job['estimatedMemoryUsage']) if job.get('estimatedMemoryUsage', None) else 1000
            ad['My.OriginalMemory'] = str(requestMemory)
            ad['My.ExtraMemory'] = str(self.extraMem)
            ad['request_memory'] = 'OriginalMemory + ExtraMemory * (WMCore_ResizeJob ? (RequestCpus-OriginalCpus) : 0)'
            requestDisk = int(job['estimatedDiskUsage']) if job.get('estimatedDiskUsage', None) else 20 * 1000 * 1000 * origCores
            ad['request_disk'] = str(requestDisk)
            # Set up JDL for multithreaded jobs.
            # By default, RequestCpus will evaluate to whatever CPU request was in the workflow.
            # If the job is labelled as resizable, then the logic is more complex:
            # - If the job is running in a slot with N cores, this should evaluate to N
            # - If the job is being matched against a machine, match all available CPUs, provided
            # they are between min and max CPUs.
            # - Otherwise, just use the original CPU count.
            ad['My.MinCores'] = str(job.get('minCores', max(1, origCores / 2)))
            ad['My.MaxCores'] = str(max(int(job.get('maxCores', origCores)), origCores))
            ad['My.OriginalCpus'] = str(origCores)
            # Prefer slots that are closest to our MaxCores without going over.
            # If the slot size is _greater_ than our MaxCores, we prefer not to
            # use it - we might unnecessarily fragment the slot.
            ad['Rank'] = 'isUndefined(Cpus) ? 0 : ifThenElse(Cpus > MaxCores, -Cpus, Cpus)'
            # Record the number of CPUs utilized at match time.  We'll use this later
            # for monitoring and accounting.  Defaults to 0; once matched, it'll
            # put an attribute in the job  MATCH_EXP_JOB_GLIDEIN_Cpus = 4
            ad['My.JOB_GLIDEIN_Cpus'] = classad.quote("$$(Cpus:0)")
            # Make sure the resize request stays within MinCores and MaxCores.
            ad['My.RequestResizedCpus'] = '(Cpus>MaxCores) ? MaxCores : ((Cpus < MinCores) ? MinCores : Cpus)'
            # If the job is running, then we should report the matched CPUs in RequestCpus - but only if there are sane
            # values.  Otherwise, we just report the original CPU request
            ad['My.JobCpus'] = ('((JobStatus =!= 1) && (JobStatus =!= 5) && !isUndefined(MATCH_EXP_JOB_GLIDEIN_Cpus) '
                              '&& (int(MATCH_EXP_JOB_GLIDEIN_Cpus) isnt error)) ? int(MATCH_EXP_JOB_GLIDEIN_Cpus) : OriginalCpus')
            # Cpus is taken from the machine ad - hence it is only defined when we are doing negotiation.
            # Otherwise, we use either the cores in the running job (if available) or the original cores.
            ad['request_cpus'] = 'WMCore_ResizeJob ? (!isUndefined(Cpus) ? RequestResizedCpus : JobCpus) : OriginalCpus'
            ad['My.WMCore_ResizeJob'] = str(job.get('resizeJob', False))
            taskPriority = int(job.get('taskPriority', 1))
            priority = int(job.get('wf_priority', 0))
            ad['My.JobPrio'] = str(int(priority + taskPriority * self.maxTaskPriority))
            ad['My.PostJobPrio1'] = str(int(-1 * len(job.get('potentialSites', []))))
            ad['My.PostJobPrio2'] = str(int(-1 * job['task_id']))
            # Add OS requirements for jobs
            requiredOSes = self.scramArchtoRequiredOS(job.get('scramArch'))
            ad['My.REQUIRED_OS'] = classad.quote(requiredOSes)
            cmsswVersions = ','.join(job.get('swVersion'))
            ad['My.CMSSW_Versions'] = classad.quote(cmsswVersions)
            requiredArchs = self.scramArchtoRequiredArch(job.get('scramArch'))
            if not requiredArchs:  # only Cleanup jobs should not have ScramArch defined
                ad['My.REQUIRED_ARCH'] = undefined
                ad['Requirements'] = '(TARGET.Arch =!= REQUIRED_ARCH)'
            else:
                ad['My.REQUIRED_ARCH'] = classad.quote(str(requiredArchs))
                ad['Requirements'] = 'stringListMember(TARGET.Arch, REQUIRED_ARCH)'

            jobParameters.append(ad)    
             
        return jobParameters


    def createSubmitRequest(self, jobList):
        """
        _createSubmitRequest_

        Return the submit object to pass to htcondor.Submit()

        """

        sub = htcondor.Submit("""
            universe = vanilla
            should_transfer_files = YES
            when_to_transfer_output = ON_EXIT
            notification = NEVER
            log_xml = True
            rank = 0.0
            transfer_input = False
            Output = condor.$(Cluster).$(Process).out
            Error = condor.$(Cluster).$(Process).err
            Log = condor.$(Cluster).$(Process).log
            +JobLeaseDuration = (isUndefined(MachineAttrMaxHibernateTime0) ? 1200 : MachineAttrMaxHibernateTime0)
            +PeriodicRemove = ( JobStatus =?= 5 ) && ( time() - EnteredCurrentStatus > 10 * 60 )
            +PeriodicRemoveReason = PeriodicRemove ? "Job automatically removed for being in Held status" : ""
            """)
        sub['executable'] = self.scriptFile

        # Required for global pool accounting
        sub['My.WMAgent_AgentName'] = classad.quote(self.agent)
        sub['accounting_group'] = self.acctGroup
        sub['accounting_group_user'] = self.acctGroupUser
        sub['My.JobMachineAttrs'] = classad.quote("GLIDEIN_CMSSite,GLIDEIN_Gatekeeper")
        sub['My.JobAdInformationAttrs'] = classad.quote("JobStatus,QDate,EnteredCurrentStatus,JobStartDate,DESIRED_Sites,ExtDESIRED_Sites,WMAgent_JobID,MachineAttrGLIDEIN_CMSSite0,MyType")

        # entries required for monitoring
        sub['My.CMS_WMTool'] = classad.quote("WMAgent")
        sub['My.CMS_SubmissionTool'] = classad.quote("WMAgent")

        jobParameters = self.getJobParameters(jobList)

        return sub, jobParameters
