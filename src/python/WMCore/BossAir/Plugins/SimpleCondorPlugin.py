#!/usr/bin/env python
"""
_SimpleCondorPlugin_

"""
from __future__ import print_function
from __future__ import division

import os
import os.path
import re
import time
import logging
import threading

import WMCore.Algorithms.BasicAlgos as BasicAlgos

from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit import getWMBASE
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin
from WMCore.FwkJobReport.Report import Report
from WMCore.Credential.Proxy import Proxy
from Utils.IterTools import grouper, convertFromUnicodeToStr

##  python-condor stuff
import htcondor
import classad


class SimpleCondorPlugin(BasePlugin):
    """
    _SimpleCondorPlugin_

    Condor plugin for glide-in submissions
    """

    @staticmethod
    def stateMap():
        """
        For a given name, return a global state
        """
        stateMap = {'1': 'Pending', #Idle
                    '2': 'Running', #Running
                    '3': 'Error', #Removed
                    '4': 'Complete', #Completed
                    '5': 'Running', #Held
                    '6': 'Running', #Transfering output
                    '7': 'Error', #Suspended
                    '100': 'Error'} #Unknown

        return stateMap

    @staticmethod
    def exitCodeMap():
        """
        Exit Codes and their meaing
        https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
        """
        exitCodeMap = {0: "Unknown",
                       1: "Idle",
                       2: "Running",
                       3: "Removed",
                       4: "Complete",
                       5: "Held"}

        return exitCodeMap

    @staticmethod
    def logToScheddExitCodeMap(x):
        """
        JobStatus shows the last status of the job
        Get TriggerEventTypeNumber which is the current status of the job
        Map it back to Schedd Status
        Mapping done using the exit codes from condor website,
        https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
        """
        logExitCode = {0: 1, 1: 1, 2: 0, 3: 2, 4: 3, 5: 4, 6: 2, 7: 0, 8: 0, 9: 4, 10: 0, 11: 1, 12: 5, 13: 2}
        return logExitCode.get(x, 100)

    def __init__(self, config):
        BasePlugin.__init__(self, config)

        self.locationDict = {}

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        self.locationAction = daoFactory(classname="Locations.GetSiteInfo")

        self.packageDir = None

        if os.path.exists(os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')):
            self.unpacker = os.path.join(getWMBASE(),
                                         'src/python/WMCore/WMRuntime/Unpacker.py')
        else:
            self.unpacker = os.path.join(getWMBASE(),
                                         'WMCore/WMRuntime/Unpacker.py')

        self.agent = getattr(config.Agent, 'agentName', 'WMAgent')
        self.sandbox = None

        self.scriptFile = config.JobSubmitter.submitScript

        self.defaultTaskPriority = getattr(config.BossAir, 'defaultTaskPriority', 0)
        self.maxTaskPriority = getattr(config.BossAir, 'maxTaskPriority', 1e7)
        self.jobsPerSubmit = getattr(config.JobSubmitter, 'jobsPerSubmit', 200)

        # Required for global pool accounting
        self.acctGroup = getattr(config.BossAir, 'acctGroup', "production")
        self.acctGroupUser = getattr(config.BossAir, 'acctGroupUser', "cmsdataops")

        # Build a requirement string
        self.reqStr = "stringListMember(GLIDEIN_CMSSite, DESIRED_Sites) && ((REQUIRED_OS=?=\"any\") || (GLIDEIN_REQUIRED_OS=?=REQUIRED_OS)) && (TARGET.Cpus >= RequestCpus)"
        if hasattr(config.BossAir, 'condorRequirementsString'):
            self.reqStr = config.BossAir.condorRequirementsString

        # x509 proxy handling
        proxy = Proxy({'logger': myThread.logger})
        self.x509userproxy = proxy.getProxyFilename()
        self.x509userproxysubject = proxy.getSubject()

        return

    def submit(self, jobs, info=None):
        """
        _submit_


        Submit jobs for one subscription
        """
        successfulJobs = []
        failedJobs = []

        if len(jobs) == 0:
            # Then was have nothing to do
            return successfulJobs, failedJobs

        schedd = htcondor.Schedd()

        # Submit the jobs
        for jobsReady in grouper(jobs, self.jobsPerSubmit):

            cluster_ad = self.getClusterAd()
            proc_ads = self.getProcAds(jobsReady)

            logging.debug("Start: Submitting %d jobs using Condor Python SubmitMany" % len(proc_ads))
            try:
                clusterId = schedd.submitMany(cluster_ad, proc_ads)
            except Exception as ex:
                logging.error("SimpleCondorPlugin job submission failed.")
                logging.error("Moving on the the next batch of jobs and/or cycle....")
                logging.exception(ex)

                condorErrorReport = Report()
                condorErrorReport.addError("JobSubmit", 61202, "CondorError", str(ex))
                for job in jobsReady:
                    job['fwjr'] = condorErrorReport
                    failedJobs.append(job)
            else:
                logging.debug("Finish: Submitting jobs using Condor Python SubmitMany")
                for index,job in enumerate(jobsReady):
                    job['gridid'] = "%s.%s" % (clusterId, index)
                    job['status'] = '1'
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

        changeList = []
        completeList = []
        runningList = []

        # get info about all active and recent jobs
        logging.debug("SimpleCondorPlugin is going to track %s jobs", len(jobs))

        schedd = htcondor.Schedd()

        logging.debug("Start: Retrieving classAds using Condor Python XQuery")
        try:
            itobj = schedd.xquery("WMAgent_AgentName == %s" % classad.quote(self.agent),
                                  ['ClusterId', 'ProcId', 'JobStatus', 'MATCH_EXP_JOBGLIDEIN_CMSSite'])
        except Exception as ex:
            logging.error("Query to condor schedd failed in SimpleCondorPlugin.")
            logging.error("Returning empty lists for all job types...")
            logging.exception(ex)
            return runningList, changeList, completeList
        else:
            logging.debug("Finish: Retrieving classAds using Condor Python XQuery")
            jobInfo = {}
            for jobAd in itobj:
                gridId = "%s.%s" % (jobAd['ClusterId'], jobAd['ProcId'])
                jobStatus = str(jobAd.get('JobStatus', 100))
                if jobStatus not in SimpleCondorPlugin.stateMap():
                    jobStatus = '100'
                location = jobAd.get('MATCH_EXP_JOBGLIDEIN_CMSSite', None)
                jobInfo[gridId] = (jobStatus, location)

            logging.debug("SimpleCondorPlugin retrieved %s classAds from condor schedd", len(jobInfo))

        # now go over the jobs and see what we have
        for job in jobs:

            # if the schedd doesn't know a job, consider it complete
            # doing any further checks is not cost effective
            if job['gridid'] not in jobInfo:
                (newStatus, location) = ('4', None)
            else:
                (newStatus,location) = jobInfo[job['gridid']]

            # check for status changes
            if newStatus != job['status']:

                # update location info for Idle->Running transition
                if newStatus == '2' and job['status'] == '1':
                    if location:
                        job['location'] = location
                        logging.debug("JobAdInfo: Job location for jobid=%i gridid=%s changed to %s", job['jobid'], job['gridid'], location)

                job['status'] = newStatus
                job['status_time'] = int(time.time())
                logging.debug("JobAdInfo: Job status for jobid=%i gridid=%s changed to %s", job['jobid'], job['gridid'], job['status'])
                changeList.append(job)

            job['globalState'] = SimpleCondorPlugin.stateMap()[newStatus]

            # stop tracking finished jobs
            if job['globalState'] in [ 'Complete', 'Error' ]:
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
                logging.error("The job report for job with id %s and gridid %s is a directory", job['id'], job['gridid'])
                logging.error("Ignoring this, but this is very strange")
            else:
                logging.error("No job report for job with id %s and gridid %s", job['id'], job['gridid'])

                if os.path.isfile(reportName):
                    os.remove(reportName)

                # create a report from scratch
                condorReport = Report()
                logOutput = 'Could not find jobReport\n'

                if os.path.isdir(job['cache_dir']):
                    condorOut = "condor.%s.out" % job['gridid']
                    condorErr = "condor.%s.err" % job['gridid']
                    condorLog = "condor.%s.log" % job['gridid']
                    for condorFile in [ condorOut, condorErr, condorLog ]:
                        condorFilePath = os.path.join(job['cache_dir'], condorFile)
                        if os.path.isfile(condorFilePath):
                            logTail = BasicAlgos.tail(condorFilePath, 50)
                            logOutput += 'Adding end of %s to error message:\n' % condorFile
                            logOutput += '\n'.join(logTail)
                    condorReport.addError("NoJobReport", 99303, "NoJobReport", logOutput)
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

        with sd.transaction() as txn:
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
                desiredList = ",".join(desiredList)
                extDesiredList = ",".join(extDesiredList)

                try:
                    sd.edit('DESIRED_Sites =?= %s && ExtDESIRED_Sites =?= %s' % (classad.quote(siteStrings[0]),
                                                                                 classad.quote(siteStrings[1])),
                            "DESIRED_Sites", classad.quote(str(desiredList)))
                    sd.edit('DESIRED_Sites =?= %s && ExtDESIRED_Sites =?= %s' % (classad.quote(siteStrings[0]),
                                                                                 classad.quote(siteStrings[1])),
                            "ExtDESIRED_Sites", classad.quote(str(extDesiredList)))
                except RuntimeError as ex:
                    msg = 'Failed to condor edit job sites. Could be that no jobs were in condor anymore: %s' % str(ex)
                    logging.warning(msg)

        # now update the list of jobs to be killed
        jobtokill = [job for job in jobs if job['id'] in jobIdToKill]

        return jobtokill

    def kill(self, jobs):
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

        return

    def killWorkflowJobs(self, workflow):
        """
        _killWorkflowJobs_

        Kill all the jobs belonging to a specific workflow.
        """
        logging.info("Going to remove all the jobs for workflow %s", workflow)

        schedd = htcondor.Schedd()

        try:
            schedd.act(htcondor.JobAction.Remove, "WMAgent_RequestName == %s" % classad.quote(workflow))
        except RuntimeError:
            logging.warn("Error while killing jobs on the schedd: WMAgent_RequestName=%s", workflow)

        return

    def updateJobInformation(self, workflow, task, **kwargs):
        """
        _updateJobInformation_

        Update job information for all jobs in the workflow and task,
        the change will take effect if the job is Idle or becomes idle.

        The currently supported changes are only priority for which both the task (taskPriority)
        and workflow priority (requestPriority) must be provided.
        """
        schedd = htcondor.Schedd()

        if 'taskPriority' in kwargs and 'requestPriority' in kwargs:
            newPriority = int(kwargs['requestPriority']) + int(kwargs['taskPriority'] * self.maxTaskPriority)
            try:
                constraint = "WMAgent_SubTaskName =?= %s" % classad.quote(task)
                constraint += " && WMAgent_RequestName =?= %s" % classad.quote(workflow)
                constraint += " && JobPrio =!= %d" % newPriority
                schedd.edit(constraint, 'JobPrio', classad.Literal(newPriority))
            except Exception as ex:
                logging.error("Failed to update JobPrio for WMAgent_SubTaskName=%s", task)
                logging.exception(ex)

        return

    def getClusterAd(self):
        """
        _initSubmit_

        Return common cluster classad

        scriptFile & Output/Error/Log filenames shortened to
        avoid condorg submission errors from >256 chars paths

        """
        ad = classad.ClassAd()

        #ad['universe'] = "vanilla"
        ad['Requirements'] = classad.ExprTree(self.reqStr)
        ad['ShouldTransferFiles'] = "YES"
        ad['WhenToTransferOutput'] = "ON_EXIT"
        ad['UserLogUseXML'] = True
        ad['JobNotification'] = 0
        ad['Cmd'] = self.scriptFile
        ad['Out'] = classad.ExprTree('strcat("condor.", ClusterId, ".", ProcId, ".out")')
        ad['Err'] = classad.ExprTree('strcat("condor.", ClusterId, ".", ProcId, ".err")')
        ad['UserLog'] = classad.ExprTree('strcat("condor.", ClusterId, ".", ProcId, ".log")')

        ad['WMAgent_AgentName'] = self.agent

        ad['JOBGLIDEIN_CMSSite'] = classad.ExprTree('isUndefined(GLIDEIN_CMSSite) ? Unknown : GLIDEIN_CMSSite')

        ad['JobLeaseDuration'] = classad.ExprTree('isUndefined(MachineAttrMaxHibernateTime0) ? 1200 : MachineAttrMaxHibernateTime0')

        # Required for global pool accounting
        ad['AcctGroup'] = self.acctGroup
        ad['AcctGroupUser'] = self.acctGroupUser

        # Customized classAds for this plugin
        ad['DESIRED_Archs'] = "INTEL,X86_64"

        ad['x509userproxy'] = self.x509userproxy
        ad['x509userproxysubject'] = self.x509userproxysubject

        ad['Rank'] = 0.0
        ad['TransferIn'] = False

        # TODO: remove when 8.5.7 is deployed
        params_to_add = htcondor.param['SUBMIT_ATTRS'].split() + htcondor.param['SUBMIT_EXPRS'].split()
        params_to_skip = ['accounting_group', 'use_x509userproxy', 'PostJobPrio2', 'JobAdInformationAttrs']
        for param in params_to_add:
            if (param not in ad) and (param in htcondor.param) and (param not in params_to_skip):
                ad[param] = classad.ExprTree(htcondor.param[param])
        #ad = convertFromUnicodeToStr(ad)
        return ad

    def getProcAds(self, jobList):
        """
        _getProcAds_

        Return list of job specific classads for submission

        """
        classAds = []
        for job in jobList:
            ad = classad.ClassAd()

            ad['Iwd'] = job['cache_dir']
            ad['TransferInput'] = "%s,%s/%s,%s" % (job['sandbox'], job['packageDir'],
                                                   'JobPackage.pkl', self.unpacker)
            ad['Arguments'] = "%s %i" % (os.path.basename(job['sandbox']), job['id'])

            ad['TransferOutput'] = "Report.%i.pkl" % job["retry_count"]

            ad['JobMachineAttrs'] = "GLIDEIN_CMSSite"
            ad['JobAdInformationAttrs'] = "JobStatus,QDate,EnteredCurrentStatus,JobStartDate,DESIRED_Sites,ExtDESIRED_Sites,WMAgent_JobID,MATCH_EXP_JOBGLIDEIN_CMSSite"

            sites = ','.join(sorted(job.get('possibleSites')))
            ad['DESIRED_Sites'] = sites

            sites = ','.join(sorted(job.get('potentialSites')))
            ad['ExtDESIRED_Sites'] = sites

            ad['WMAgent_RequestName'] = job['requestName']

            match = re.compile("^[a-zA-Z0-9_]+_([a-zA-Z0-9]+)-").match(job['requestName'])
            if match:
                ad['CMSGroups'] = match.groups()[0]
            else:
                ad['CMSGroups'] = classad.Value.Undefined

            ad['WMAgent_JobID'] = job['jobid']
            ad['WMAgent_SubTaskName'] = job['taskName']
            ad['CMS_JobType'] = job['taskType']

            # Handling for AWS, cloud and opportunistic resources
            ad['AllowOpportunistic'] = job.get('allowOpportunistic', False)

            if job.get('inputDataset'):
                ad['DESIRED_CMSDataset'] = job['inputDataset']
            else:
                ad['DESIRED_CMSDataset'] = classad.Value.Undefined

            if job.get('inputDatasetLocations'):
                sites = ','.join(sorted(job['inputDatasetLocations']))
                ad['DESIRED_CMSDataLocations'] = sites
            else:
                ad['DESIRED_CMSDataLocations'] = classad.Value.Undefined

            # HighIO and repack jobs
            ad['Requestioslots'] = 1 if job['taskType'] in ["Merge", "Cleanup", "LogCollect"] else 0
            ad['RequestRepackslots'] = 1 if job['taskType'] == 'Repack' else 0

            # Performance and resource estimates
            numberOfCores = job.get('numberOfCores', 1)
            ad['RequestCpus'] = numberOfCores
            ad['RequestMemory'] = int(job['estimatedMemoryUsage']) if job.get('estimatedMemoryUsage', None) else 1000
            ad['RequestDisk'] = int(job['estimatedDiskUsage']) if job.get('estimatedDiskUsage', None) else 20*1000*1000*numberOfCores
            ad['MaxWallTimeMins'] = int(job['estimatedJobTime'])/60.0 if job.get('estimatedJobTime', None) else 12*6

            taskPriority = job.get('taskPriority', self.defaultTaskPriority)
            try:
                taskPriority = int(taskPriority)
            except ValueError:
                logging.error("Job taskPriority %s not an int, using default", taskPriority)
                taskPriority = self.defaultTaskPriority

            priority = job.get('priority', 0)
            try:
                priority = int(priority)
            except ValueError:
                logging.error("Job priority %s not an int, using 0", priority)
                priority = 0

            ad['JobPrio'] = int(taskPriority + priority * self.maxTaskPriority)

            postJobPrio1 = -1 * len(job.get('potentialSites', []))
            postJobPrio2 = -1 * job['taskID']

            ad['PostJobPrio1'] = int(postJobPrio1)
            ad['PostJobPrio2'] = int(postJobPrio2)

            # Add OS requirements for jobs
            if job.get('scramArch') is not None and job.get('scramArch').startswith("slc6_"):
                ad['REQUIRED_OS'] = "rhel6"
            else:
                ad['REQUIRED_OS'] = "any"
            
            #ad = convertFromUnicodeToStr(ad)
            classAds.append((ad,1))

        return classAds
