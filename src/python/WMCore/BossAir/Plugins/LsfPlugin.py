#!/usr/bin/env python
"""
_LsfPlugin_

Plugin to support use of LSF queues (at CERN)

Does not support rerunnable jobs, ie. which are
automatically requeued by LSF with a different job id
"""

import os
import re
import errno
import time
import datetime
import socket
import logging
import subprocess

from WMCore.WMInit import getWMBASE
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin
from WMCore.FwkJobReport.Report import Report

class LsfPlugin(BasePlugin):
    """
    _LsfPlugin_

    """
    @staticmethod
    def stateMap():
        """
        For a given name, return a global state

        """
        stateDict = {'New':  'Pending',
                     'PEND': 'Pending',
                     'PSUSP': 'Pending',
                     'WAIT': 'Pending',
                     'RUN': 'Running',
                     'USUSP': 'Running',
                     'SSUSP': 'Running',
                     'DONE': 'Complete',
                     'EXIT': 'Error',
                     'UNKWN': 'Error',
                     'ZOMBI': 'Error',
                     'Timeout' : 'Error'}

        return stateDict


    def __init__(self, config):

        self.config = config

        BasePlugin.__init__(self, config)

        self.packageDir = None
        self.unpacker = os.path.join(getWMBASE(),
                                     'WMCore/WMRuntime/Unpacker.py')
        self.agent = config.Agent.agentName
        self.sandbox = None
        self.scriptFile = None
        self.queue = None
        self.resourceReq = None
        self.jobGroup = None
        self.basePrio = getattr(config.BossAir, 'LsfBasePrio', 50)
        self.batchOutput = None

        return


    def submit(self, jobs, info=None):
        """
        _submit_

        Submit jobs for one subscription

        """
        # If we're here, then we have submitter components
        self.scriptFile = self.config.JobSubmitter.submitScript
        self.queue = self.config.JobSubmitter.LsfPluginQueue
        self.resourceReq = getattr(self.config.JobSubmitter, 'LsfPluginResourceReq', None)
        self.jobGroup = self.config.JobSubmitter.LsfPluginJobGroup
        self.batchOutput = getattr(self.config.JobSubmitter, 'LsfPluginBatchOutput', None)

        successfulJobs = []
        failedJobs = []

        if len(jobs) == 0:
            # Then we have nothing to do
            return successfulJobs, failedJobs


        # Now assume that what we get is the following; a mostly
        # unordered list of jobs with random sandboxes.
        # We intend to sort them by sandbox.

        submitDict = {}
        for job in jobs:
            sandbox = job['sandbox']
            if sandbox not in submitDict:
                submitDict[sandbox] = []
            submitDict[sandbox].append(job)


        # Now submit the bastards
        for sandbox in submitDict:
            jobList = submitDict.get(sandbox, [])
            while len(jobList) > 0:
                jobsReady = jobList[:self.config.JobSubmitter.jobsPerWorker]
                jobList = jobList[self.config.JobSubmitter.jobsPerWorker:]

                for job in jobsReady:

                    if job == {}:
                        # Then I don't know how we got here either
                        logging.error("Was passed a nonexistant job.  Ignoring")
                        continue

                    submitScript = self.makeSubmit(job)

                    if not submitScript:
                        # Then we got nothing
                        logging.error("No submit script made!")
                        return {'NoResult': [0]}

                    submitScriptFile = os.path.join(job['cache_dir'], "submit.sh")
                    with open(submitScriptFile, 'w') as handle:
                        handle.writelines(submitScript)

                    # make reasonable job name
                    jobName = "WMAgentJob"
                    regExpParser = re.compile('.*/JobCreator/JobCache/([^/]+)/[^/]+/.*')
                    match = regExpParser.match(job['cache_dir'])
                    if match != None:
                        jobName = "%s-%s" % (match.group(1), job['id'])

                    # //
                    # // Submit LSF job
                    # //
                    command = 'bsub'
                    command += ' -q %s' % self.queue

                    if self.resourceReq != None:
                        command += ' -R "%s"' % self.resourceReq

                    command += ' -g %s' % self.jobGroup
                    command += ' -J %s' % jobName

                    lsfLogDir = self.batchOutput
                    if lsfLogDir != None:
                        now = datetime.datetime.today()
                        lsfLogDir += '/%s' % now.strftime("%Y%m%d%H")
                        try:
                            os.mkdir(lsfLogDir)
                            logging.debug("Created directory %s", lsfLogDir)
                        except OSError as err:
                            # suppress LSF log unless it's about an already exisiting directory
                            if err.errno != errno.EEXIST or not os.path.isdir(lsfLogDir):
                                logging.error("Can't create directory %s, turning off LSF log", lsfLogDir)
                                lsfLogDir = None

                    if lsfLogDir == None:
                        command += ' -oo /dev/null'
                    else:
                        command += ' -oo %s/%s.%%J.out' % (lsfLogDir, jobName)

                    if 'priority' in job:
                        try:
                            prio = int(job['priority'])
                            command += ' -sp %i' % (self.basePrio + prio)
                        except (ValueError, TypeError):
                            logging.debug("Priority for job %i not castable to an int\n", job['id'])
                            logging.debug("Not setting priority")
                            logging.debug("Priority: %s", job['priority'])
                        except Exception as ex:
                            logging.debug("Got unhandled exception while setting priority for job %i\n", job['id'])
                            logging.debug(str(ex))
                            logging.debug("Not setting priority")

                    command += ' < %s' % submitScriptFile

                    logging.info("Submitting LSF job: %s", command)

                    p = subprocess.Popen(command, shell=True,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT)

                    stdout = p.communicate()[0]
                    returncode = p.returncode

                    if returncode == 0:
                        # check for correct naming convention in PFN
                        regExpParser = re.compile('Job <([0-9]+)> is submitted to queue')
                        match = regExpParser.search(stdout)
                        if match != None:
                            job['gridid'] = match.group(1)
                            successfulJobs.append(job)
                            logging.info("LSF Job ID : %s", job['gridid'])
                            continue
                        else:
                            logging.error("bsub didn't return a valid Job ID. Job is not submitted")
                            logging.error(stdout)

                    lsfErrorReport = Report()
                    lsfErrorReport.addError("JobSubmit", 61202, "LsfError", stdout)
                    job['fwjr'] = lsfErrorReport
                    failedJobs.append(job)

        # We must return a list of jobs successfully submitted,
        # and a list of jobs failed
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
        # If we're here, then we have submitter components
        self.jobGroup = self.config.JobSubmitter.LsfPluginJobGroup

        changeList = []
        completeList = []
        runningList = []

        # get info about all active and recent jobs
        command = 'bjobs -a -w'
        command += ' -g %s' % self.jobGroup

        p = subprocess.Popen(command, shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout = p.communicate()[0]
        returncode = p.returncode

        if returncode == 0:

            jobInfo = {}
            for line in stdout.splitlines(False)[1:]:

                # take line apart into elements
                linelist = line.rstrip().split()

                # dict with LSF jobid as key and LSF jobs status as value
                jobInfo[linelist[0]] = linelist[2]

            # now go over the jobs and see what we have
            for job in jobs:

                # if LSF doesn't know anything about the job, mark it complete
                if job['gridid'] not in jobInfo:
                    completeList.append(job)

                # otherwise act on LSF job status
                else:
                    newStatus = jobInfo[job['gridid']]

                    # track status changes
                    if newStatus != job['status']:
                        job['status'] = newStatus
                        job['status_time'] = int(time.time())
                        changeList.append(job)

                    job['globalState'] = LsfPlugin.stateMap()[newStatus]

                    # stop tracking finished jobs
                    if job['globalState'] in ['Complete', 'Error']:
                        completeList.append(job)
                    else:
                        runningList.append(job)

        return runningList, changeList, completeList


    def kill(self, jobs, raiseEx=False):
        """
        Kill a list of jobs based on their LSF jobid

        """

        for job in jobs:

            command = "bkill %s\n" % job['gridid']
            p = subprocess.Popen(command, shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            p.communicate()

        return


    def makeSubmit(self, job):
        """
        _makeSubmit_

        For a given job make a shell script to submit the job

        """

        script = ["#!/bin/sh\n"]

        # needed to construct rfio URL to access head node
        hostname = socket.getfqdn()

        # files needed to copied from head node to WN
        jobInputFiles = [job['sandbox'],
                         "%s/JobPackage.pkl" % job['packageDir'],
                         self.unpacker,
                         self.scriptFile]
        for filename in jobInputFiles:
            script.append("rfcp %s:%s .\n" % (hostname, filename))

        script.append("bash %s %s %s\n" % (os.path.basename(self.scriptFile),
                                           os.path.basename(job['sandbox']),
                                           job['id']))

        script.append("rfcp Report.%i.pkl %s:%s/\n" % (job["retry_count"], hostname, job['cache_dir']))

        # get back a lot of debug information to the head node
        #script.append("find . -type f -name '*.log' -exec rfcp {} %s:%s/ \;\n" % (hostname, job['cache_dir']))

        return script
