#!/usr/bin/env python
"""
_LsfPlugin_

Plugin to support use of LSF queues (at CERN)

Does not support rerunnable jobs, ie. which are
automatically requeued by LSF with a different job id
"""

import os
import re
import time
import socket
import os.path
import logging
import threading
import subprocess


from WMCore.DAOFactory import DAOFactory

from WMCore.WMInit import getWMBASE

from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException

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
        stateDict = {'PEND': 'Pending',
                     'PSUSP': 'Pending',
                     'WAIT': 'Pending',
                     'RUN': 'Running',
                     'USUSP': 'Running',
                     'SSUSP': 'Running',
                     'DONE': 'Complete',
                     'EXIT': 'Error',
                     'UNKWN': 'Error',
                     'ZOMBI': 'Error'}

        return stateDict


    def __init__(self, config):

        self.config = config

        BasePlugin.__init__(self, config)

        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.packageDir = None
        self.unpacker   = os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')
        self.agent      = config.Agent.agentName
        self.sandbox    = None
        self.scriptFile = None

        return


    def submit(self, jobs, info):
        """
        _submit_

        Submit jobs for one subscription

        """
        # If we're here, then we have submitter components
        self.scriptFile = self.config.JobSubmitter.submitScript
        self.queue = self.config.JobSubmitter.LsfPluginQueue
        self.resourceReq =  getattr(self.config.JobSubmitter, 'LsfPluginResourceReq', None)
        self.jobGroup = self.config.JobSubmitter.LsfPluginJobGroup


        successfulJobs = []
        failedJobs     = []

        if len(jobs) == 0:
            # Then we have nothing to do
            return successfulJobs, failedJobs


        # Now assume that what we get is the following; a mostly
        # unordered list of jobs with random sandboxes.
        # We intend to sort them by sandbox.

        submitDict = {}
        for job in jobs:
            sandbox = job['sandbox']
            if not sandbox in submitDict.keys():
                submitDict[sandbox] = []
            submitDict[sandbox].append(job)


        # Now submit the bastards
        for sandbox in submitDict.keys():
            jobList = submitDict.get(sandbox, [])
            while len(jobList) > 0:
                jobsReady = jobList[:self.config.JobSubmitter.jobsPerWorker]
                jobList   = jobList[self.config.JobSubmitter.jobsPerWorker:]

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
                    handle = open(submitScriptFile, 'w')
                    handle.writelines(submitScript)
                    handle.close()

                    # //
                    # // Submit LSF job
                    # //
                    command = 'bsub'
                    command += ' -q %s' % self.queue

                    if self.resourceReq != None:
                        command += ' -R "%s"' % self.resourceReq

                    command += ' -g %s' % self.jobGroup
                    command += ' -J %s' % "WMAgentJob"
                    command += ' -oo /dev/null'
                    #command += ' -oo /afs/cern.ch/user/h/hufnagel/scratch0/logs'
                    command += ' < %s' % submitScriptFile

                    logging.info("Submitting LSF job: %s" % command)

                    p = subprocess.Popen(command, shell = True,
                                         stdout = subprocess.PIPE,
                                         stderr = subprocess.STDOUT)
                    stdout, stderr = p.communicate()
                    returncode = p.returncode

                    if returncode == 0:
                        # check for correct naming convention in PFN
                        regExpParser = re.compile('Job <([0-9]+)> is submitted to queue')
                        match = regExpParser.match(stdout)
                        if match != None:
                            job['gridid'] = match.group(1)
                            successfulJobs.append(job)
                            continue

                    condorErrorReport = Report()
                    condorErrorReport.addError("JobSubmit", 61202, "CondorError", errorMsg)
                    job['fwjr'] = condorErrorReport
                    failedJobs.append(job)
                    
        # We must return a list of jobs successfully submitted,
        # and a list of jobs failed
        return successfulJobs, failedJobs


    def track(self, jobs, info = None):
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

        changeList   = []
        completeList = []
        runningList  = []

        # get info about all active and recent jobs
        command = 'bjobs -a -w'
        command += ' -g %s' % self.jobGroup

        p = subprocess.Popen(command, shell = True,
                             stdout = subprocess.PIPE,
                             stderr = subprocess.PIPE)
        stdout, stderr = p.communicate()
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
                if not jobInfo.has_key(job['gridid']):
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
                    if job['globalState'] in [ 'Complete', 'Error' ]:
                        completeList.append(job)
                    else:
                        runningList.append(job)

        return runningList, changeList, completeList


    def kill(self, jobs, info = None):
        """
        Kill a list of jobs based on their LSF jobid

        """

        for job in jobs:

            command = "bkill %s\n" % job['gridid']
            p = subprocess.Popen(command, shell = True,
                                 stdout = subprocess.PIPE,
                                 stderr = subprocess.STDOUT)
            stdout, stderr = p.communicate()

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
        jobInputFiles =[ job['sandbox'],
                         "%s/JobPackage.pkl" % job['packageDir'],
                         self.unpacker,
                         self.scriptFile ]
        for filename in jobInputFiles:
            script.append("rfcp %s:%s .\n" % (hostname,filename))

        script.append("bash %s %s %s\n" % (os.path.basename(self.scriptFile),
                                           os.path.basename(job['sandbox']),
                                           job['id']))

         #script.append("Going to sleep...\n")
         #script.append("sleep 3600\n")

##         stageHost = os.getenv("STAGE_HOST")
##         if stageHost:
##             script.append("export STAGE_HOST=%s\n" % stageHost)

        script.append("rfcp Report.%i.pkl %s:%s/\n" % (job["retry_count"], hostname, job['cache_dir']))

        # get back a lot of debug information to the head node
        #script.append("find . -type f -name '*.log' -exec rfcp {} %s:%s/ \;\n" % (hostname, job['cache_dir']))

        return script
