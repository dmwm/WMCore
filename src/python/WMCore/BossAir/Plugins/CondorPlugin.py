#!/usr/bin/env python
"""
_CondorPlugin_

Example of Condor plugin
For glide-in use.
"""

import os
import re
import time
import os.path
import logging
import threading
import subprocess
import multiprocessing


from WMCore.DAOFactory import DAOFactory

from WMCore.WMInit import getWMBASE

from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException

from WMCore.FwkJobReport.Report import Report


def submitWorker(input, results):
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
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            logging.error(crashMessage)
            break

        if work == 'STOP':
            # Put the brakes on
            break        

        command = work
        pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE, shell = True)
        stdout, stderr = pipe.communicate()
        results.put({'stdout': stdout, 'stderr': stderr})

    return 0


def parseError(error):
    """
    Do some basic condor error parsing

    """

    errorCondition = False
    errorMsg       = ''

    if 'ERROR: proxy has expired\n' in error:
        errorCondition = True
        errorMsg += 'CRITICAL ERROR: Your proxy has expired!'


    return errorCondition, errorMsg


def stateMap(name):
    """
    For a given name, return a global state


    """

    stateDict = {'New': 'Pending',
                 'Idle': 'Pending',
                 'Running': 'Running',
                 'Held': 'Running',
                 'Complete': 'Complete',
                 'Error': 'Error',
                 'Timeout': 'Error'}

    return stateDict.get(name, 'Error')



class CondorPlugin(BasePlugin):
    """
    _CondorPlugin_

    Condor plugin for glide-in submissions
    """

    def __init__(self, config):

        self.config = config

        BasePlugin.__init__(self, config)

        self.states = ['New', 'Running', 'Idle', 'Complete', 'Held', 'Error', 'Timeout']

        self.locationDict = {}

        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")


        self.packageDir = None
        self.unpacker   = self.unpacker   = os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')
        self.agent      = config.Agent.agentName
        self.sandbox    = None
        self.scriptFile = None
        self.submitDir  = None


        # Build ourselves a pool
        self.pool     = []
        self.input    = multiprocessing.Queue()
        self.result   = multiprocessing.Queue()
        self.nProcess = getattr(self.config.BossAir, 'nCondorProcesses', 4)
        


        return


    def __del__(self):
        """
        __del__
        
        Trigger a close of connections if necessary
        """
        self.close()


    def close(self):
        """
        _close_

        Kill all connections and terminate
        """
        terminate = False
        for x in self.pool:
            try:
                self.input.put('STOP')
            except Exception, ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                logging.error(msg)
                terminate = True
        self.input.close()
        self.result.close()
        for proc in self.pool:
            if terminate:
                proc.terminate()
            else:
                proc.join()
        return



    def submit(self, jobs, info):
        """
        _submit_

        
        Submit jobs for one subscription
        """

        if len(self.pool) == 0:
            # Starting things up
            # This is obviously a submit API
            for x in range(self.nProcess):
                p = multiprocessing.Process(target = submitWorker,
                                            args = (self.input, self.result))
                p.start()
                self.pool.append(p)


        # If we're here, then we have submitter components
        self.scriptFile = self.config.JobSubmitter.submitScript
        self.submitDir  = self.config.JobSubmitter.submitDir

        if not os.path.exists(self.submitDir):
            os.makedirs(self.submitDir)


        successfulJobs = []
        failedJobs     = []

        if len(jobs) == 0:
            # Then we have nothing to do
            return successfulJobs, failedJobs



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
        for sandbox in submitDict.keys():
            jobList = submitDict.get(sandbox, [])
            while len(jobList) > 0:
                jobsReady = jobList[:self.config.JobSubmitter.jobsPerWorker]
                jobList   = jobList[self.config.JobSubmitter.jobsPerWorker:]
                jdlList = self.makeSubmit(jobList = jobsReady)
                if not jdlList or jdlList == []:
                    # Then we got nothing
                    logging.error("No JDL file made!")
                    return {'NoResult': [0]}
                jdlFile = "%s/submit_%i.jdl" % (self.submitDir, os.getpid())
                handle = open(jdlFile, 'w')
                handle.writelines(jdlList)
                handle.close()

            
                # Now submit them
                logging.info("About to submit %i jobs" %(len(jobsReady)))
                command = "condor_submit %s" % jdlFile
                self.input.put(command)
                nSubmits += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        for n in range(nSubmits):
            res = self.result.get()
            output = res['stdout']
            error  = res['stderr']

            if not error == '':
                logging.error("Printing out command stderr")
                logging.error(error)
                
            errorCheck, errorMsg = parseError(error = error)

            if errorCheck:
                condorErrorReport = Report()
                condorErrorReport.addError("JobSubmit", 61202, "CondorError", errorMsg)
                for job in jobs:
                    if job == {}:
                        continue
                    job['fwjr'] = condorErrorReport
                    failedJobs.append(job)
            else:        
                for job in jobs:
                    if job == {}:
                        continue
                    successfulJobs.append(job)


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


        # Create an object to store final info
        trackList = []

        changeList   = []
        completeList = []
        runningList  = []

        # Get the job
        jobInfo = self.getClassAds()

        for job in jobs:
            # Now go over the jobs from WMBS and see what we have
            if not job['jobid'] in jobInfo.keys():
                completeList.append(job)
            else:
                jobAd     = jobInfo.get(job['jobid'])
                jobStatus = int(jobAd.get('JobStatus', 0))
                statName  = 'Unknown'
                if jobStatus == 1:
                    # Job is Idle, waiting for something to happen
                    statName = 'Idle'
                elif jobStatus == 5:
                    # Job is Held; experienced an error
                    statName = 'Held'
                elif jobStatus == 2:
                    # Job is Running, doing what it was supposed to
                    statName = 'Running'

                # Get the global state
                job['globalState'] = stateMap(statName)

                if statName != job['status']:
                    # Then the status has changed
                    job['status']      = statName
                    job['status_time'] = jobAd.get('stateTime', 0)
                    changeList.append(job)


                runningList.append(job)

                

        return runningList, changeList, completeList






    def kill(self, jobs, info = None):
        """
        Kill a list of jobs based on the WMBS job names

        """

        for job in jobs:
            jobID = job['jobid']
            # This is a very long and painful command to run
            command = 'condor_rm -constraint \"WMAgent_JobID =?= %i\"' % (jobID)
            proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                    stdout = subprocess.PIPE, shell = True)
            out, err = proc.communicate()

        return





    # Start with submit functions


    def initSubmit(self):
        """
        _makeConfig_

        Make common JDL header
        """        
        jdl = []


        # -- scriptFile & Output/Error/Log filenames shortened to 
        #    avoid condorg submission errors from > 256 character pathnames
        
        jdl.append("universe = vanilla\n")
        jdl.append("requirements = (Memory >= 1 && OpSys == \"LINUX\" ) && (Arch == \"INTEL\" || Arch == \"X86_64\") && stringListMember(GLIDEIN_Site, DESIRED_Sites)\n")
        #jdl.append("should_transfer_executable = TRUE\n")
        jdl.append("transfer_output_files = Report.pkl\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Executable = %s\n" % self.scriptFile)
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")
        # Things that are necessary for the glide-in

        jdl.append('+DESIRED_Archs = \"INTEL,X86_64\"\n')
        jdl.append("+WMAgent_AgentName = \"%s\"\n" %(self.agent))
        
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

        jdl = self.initSubmit()


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

            jobCE = job['location']
            if not jobCE:
                # Then we ended up with a site that doesn't exist?
                logging.error("Job for non-existant site %s" \
                              % (job['location']))
                continue
            jdl.append('+DESIRED_Sites = \"%s\"\n' %(jobCE))

            # Transfer the output files into new names
            jdl.append("transfer_output_remaps = \"Report.pkl = Report.%i.pkl\"\n" \
                       % (job["retry_count"]))

            jdl.append("+WMAgent_JobID = %s\n" % job['jobid'])
        
            jdl.append("Queue 1\n")

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

        constraint = "\"WMAgent_JobID =!= UNDEFINED\""


        jobInfo = {}

        command = ['condor_q', '-constraint', 'WMAgent_JobID =!= UNDEFINED',
                   '-constraint', 'WMAgent_AgentName == \"%s\"' % (self.agent),
                   '-format', '(JobStatus:\%s)  ', 'JobStatus',
                   '-format', '(stateTime:\%s)  ', 'EnteredCurrentStatus',
                   '-format', '(WMAgentID:\%d):::',  'WMAgent_JobID']

        pipe = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
        stdout, stderr = pipe.communicate()
        classAdsRaw = stdout.split(':::')
        

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

