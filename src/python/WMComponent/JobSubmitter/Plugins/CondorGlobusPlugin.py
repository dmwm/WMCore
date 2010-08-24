#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, E1103
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# E1103: The thread will have a logger and a dbi before it gets here

"""
_CondorGlobusPlugin_

A plug-in that should submit directly to condor globus CEs

"""




import os
import os.path
import logging
import threading
import cPickle

# Delete these
import cProfile
import commands

import subprocess

from WMCore.DAOFactory import DAOFactory

from WMCore.WMInit import getWMBASE

from WMComponent.JobSubmitter.Plugins.PluginBase import PluginBase


subprocess._cleanup = lambda: None



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

class CondorGlobusPlugin(PluginBase):
    """
    _CondorGlobusPlugin_
    
    A plug-in that should submit directly to condor globus CEs
    """

    def __init__(self, **configDict):

        PluginBase.__init__(self, config = configDict)
        
        self.config = configDict

        self.locationDict = {}

        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")

        self.packageDir = None
        self.unpacker   = None
        self.sandbox    = None

        return

    def __call__(self, parameters):
        """
        _submitJobs_
        
        If this class actually did something, this would handle submissions
        """

        if parameters == {} or parameters == []:
            return {'NoResult': [0]}

        result = {'Success': []}

        successList = []
        failList    = []

        for entry in parameters:
            jobList         = entry.get('jobs')
            self.packageDir = entry.get('packageDir', None)
            index           = entry.get('index', 0)
            self.sandbox    = entry.get('sandbox', None)
            self.agent      = entry.get('agentName', 'test')
            self.unpacker   = os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')
            
            logging.debug("I have jobs")
            logging.debug(jobList[0])
            
            if type(jobList) == dict:
                # We only got one of them
                # Retain list functionality for possibiity of future multi-jobs
                jobList = [jobList]

            if not os.path.isdir(self.config['submitDir']):
                if not os.path.exists(self.config['submitDir']):
                    os.mkdir(self.config['submitDir'])


            jdlList = self.makeSubmit(jobList, index)
            if not jdlList or jdlList == []:
                # Then we got nothing
                logging.error("No JDL file made!")
                return {'NoResult': [0]}
            jdlFile = "%s/submit_%i.jdl" % (self.config['submitDir'], os.getpid())
            #jdlFile = "%s/submit.jdl" % (self.config['submitDir'])
            handle = open(jdlFile, 'w')
            handle.writelines(jdlList)
            handle.close()


            # Now submit them
            logging.info("About to submit %i jobs" %(len(jobList)))
            command = "condor_submit %s" %(jdlFile)
            pipe = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
            output, error = pipe.communicate()


            if not error == '':
                logging.error("Printing out command stderr")
                logging.error(error)

            errorCheck, errorMsg = parseError(error = error)

                    
            

            if not errorCheck:
                for job in jobList:
                    if job == {}:
                        continue
                    result['Success'].append(job['id'])
                    job['couch_record'] = None
                    successList.append(job)
            else:
                logging.error("JobSubmission failed due to error")
                for job in jobList:
                    if job == {}:
                        continue
                    job['couch_record'] = None
                    failList.append(job)
                


        if len(successList) > 0:
            logging.info("About to pass jobs")
            logging.info(successList)
            self.passJobs(jobList = successList)
        if len(failList) > 0:
            self.failJobs(jobList = failList)


        # We must return a list of jobs successfully submitted,
        # and a list of jobs failed
        return result


    def initSubmit(self):
        """
        _makeConfig_

        Make common JDL header
        """        
        jdl = []


        # -- scriptFile & Output/Error/Log filenames shortened to 
        #    avoid condorg submission errors from > 256 character pathnames
        scriptFile = "%s" % self.config['submitScript']

        
        jdl.append("universe = globus\n")
        jdl.append("requirements = (Memory >= 1 && OpSys == \"LINUX\" ) && (Arch == \"INTEL\" || Arch == \"X86_64\")\n" )
        jdl.append("transfer_output_files = Report.pkl\n")
        jdl.append("transfer_output_remaps = \"Report.pkl = Report.$(Cluster).$(Process).pkl\"\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Executable = %s\n" % scriptFile)
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")
        jdl.append("+WMAgent_AgentName = \"%s\"\n" %(self.agent))
        
        return jdl
    
        
    def makeSubmit(self, jobList, index):
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
            if not job['custom'].has_key('location'):
                # Then we're screwed, because we don't know where to go
                logging.error("Had no location")
                continue
            job['location'] = job['custom'].get('location', None)
            jdl.append("initialdir = %s\n" % job['cache_dir'])
            jdl.append("transfer_input_files = %s, %s/%s, %s\n" \
                       % (self.sandbox, self.packageDir,
                          'JobPackage.pkl', self.unpacker))
            argString = "arguments = %s %i\n" \
                        % (os.path.basename(self.sandbox), job['id'])
            jdl.append(argString)

            jobCE = self.getCEName(jobSite = job['location'])
            if not jobCE:
                # Then we ended up with a site that doesn't exist?
                logging.error("Job for non-existant site %s" \
                              % (job['location']))
                continue
            jdl.append("globusscheduler = %s\n" % (jobCE))

            #jdl.append("+WMAgent_JobName = \"%s\"\n" % job['name'])
            jdl.append("+WMAgent_JobID = %s\n" % job['id'])
        
            jdl.append("Queue 1\n")

            index += 1
        
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
