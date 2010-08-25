#!/usr/bin/env python

"""
_BasicPlugin_

A test plugin just to test the jobSubmitter class
"""

import os
import os.path
import logging
import threading

from subprocess import Popen, PIPE

from WMComponent.JobSubmitter.Plugins.PluginBase import PluginBase

class TestPlugin(PluginBase):

    def __init__(self, **configDict):
        self.config = configDict

    def __call__(self, parameters):
        """
        _submitJobs_
        
        If this class actually did something, this would handle submissions
        """

        jobList = parameters

        if parameters == {} or parameters == []:
            return {'NoResult': [0]}

        if type(jobList) == dict:
            #We only got one of them
            #Retain list functionality for possibiity of future multi-jobs
            jobList = [jobList]





        myThread = threading.currentThread()

        if not os.path.isdir(self.config['submitDir']):
            if not os.path.exists(self.config['submitDir']):
                os.mkdir(self.config['submitDir'])


        baseConfig = self.initSubmit()

        jobSubmitFiles = []
        for job in jobList:
            logging.error('Have job separated out')
            logging.error(job)
            if job == {}:
                continue
            tmpList = []
            tmpList.extend(baseConfig)
            tmpList.extend(self.makeSubmit(job))
            jdlFile = "%s/submit_%i.jdl" %(self.config['submitDir'], job['id'])
            handle = open(jdlFile, 'w')
            handle.writelines(tmpList)
            handle.close()

            jobSubmitFiles.append(jdlFile)

        #Now submit them

        for submit in jobSubmitFiles:
            #submit = jobSubmitFiles[0]
            command = ["condor_submit", submit]
            pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
            pipe.wait()
            logging.error("I have submitted a job to condor")


        result = {'Success': []}

        for job in jobList:
            if job == {}:
                continue
            result['Success'].append(job['id'])

        #We must return a list of jobs successfully submitted, and a list of jobs failed
        return result



    def initSubmit(self):
        """
        _makeConfig_

        Make common JDL header
        """        
        jdl = []
        jdl.append("universe = vanilla\n")
        #jdl.append("universe = globus\n")
        #jdl.append("globusscheduler = %s\n" % self.job)
        jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Output = condor.out\n")
        jdl.append("Error = condor.err\n")
        jdl.append("Log = condor.log\n")
        
        return jdl
    
        
    def makeSubmit(self, job):
        """
        _makeJobJDL_

        For a given job/cache/spec make a JDL fragment to submit the job

        """

        logging.error("In makeSubmit")
        logging.error(job)
        logging.error(type(job))
        logging.error(job.keys())
        logging.error(job.get('cache_dir', 'This is silly'))
        
        # -- scriptFile & Output/Error/Log filenames shortened to 
        #    avoid condorg submission errors from > 256 character pathnames
        scriptFile = "%s" %self.config['submitScript']
        #self.makeWrapperScript(scriptFile, jobID)
        logging.debug("Submit Script: %s" % scriptFile)
        
        jdl = []
        jdl.append("Executable = %s\n" % scriptFile)
        jdl.append("initialdir = %s\n" % job['cache_dir'])
        argString = "arguments = %i %s %s\n" %(job['id'], self.config['submitNode'], job['sandbox'])
        jdl.append(argString)


        jdl.append("+WMAgent_JobName = \"%s\"\n" % job['name'])
        jdl.append("+WMAgent_JobID = \"%s\"\n" % job['id'])
        
        jdl.append("Queue\n")
        
        return jdl
