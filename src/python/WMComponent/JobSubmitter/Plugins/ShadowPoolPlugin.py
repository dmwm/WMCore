#!/usr/bin/env python

"""
_ShadowPoolPlugin_

A shadow pool plugin just to test the jobSubmitter class, and submit to
cms-sleepgw.fnal.gov

"""




import os
import os.path
import logging
import threading

from subprocess import Popen, PIPE

from WMComponent.JobSubmitter.Plugins.PluginBase import PluginBase

class ShadowPoolPlugin(PluginBase):
    """
    _ShadowPoolPlugin_
    
    A shadow pool plugin just to test the jobSubmitter class, and submit to
    cms-sleepgw.fnal.gov
    """

    def __init__(self, **configDict):
        self.config = configDict

    def __call__(self, parameters):
        """
        _submitJobs_
        
        If this class actually did something, this would handle submissions
        """

        if parameters == {} or parameters == [] or not 'jobs' in parameters.keys():
            return {'NoResult': [0]}

        jobList = parameters.get('jobs')
        self.packageDir = parameters.get('packageDir', None)
        self.unpacker   = os.path.join(os.getenv('WMCOREBASE'), 'src/python/WMCore/WMRuntime/Unpacker.py')

        if type(jobList) == dict:
            #We only got one of them
            #Retain list functionality for possibiity of future multi-jobs
            jobList = [jobList]

        if not os.path.isdir(self.config['submitDir']):
            if not os.path.exists(self.config['submitDir']):
                os.mkdir(self.config['submitDir'])


        baseConfig = self.initSubmit()

        jobSubmitFiles = []
        index = 0
        for job in jobList:

            logging.error('Have job separated out')
            logging.error(job)
            if job == {}:
                continue
            tmpList = []
            tmpList.extend(baseConfig)
            tmpList.extend(self.makeSubmit(job, index))
            jdlFile = "%s/submit_%i.jdl" % (self.config['submitDir'], job['id'])
            handle = open(jdlFile, 'w')
            handle.writelines(tmpList)
            handle.close()

            #Increment the index after the job
            index += 1


            jobSubmitFiles.append(jdlFile)

        #Now submit them

        for submit in jobSubmitFiles:
            command = ["condor_submit", submit]
            pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
            pipe.wait()


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
        #jdl.append("universe = vanilla\n")
        jdl.append("universe = globus\n")
        jdl.append("globusscheduler = cms-sleepgw.fnal.gov/jobmanager-condor\n" )
        jdl.append("should_transfer_executable = TRUE\n")
        #jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("transfer_output_files = Report.pkl\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")
        
        return jdl
    
        
    def makeSubmit(self, job, index):
        """
        _makeJobJDL_

        For a given job/cache/spec make a JDL fragment to submit the job

        """

        # -- scriptFile & Output/Error/Log filenames shortened to 
        #    avoid condorg submission errors from > 256 character pathnames
        scriptFile = "%s" % self.config['submitScript']
        #self.makeWrapperScript(scriptFile, jobID)
        logging.debug("Submit Script: %s" % scriptFile)
        
        jdl = []
        jdl.append("Executable = %s\n" % scriptFile)
        jdl.append("initialdir = %s\n" % job['cache_dir'])
        jdl.append("transfer_input_files = %s, %s/%s, %s\n" % (job['sandbox'], self.packageDir, 'JobPackage.pkl', self.unpacker))
        argString = "arguments = %s %i\n" % (os.path.basename(job['sandbox']), index)
        jdl.append(argString)


        jdl.append("+WMAgent_JobName = \"%s\"\n" % job['name'])
        jdl.append("+WMAgent_JobID = %s\n" % job['id'])
        
        jdl.append("Queue 1\n")
        
        return jdl
