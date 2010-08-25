#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, E1103
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# E1103: The thread will have a logger and a dbi before it gets here

"""
_CondorGlideInPlugin_

A plug-in that should submit directly to condor glide-in nodes

"""

__revision__ = "$Id: CondorGlideInPlugin.py,v 1.2 2010/04/26 15:14:07 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

import os
import os.path
import logging
import threading

from subprocess import Popen, PIPE

from WMCore.DAOFactory import DAOFactory

from WMCore.WMInit import getWMBASE

from WMComponent.JobSubmitter.Plugins.PluginBase import PluginBase

class CondorGlideInPlugin(PluginBase):
    """
    _CondorGlideInPlugin_
    
    A plug-in that should submit directly to condor glide-in nodes
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

        return

    def __call__(self, parameters):
        """
        _submitJobs_
        
        If this class actually did something, this would handle submissions
        """

        if parameters == [] or not 'jobs' in parameters.keys():
            return {'NoResult': [0]}

        logging.error(parameters)

        jobList = parameters.get('jobs')
        self.packageDir = parameters.get('packageDir', None)
        self.unpacker   = os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')

        logging.error("I have jobs")
        logging.error(jobList[0])

        if type(jobList) == dict:
            #We only got one of them
            #Retain list functionality for possibiity of future multi-jobs
            jobList = [jobList]

        if not os.path.isdir(self.config['submitDir']):
            if not os.path.exists(self.config['submitDir']):
                os.mkdir(self.config['submitDir'])


        jdlList = self.makeSubmit(jobList)
        if not jdlList or jdlList == []:
            # Then we got nothing
            logging.error("No JDL file made!")
            return {'NoResult': [0]}
        jdlFile = "%s/submit.jdl" % (self.config['submitDir'])
        handle = open(jdlFile, 'w')
        handle.writelines(jdlList)
        handle.close()


        #Now submit them
        logging.error("About to submit %i jobs" %(len(jobList)))
        command = ["condor_submit", jdlFile]
        pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.wait()


        result = {'Success': []}

        for job in jobList:
            if job == {}:
                continue
            result['Success'].append(job['id'])

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

        
        jdl.append("universe = vanilla\n")
        jdl.append('(Memory >= 1 && OpSys == \"LINUX\" ) && (Arch == \"INTEL\" || Arch == \"X86_64\")\n')
        jdl.append("should_transfer_executable = TRUE\n")
        jdl.append("transfer_output_files = Report.pkl\n") 
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Executable = %s\n" % scriptFile)
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")
        # Things that are necessary for the glide-in
        jdl.append('+DESIRED_Sites = \"FNAL\"\n')
        jdl.append('+DESIRED_Archs = \"INTEL,X86_64\"\n')
        
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


        index = 0
        
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
                       % (job['sandbox'], self.packageDir,
                          'JobPackage.pkl', self.unpacker))
            argString = "arguments = %s %i\n" \
                        % (os.path.basename(job['sandbox']), index)
            jdl.append(argString)

            #jobCE = self.getCEName(jobSite = job['location'])
            #if not jobCE:
                # Then we ended up with a site that doesn't exist?
            #    logging.error("Job for non-existant site %s" \
            #                  % (job['location']))
            #    continue
            #jdl.append("globusscheduler = %s\n" % (jobCE))

            jdl.append("+WMAgent_JobName = \"%s\"\n" % job['name'])
            jdl.append("+WMAgent_JobID = %s\n" % job['id'])
        
            jdl.append("Queue 1\n")

            index += 1
        
        return jdl


    #def getCEName(self, jobSite):
    #    """
    #    _getCEName_
    #
    #    This is how you get the name of a CE for a job
    #    """
    #
    #    if not jobSite in self.locationDict.keys():
    #        siteInfo = self.locationAction.execute(siteName = jobSite)
    #        self.locationDict[jobSite] = siteInfo[0].get('ce_name', None)
    #        logging.error("About to get jobSite info")
    #        logging.error(jobSite)
    #        logging.error(siteInfo)
    #        logging.error(self.locationDict)
    #    return self.locationDict[jobSite]
