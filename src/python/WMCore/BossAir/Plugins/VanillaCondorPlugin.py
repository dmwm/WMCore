#!/usr/bin/env python

"""
VanillaCondorPlugin

BossAir plugin for vanilla condor
"""
import os.path
import logging

from WMCore.BossAir.Plugins.CondorPlugin import CondorPlugin
from WMCore.Credential.Proxy             import Proxy


class VanillaCondorPlugin(CondorPlugin):
    """
    _VanillaCondorPlugin_

    Minor variation on standard glide-in based plugin to allow
    us to submit vanilla jobs.
    """

    def initSubmit(self, jobList = None):
        """
        _makeConfig_

        Make common JDL header: Modified to removed +DESIRED variables
        """
        jdl = []


        # -- scriptFile & Output/Error/Log filenames shortened to
        #    avoid condorg submission errors from > 256 character pathnames

        jdl.append("universe = vanilla\n")
        jdl.append("requirements = (Memory >= 1 && OpSys == \"LINUX\" ) && (Arch == \"INTEL\" || Arch == \"X86_64\")\n ")

        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Executable = %s\n" % self.scriptFile)
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")
        # Things that are necessary for the glide-in

        jdl.append("+WMAgent_AgentName = \"%s\"\n" %(self.agent))

        if self.proxy:
            # Then we have to retrieve a proxy for this user
            job0   = jobList[0]
            userDN = job0.get('userdn', None)
            if not userDN:
                # Then we can't build ourselves a proxy
                logging.error("Asked to build myProxy plugin, but no userDN available!")
                logging.error("Checked job %i" % job0['id'])
                return jdl
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
            jdl.append("x509userproxy = %s\n" % filename)

        return jdl


    def makeSubmit(self, jobList):
        """
        _makeSubmit_

        Modified so that it doesn't look for the location

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

            # Check for multicore
            if job.get('taskType', None) in self.multiTasks:
                jdl.append('+RequiresWholeMachine?' 'TRUE')

            # Transfer the output files
            jdl.append("transfer_output_files = Report.%i.pkl\n" % (job["retry_count"]))

            # Add priority if necessary
            if job.get('priority', None) != None:
                try:
                    prio = int(job['priority'])
                    jdl.append("priority = %i\n" % prio)
                except ValueError:
                    logging.error("Priority for job %i not castable to an int\n" % job['id'])
                    logging.error("Not setting priority")
                    logging.debug("Priority: %s" % job['priority'])
                except Exception, ex:
                    logging.error("Got unhandled exception while setting priority for job %i\n" % job['id'])
                    logging.error(str(ex))
                    logging.error("Not setting priority")

            jdl.append("+WMAgent_JobID = %s\n" % job['jobid'])

            jdl.append("Queue 1\n")

        return jdl
