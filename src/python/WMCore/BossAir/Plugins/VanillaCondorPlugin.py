#!/usr/bin/env python

"""
VanillaCondorPlugin

BossAir plugin for vanilla condor
"""
import os.path

from WMCore.BossAir.Plugins.CondorPlugin import CondorPlugin


class VanillaCondorPlugin(CondorPlugin):
    """
    _VanillaCondorPlugin_

    Minor variation on standard glide-in based plugin to allow
    us to submit vanilla jobs.
    """

    def initSubmit(self):
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
