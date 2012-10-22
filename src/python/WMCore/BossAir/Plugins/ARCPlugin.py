#!/usr/bin/env python
#
# Plugin to be used together with the NorduGrid ARC grid middleware.
#
# To work, it needs the env. variable CLEAN_LD_LIBRARY_PATH to be set to
# something that can be used as LD_LIBRARY_PATH when running the ARC ng*
# commands.
#

import os
import time
import re
import logging
import tempfile
import subprocess
from WMCore.DAOFactory                 import DAOFactory
from WMCore.WMInit                     import getWMBASE
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException


def gen_wrapper_script():
    code = "#!/bin/bash\n\n"

    code += "env\n"

    code += "# Make sure python is version 2.6\n"
    code += "pyv=`python -V 2>&1 | sed 's/Python \([0-9]\.[0-9]\).*/\1/'`\n"
    code += "if [ $pyv != \"2.6\" ]; then\n"
    code += "   python_version=`ls -t $VO_CMS_SW_DIR/slc5_amd64_gcc434/external/python | head -1`\n"
    code += "   . $VO_CMS_SW_DIR/slc5_amd64_gcc434/external/python/$python_version/etc/profile.d/init.sh\n"
    code += "fi\n\n"
    code += "python -V\n\n"

    code += "bash $@\n"

    script_file = tempfile.NamedTemporaryFile(prefix="wrapper.")
    script_file.write(code)
    script_file.flush()
    return script_file


def gen_xrsl(job, wrapper_script, jobscript, unpacker):
    xrsl =  '&'

    xrsl += '(executable=%s)' % os.path.basename(wrapper_script)
    xrsl += '(arguments=%s %s %s)' % (os.path.basename(jobscript), os.path.basename(job['sandbox']),
                                      job['id'])
    xrsl += '(inputfiles='
    xrsl += '(%s %s)' % (os.path.basename(wrapper_script), wrapper_script)
    xrsl += '(%s %s)' % (os.path.basename(jobscript), jobscript)
    xrsl += '(%s %s)' % (os.path.basename(unpacker), unpacker)
    xrsl += '(%s %s)' % ("JobPackage.pkl", job['packageDir'] + "/JobPackage.pkl")
    xrsl += '(%s %s)' % (os.path.basename(job['sandbox']), job['sandbox'])
    xrsl += ')'

    xrsl += '(outputFiles=("/" ""))'

    xrsl += '(stderr=stderr)'
    xrsl += '(stdout=stdout)'

    xrsl += '(runtimeenvironment>=APPS/HEP/CMSSW-3.0.0)'  # Hopefully, the exact version doesn't matter

    xrsl += '(jobName=WMAgent-%s)' % job['jobid']

    return xrsl


def executeCommand(cmd):
    pre = "LD_LIBRARY_PATH=$CLEAN_LD_LIBRARY_PATH"
    command = pre + " " + cmd
    logging.debug("Executing '%s'" % command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,
                                                      stderr=subprocess.STDOUT)
    output = p.communicate()[0]
    stat = p.returncode
    return stat, output


def splitNgstatOutput(output):
    """
    Split a string of ngstat output into a list with one job per list
    item.

    The assumption is that the first line of a job has no indentation, and
    subsequent lines are indented by at least 1 space or start with "This
    job was only very recently submitted".  """

    jobs = []
    s = ""
    for line in output.split('\n'):

        if len(line) == 0:
            continue

        if line[0].isspace():
            s += '\n' + line
        elif re.match("This job was only very recently submitted", line):
            s += ' ' + line
        else:
            if len(s) > 0:
                jobs.append(s + '\n')
            s = line
    if len(s) > 0:
        jobs.append(s)

    return jobs


class ARCPlugin(BasePlugin):


    @staticmethod
    def stateMap():
        stateDict = {"ACCEPTING":  "Pending",
                     "ACCEPTED":   "Pending",
                     "PREPARING":  "Pending",
                     "PREPARED":   "Pending",
                     "SUBMITTING": "Pending",
                     "INLRMS:Q":   "Pending",

                     "INLRMS:R":   "Running",
                     "INLRMS:S":   "Running",  # Suspended
                     "INLRMS:E":   "Running",  # Executed
                     "INLRMS:O":   "Running",  # Other
                     "EXECUTED":   "Running",
                     "FINISHING":  "Running",

                     "FINISHED":   "Complete",
                     "KILLING":    "Complete", # Being canceled by user
                     "KILLED":     "Complete", # Canceled by user

                     "DELETED":    "Error",    # Killed by system
                     "FAILED":     "Error",

                     "LOST":       "Error"
                     }
        return stateDict


    def submit(self, jobs, info = None):

        jobscript = self.config.JobSubmitter.submitScript
        unpacker = os.path.join(getWMBASE(),'src/python/WMCore/WMRuntime/Unpacker.py')

        xrsl = '+\n'
        wrapper = gen_wrapper_script()
        for j in jobs:
            logging.debug("ARCplugin.submit: got job %s" % str(j))
            xrsl += '(' + gen_xrsl(j, wrapper.name, jobscript, unpacker) + ')\n'
        xrsl_file = tempfile.NamedTemporaryFile(prefix="xrslcode.")
        xrsl_file.write(xrsl)
        xrsl_file.flush()

        cmd = "ngsub %s -c %s" % (xrsl_file.name, jobs[0]['location'])
        s, output = executeCommand(cmd)
        logging.debug("executed %s" % cmd)
        logging.debug("with exit stat %s" % s)
        logging.debug("and output %s" % output)
        xrsl_file.close()
        wrapper.close()

        subRe = re.compile("Job submitted with jobid: +(\w+://([a-zA-Z0-9.-]+)(:\d+)?(/.*)?/\d+)")
        lines = output.split('\n')
        n = 0
        failure = []
        success = []
        for j in jobs:
            if n < len(lines):
                m = re.match(subRe, lines[n])
            else:
                m = None

            if m:
                j['gridid'] = m.group(1)
                success.append(j)
                logging.debug("Successful job %s" % j['gridid'])
            else:
                failure.append(j)
                logging.debug("Failed job")

            n += 1

        logging.info("%i successful job submissions, %i failures" % (len(success), len(failure)))
        return success, failure


    def createJobsFile(self, jobs):
        """
        Create a file with job arcIds.
        Return a file object, and an {arcId: job}-dictionary.
        The file will be removed when the file object is closed.
        """

        arcId2job = {}
        jobsFile = tempfile.NamedTemporaryFile(prefix="crabjobs.")

        for j in jobs:
            arcId = j['gridid']
            jobsFile.write(arcId + "\n")
            arcId2job[arcId] = j
        jobsFile.flush()

        return jobsFile, arcId2job


    def track(self, jobs, info = None):
        changeList   = []
        completeList = []
        runningList  = []

        jobsFile, arcId2job = self.createJobsFile(jobs)

        s, output = executeCommand("ngstat -t 180 -i %s" % jobsFile.name)
        if s != 0:
            raise BossAirPluginException, "ngstat failed:" + output

        for js in splitNgstatOutput(output):
            arcStat = None
            if js.find("Job information not found") >= 0:
                if js.find("job was only very recently submitted"):
                    arcStat = "NOT_FOUND_NEW"
                else:
                    arcStat = "LOST"

                arcIdMatch = re.search("(\w+://([a-zA-Z0-9.-]+)\S*/\d*)", js)
                if not arcIdMatch:
                    raise BossAirPluginException, "No grid job ID!"
                arcId = arcIdMatch.group(1)

            elif js.find("Malformed URL:") >= 0:
                # This shouldn't be possible, since we are pass arcID:s to
                # ngstat.
                arcIdMatch = re.search("URL: (\w+://([a-zA-Z0-9.-]+)\S*/\d*)", js)
                raise BossAirPluginException, "Malformed URL for job " + arcIdMatch.group(1)
            else:
                # With special cases taken care of above, we are left with
                # "normal" jobs. They are assumed to have the format
                #
                # Job <arcId>
                #   Status: <status>
                #   Whatever: blah
                #

                for line in js.split('\n'):

                    arcIdMatch = re.match("Job +(\w+://([a-zA-Z0-9.-]+)\S*/\d*)", line)
                    if arcIdMatch:
                        arcId = arcIdMatch.group(1)
                        continue

                    statusMatch = re.match(" +Status: *(.+)", line)
                    if statusMatch:
                        arcStat = statusMatch.group(1)
                        continue

            j = arcId2job[arcId]

            if arcStat == "NOT_FOUND_NEW":
                if j['status'] in [ "New", "ACCEPTING" ] and (not j['status_time']) \
                                                         or int(time.time()) - j['status_time'] < 60:
                    arcStat = "ACCEPTING" # Probably approximately true
                else:
                    arcStat = "LOST"

            j['globalState'] = ARCPlugin.stateMap()[arcStat]

            if arcStat != j['status']:
                j['status'] = arcStat
                j['status_time'] = int(time.time())
                changeList.append(j)

            logging.debug("Job %s has status %s" % (j['gridid'], j['status']))

            if ARCPlugin.stateMap()[arcStat] not in ["Complete", "Error"]:
                runningList.append(j)
            else:
                completeList.append(j)

        return runningList, changeList, completeList


    def complete(self, jobs):
        """
        Fetch log-files and remove jobs.
        """

        toBeRemoved = ""
        for j in jobs:
            if j['status'] in [ "KILLING", "KILLED", "DELETED" ]:
                logging.info("Complete: do nothing for %s, because it's '%s'" % (j['gridid'], j['status']))
                continue

            if j.get('cache_dir', None) == None:
                logging.warning("job %s has no 'cache_dir'" % j['id'])
                continue

            if j.get('retry_count', None) == None:
                logging.warning("job %s has no 'retry_count'" % j['id'])
                continue

            #reportName = 'Report.%i.pkl' % j['retry_count']
            reportName = 'Report.pkl'
            reportPath = os.path.join(j['cache_dir'], reportName)
            if os.path.isfile(reportPath) and os.path.getsize(reportPath) > 0:
                logging.info("Job %s already has its log file fetched in %s!" % (j['id'], reportPath))
                continue

            cmd = "ngcp -t 180 %s/%s %s" % (j['gridid'], reportName, reportPath)
            s, output = executeCommand(cmd)
            if s != 0:
                logging.error("Fetching report file for job %s/%s failed: %s/%i" % (j['id'], j['gridid'],
                                                                                 output, s))
                continue

            toBeRemoved += " " + j['gridid']

        logging.info("Removing jobs %s" % toBeRemoved)
        if toBeRemoved:
            s, output = executeCommand("ngclean -t 180 " + toBeRemoved)
            if s != 0:
                logging.error("ngclean failed!: %s" % output)

        return


    def kill(self, jobs, info = None):
        for j in jobs:
            n = 0
            while True:
                # Kill the job, or, if it's already finished, clean it.
                s, output = executeCommand("ngkill -t 180 %s" % j['gridid'])
                if output.find("Job has already finished") >= 0:
                    s, output = executeCommand("ngclean -t 180  %s" % j['gridid'])

                n += 1
                if output.find("Job information not found") >= 0:
                    if n <= 10:
                        # Wait for a while and try again
                        nap = 20
                        logging.debug("Job information not found for %s; trying again in %i s" % ( j['gridid'], nap))
                        time.sleep(nap)
                    else:
                        logging.error("Killing job %s/%s failed: %s" % (j['id'], j['gridid'], output))
                        break # give up
                else:
                    if s != 0:
                        logging.error("Killing job %s/%s failed: %s" % (j['id'], j['gridid'], output))
                    else:
                        j['status'] = "KILLED"
                        j['status_time'] = int(time.time())
                    break
