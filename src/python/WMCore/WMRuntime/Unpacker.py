#!/usr/bin/env python2.6
"""
_Unpacker_

Minimal WN tool to unpack the WMSandbox and start bootstrapping the job

Inputs are:

1. Task level Sandbox file
2. Job Package file
3. Job Package Index
4. Job Name is needed to generate an error report in case of failure

This will then do the following:
- create a job directory
- unpack the sandbox
- drop the package and index into the job area
- set everything up so that you can just add the job dir to the pythonpath and then call the runtime startup for the WMCore/WMRuntime stuff

"""
import sys
import os
import tarfile
import zipfile
import getopt
import shutil
import traceback

from subprocess import Popen, PIPE


options = {
    "sandbox=" : "WMAGENT_SANDBOX", # sandbox archive file
    "package=" : "WMAGENT_PACKAGE", # job package pickle file
    "index="   : "WMAGENT_INDEX",   # index of job to be run
    "jobname=" : "WMAGENT_JOBNAME", # job name/id
    }



def makeErrorReport(jobName, exitCode, message):
    """
    _makeErrorReport_

    Generate a Job Report indicating failure to bootstrap.
    Build as simple string to avoid dependencies on libraries required
    during bootstrap which may have failed.

    TODO: Include side & node information lookup
    TODO: FwkJobRep objects need refactor/cleanup for WMAgent
    TODO: Identify set of error codes

    """
    xml = "<xml>\n"
    xml += "<FrameworkJobReport Status=\"Failed\" Name=\"%s\">\n" % jobName
    xml += "<ExitCode Value=\"%s\"/>\n" % exitCode
    xml += "<FrameworkError Type = \"BootstrapError\">\n"
    xml += str(message)
    xml += "\n"
    xml += "</FrameworkError>\n"
    xml += "</FrameworkJobReport>\n"
    xml += "</xml>\n"
    handle = open("FrameworkJobReport.xml", 'w')
    handle.write(xml)
    handle.close()



def createWorkArea(sandbox):
    """
    _createWorkArea_

    Create a job working area containing all the bits and pieces
    needed to bootstrap up and kickstart the job

    """
    currentDir = os.getcwd()
    jobDir = "%s/job" % currentDir
    if not os.path.exists(jobDir):
        os.makedirs(jobDir)
    if not os.path.exists(os.path.join( jobDir, 'StartupScript' ) ):
        os.makedirs(os.path.join( jobDir, 'StartupScript' ) )


    tfile = tarfile.open(sandbox, "r")
    tfile.extractall(jobDir)
    tfile.close()

    # need to pull out the startup file from the zipball
    zfile = zipfile.ZipFile( os.path.join( jobDir, 'WMCore.zip' ), 'r' )
    startupScript = zfile.read( 'WMCore/WMRuntime/Startup.py' )
    fd = os.open( os.path.join( jobDir, 'Startup.py'), os.O_CREAT | os.O_WRONLY )
    os.write(fd, startupScript)
    os.close(fd)

    zfile.close()

    print "export PYTHONPATH=$PYTHONPATH:%s/WMCore.zip:%s" % (jobDir, jobDir)
    return jobDir

def installPackage(jobArea, jobPackage, jobIndex):
    """
    _installPackage_

    Install the job package and index into the job directory so that
    it can be found on bootstrap

    """
    target = "%s/WMSandbox" % jobArea
    pkgTarget = "%s/JobPackage.pcl" % target
    #shutil.copy(jobPackage, pkgTarget)
    os.system("/bin/cp %s %s" % (jobPackage, pkgTarget))

    indexPy = "%s/JobIndex.py" % target
    handle = open(indexPy, 'w')
    handle.write("jobIndex = %s\n" % jobIndex)
    handle.close()

    return


def runUnpacker(sandbox, package, jobIndex, jobname):
    """
    Run everything in the unpacker

    """



    try:
        jobArea = createWorkArea(sandbox)
        installPackage(jobArea, package, jobIndex)
        #sys.exit(0)
    except Exception, ex:
        msg = "Unable to create job area for bootstrap\n"
        msg += str(ex)
        msg += str(traceback.format_exc())
        makeErrorReport(jobname, 1, msg)
        print msg
        sys.exit(1)


if __name__ == '__main__':


    try:
        opts, args = getopt.getopt(sys.argv[1:], "", options.keys())
    except getopt.GetoptError, ex:
        msg = "Error processing commandline args:\n"
        msg += str(ex)
        print msg
        sys.exit(1)

    sandbox  = os.environ.get('WMAGENT_SANDBOX', None)
    package  = os.environ.get('WMAGENT_PACKAGE', None)
    jobIndex = os.environ.get('WMAGENT_INDEX'  , None)
    jobname  = os.environ.get('WMAGENT_JOBNAME', None)
    for opt, arg in opts:
        if opt == "--sandbox":
            sandbox = arg
        if opt == "--package":
            package = arg
        if opt == "--index":
            jobIndex = arg
        if opt == "--jobname":
            jobname = arg


    if sandbox == None:
        msg = "No Sandbox provided"
        makeErrorReport(jobname, 1, msg)
        print msg
        sys.exit(1)
    if package == None:
        msg = "No Job Package provided"
        makeErrorReport(jobname, 1, msg)
        print msg
        sys.exit(1)
    if jobIndex == None:
        msg = "No Job Index provided"
        makeErrorReport(jobname, 1, msg)
        print msg
        sys.exit(1)

    runUnpacker(sandbox = sandbox, package = package,
                jobIndex = jobIndex, jobname = jobname)
