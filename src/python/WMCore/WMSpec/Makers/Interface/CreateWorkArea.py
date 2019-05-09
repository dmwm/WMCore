#!/bin/env python



import os
import os.path
import threading
import logging
import time
import base64

from subprocess import Popen, PIPE

from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask                   import WMTask, WMTaskHelper

#This whole thing is meant to be executed in a thread
#Thus it assumes one jobGroup per instance
#I'm wondering if it's better to multithread in a different way


class CreateScript:
    """
    Simple class for creating file objects

    """

    def __init__(self, name = 'default', shell = 'bash'):
        self.name = name
        self.script = """
"""
        self.shell = ''


    def setName(self, name):
        """
        Change the file name
        """
        self.name = name
        return

    def setShell(self, shell):
        """
        Set the environment shell
        """
        self.shell = shell

    def getEnv(self):
        """
        Get environment from SHELL
        """

        self.shell = os.getenv('SHELL').split('/')[-1]
        return

    def startScript(self):
        if   self.shell == 'bash':
            self.addLine('#!/bin/bash')
        elif self.shall == 'csh':
            self.addLine('#!/bin/csh')
        else:
            self.addLine('#!/bin/sh')

    def addLine(self, line = '\n'):
        """
        Add line of to the script
        """
        self.script += line + '\n'
        return

    def addEnv(self, var, value):
        """
        Add a shell-specific environment value
        This automatically appends
        """
        if os.getenv(var) == None:
            if self.shell == 'csh':
                self.addLine('export %s %s' %(var, value))
            else:
                self.addLine('export %s=%s' %(var, value))
        else:
            if self.shell == 'csh':
                self.addLine('export %s $%s:%s' %(var, var, value))
            else:
                self.addLine('export %s=$%s:%s' %(var, var, value))


    def save(self):
        """
        Now save it to the given name
        """
        if os.path.exists(self.name):
            msg = 'I will not overwrite file %s' %(self.name)
            raise Exception(msg)
        with open(self.name, 'w') as fd:
            fd.write(self.script)
        return

class CreateWorkArea:
    """
    Basic class for doing the JobMaker dirty work

    """



    def __init__(self, jobGroupID = None, startDir = None):


        myThread           = threading.currentThread()
        myThread.logger    = logging.getLogger()

        self.jobGroupID    = jobGroupID
        self.jobGroup      = None
        self.subscript     = None
        self.workflow      = None
        self.collectionDir = None
        self.wmWorkload    = None
        if not startDir:
            self.startDir     = os.getcwd()
        else:
            self.startDir  = startDir

        self.jobs          = {}

        self.getNewJobGroup(jobGroupID)

        self.timing = {'getNewJobGroup': 0, 'createJobGroupArea': 0, 'createWorkArea': 0, 'createDirectories': 0, 'cacheNaming': 0}

        return

    def reset(self):
        """
        Reset key variables between runs

        """

        self.workflow   = None
        self.wmWorkload = None


    def processJobs(self, jobGroup, startDir, wmWorkload = None, workflow = None):
        """
        Process the work

        This allows you to pass in two pre-loaded objects, the WMWorkloadSpace and the
        WMBS workflow, to save loading time
        """
        self.reset()
        self.wmWorkload = wmWorkload
        self.workflow   = workflow
        self.getNewJobGroup(jobGroup = jobGroup, startDir = startDir)
        self.createJobGroupArea()
        self.createWorkArea()

        self.timing = {'getNewJobGroup': 0, 'createJobGroupArea': 0, 'createWorkArea': 0, 'createDirectories': 0, 'cacheNaming': 0}

        return



    def getNewJobGroup(self, jobGroup = None, startDir = None):
        """
        This gets a job group passed to the thread
        """

        #See if we actually have a jobGroupID
        if jobGroup:
            self.jobGroupID = jobGroup.id
        else:
            #Then we have no jobGroup
            return

        if startDir:
            self.startDir = startDir

        myThread = threading.currentThread()


        #We need the workflow to get the spec
        if self.workflow == None:
            # If we have something in the workflow,
            # assume we were passed a loaded workflow
            # We need the subscription mostly to get the workflow
            self.subscript = jobGroup.subscription
            self.subscript.load()
            self.workflow  = self.subscript['workflow']
            self.workflow.load()

        if not jobGroup.exists():
            msg = 'JobMaker: Was passed a non-existant Job Group ID %i' %(self.jobGroupID)
            logging.error(msg)
            raise Exception(msg)

        self.jobGroup = jobGroup

        return




    def createJobGroupArea(self):
        """
        Creates an area for the task which is just the jobGroupUID, in which the jobs will be put

        """

        workloadDir, taskDir = self.getMasterName()

        #Create the workload directory
        if not os.path.isdir(workloadDir):
            os.makedirs(workloadDir)

        #Create the task directory
        if not os.path.isdir(taskDir):
            os.makedirs(taskDir)

        logging.info('JobMaker: Now in directory %s' %(os.getcwd()))

        return




    def createWorkArea(self):
        """
        This should handle the master tasks of creating a working area
        It should take a valid jobGroup and call the functions that create the components

        """
        myThread = threading.currentThread()

        if self.jobGroup == None:
            msg = 'JobMaker: Ordered to create for non-existant jobGroup  Failing'
            logging.error(msg)
            raise Exception(msg)

        workloadDir, taskDir       = self.getMasterName()
        jobCounter    = 0
        jobCollDir    = 0
        nameList      = []

        factory = DAOFactory("WMCore.WMBS", myThread.logger, myThread.dbi)
        setBulkCache=factory(classname = "Jobs.SetCache")
        nameDictList = []


        #Now actually start to do things
        myThread.transaction.begin()
        #jobList = self.jobGroup.listJobIDs()
        for job in self.jobGroup.jobs:
            jid = job['id']

            if jobCounter%1000 == 0:
                #Create a new jobCollection
                #Increment jobCreator if there's already something there
                jobCounter += self.createJobCollection(jobCounter, taskDir)

            jobCounter = jobCounter + 1

            name = self.getDirectoryName(jid)
            nameList.append(name)
            nameDictList.append({'jobid':jid, 'cacheDir':name})
            job['cache_dir'] = name


        setBulkCache.execute(jobDictList = nameDictList)

        self.createDirectories(nameList)

        myThread.transaction.commit()

        return




    def createJobCollection(self, jobCounter, taskDir):
        """

        Create a sub-directory to allow storage of large jobs
        """

        value = jobCounter/1000
        jobCollDir = '%s/JobCollection_%i_%i' %(taskDir, self.jobGroup.id, value)
        #Set this to a global variable
        self.collectionDir = jobCollDir
        if not os.path.exists(jobCollDir):
            #This should be the only application
            #You return 0 because the directory you just made should be empty
            os.mkdir(jobCollDir)
            return 0
        if os.path.isdir(jobCollDir):
            #This should never happen
            return len(os.listdir(jobCollDir))
        elif os.path.isfile(jobCollDir):
            #Well, you're screwed.  Some other file is in the way: IN A DIRECTORY YOU JUST CREATED.
            #Time to freak the hell out
            raise Exception ("Could not create jobCollection %s; non-directory file in the way!" %(jobCollDir))
        else:
            #You're screwed
            raise Exception ('There was something in the way at %s, but we could not determine what it was' %(jobCollDir))



    def createDirectories(self, dirList):
        """
        Create the directory if everything is sane

        """
        myThread = threading.currentThread()


        #This is gonna be tricky
        cmdList = []
        cmdArgs = ['mkdir']

        while len(dirList) > 500:
            cmdArgs.extend(dirList[:500])
            cmdList.append(cmdArgs)
            cmdArgs = ['mkdir']
            dirList = dirList[500:]
        if len(dirList) > 0:
            cmdArgs.extend(dirList)
            cmdList.append(cmdArgs)

        logging.info('Executing makedir commands')
        for command in cmdList:
            pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
            pipe.wait()


        return




    def createLocalBin(self, workdir, binName = 'localBin'):
        """
        Creates a localBin in the working directory

        """

        myThread = threading.currentThread()

        binPath = workdir + '/' + binName

        if not os.path.isdir(workdir):
            msg = 'JobMaker: Ordered to generate environment script in non-existant directory %s' %(workdir)
            logging.error(msg)
            raise Exception(msg)

        if os.path.exists(binPath):
            #This is probably not a critical error
            logging.error('Someone already created %s' %(binPath))
            return

        os.mkdir(binPath)

        return



    def generateRunScript(self, workdir, scriptName = 'run.sh'):
        """
        This generates a basic run script in the workdir directory

        """

        myThread = threading.currentThread()

        scriptPath = workdir+'/'+scriptName

        if not os.path.isdir(workdir):
            msg = 'JobMaker: Ordered to generate run script in non-existant directory %s' %(workdir)
            logging.error(msg)
            raise Exception(msg)

        RunScript = CreateScript(scriptPath)

        RunScript.getEnv()
        RunScript.startScript()
        RunScript.addLine('export PRODAGENT_JOBSPEC=$1')
        RunScript.addLine('echo \"Job Spec: $PRODAGENT_JOBSPEC\"')
        RunScript.addLine(". jobEnvironment.sh")
        RunScript.addLine("shreek --config=./ShREEKConfig.xml")

        RunScript.save()


        return





    def generateEnvironmentScript(self, workdir, scriptName = 'jobEnvironment.sh'):
        """
        This generates a basic environment sourcing script in the workdir directory

        """

        myThread = threading.currentThread()

        scriptPath = workdir+'/'+scriptName

        if not os.path.isdir(workdir):
            msg = 'JobMaker: Ordered to generate environment script in non-existant directory %s' %(workdir)
            logging.error(msg)
            raise Exception(msg)

        EnvScript = CreateScript(scriptPath)

        EnvScript.getEnv()
        EnvScript.startScript()
        EnvScript.addEnv('PYTHONPATH','`pwd`/localPython')
        EnvScript.addEnv('PATH','`pwd`/localBin')
        EnvScript.addEnv('PRODAGENT_JOB_DIR','`pwd`')
        #EnvScript.addEnv('PRODAGENT_WORKFLOW_SPEC',"`pwd`/%s" % os.path.basename(self.workflowFile))

        EnvScript.save()





    def getDirectoryName(self, jid):
        """
        Gets a universal name for the directory we're working in.

        """

        name = 'job_%i' %(jid)

        return os.path.join(self.collectionDir, name)

    def getMasterName(self):
        """
        Gets a universal name for the jobGroup directory
        Return the uid as the name if none available (THIS SHOULD NEVER HAPPEN)

        """

        if self.wmWorkload != None:
            workload = self.wmWorkload.name()
        elif not os.path.exists(self.workflow.spec):
            logging.error("Could not find Workflow spec %s; labeling jobs by job ID only!" %(self.workflow.spec))
            return os.path.join(self.startDir, self.jobGroup.uid), os.path.join(self.startDir, self.jobGroup.uid)
        else:
            wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
            wmWorkload.load(self.workflow.spec)

            workload = wmWorkload.name()

        task = self.workflow.task
        if task.startswith("/" + workload + "/"):
            task = task[len(workload) + 2:]

        return os.path.join(self.startDir, workload), os.path.join(self.startDir, workload, task)
