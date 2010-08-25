#!/bin/env python



import os
import os.path
import threading
import logging


from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job

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
        file = open(self.name, 'w')
        file.write(self.script)
        file.close()
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
        if not startDir:
            self.startDir     = os.getcwd()
        else:
            self.startDir  = startDir

        self.jobs          = {}

        self.getNewJobGroup(jobGroupID)

        return



    def getNewJobGroup(self, jobGroupID = None, startDir = None):
        """
        This gets a job group passed to the thread
        """

        #See if we actually have a jobGroupID
        if self.jobGroupID == None:
            if jobGroupID != None:
                self.jobGroupID = jobGroupID
            else:
                return

        if startDir:
            self.startDir = startDir

        myThread = threading.currentThread()

        #Load the JobGroup object
        jobGroup = JobGroup(id = self.jobGroupID)

        jobGroup.load()
        self.subscript = jobGroup.subscription
        self.subscript.loadData()
        #We need the workflow to get the spec
        self.workflow  = self.subscript['workflow']

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

        os.chdir(self.startDir)

        workloadDir, taskDir = self.getMasterName()

        #Create the workload directory
        if not os.path.isdir(workloadDir):
            os.mkdir(workloadDir)

        #Change to the workload directory
        os.chdir(workloadDir)

        #Create the task directory
        if not os.path.isdir(taskDir):
            os.mkdir(taskDir)

        #Move to the task Directory
        os.chdir(taskDir)

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

        jobList = self.jobGroup.listJobIDs()

        #print "Starting a jobGroup with %i jobs and ID %i" %(len(jobList), self.jobGroup.id)

        #Now actually start to do things
        for jid in jobList:
            job = Job(id = jid)
            if jobCounter%1000 == 0:
                #Create a new jobCollection
                #Increment jobCreator if there's already something there
                #print "Creating new dir at value %i" %(jobCounter)
                jobCounter += self.createJobCollection(jobCounter, taskDir)
                
            jobCounter = jobCounter + 1

            #Only work with new jobs
            os.chdir(self.collectionDir)
            if job['state'] == 'new':
                name = self.getDirectoryName(job)
                self.createDirectory(name, job)
                

        os.chdir(self.startDir)

        return

    def createJobCollection(self, jobCounter, taskDir):
        """

        Create a sub-directory to allow storage of large jobs
        """

        value = jobCounter/1000
        jobCollDir = '%s/JobCollection_%i_%i' %(taskDir, self.jobGroup.id, value)
        #Set this to a global variable
        self.collectionDir = jobCollDir
        if os.path.isdir(jobCollDir):
            #This should never happen
            return len(os.listdir(jobCollDir))
        elif os.path.isfile(jobCollDir):
            #We'll, you're screwed, some other file is in the way: IN A DIRECTORY YOU JUST CREATED.
            #Time to freak the fuck out
            raise Exception ("Could not create jobCollection %s; non-directory file in the way!" %(jobCollDir))
        else:
            #This should be the only application
            #You return 0 because the directory you just made should be empty
            os.mkdir(jobCollDir)
            return 0
        


    def createDirectory(self, workdir, job):
        """
        Create the directory if everything is sane

        """

        myThread = threading.currentThread()
        

        if os.path.exists(workdir):
            msg = 'JobMaker: Attempting to create working directory %s that already exists\n Job details: %s' %(workdir, str(job))
            logging.error(msg)
            raise Exception(msg)


        os.mkdir(workdir)

        #if not os.path.isdir(workdir):
        #    msg = 'JobMaker: Failed to create workdir %s; could not be located as directory' %(workdir)
        #    logging.error(msg)
        #    raise Exception(msg)

        
        #logging.info('JobMaker: Created directory %s for jobGroup %s' %(workdir, self.jobGroupID))
        workloadName, taskName = self.getMasterName()
        self.jobs['%s/%s/%s' %(workloadName, taskName, job['name'])] = ('%s' %(workdir))

        #Now fill them with stuff
        #self.createLocalBin(workdir)
        #self.generateRunScript(workdir)
        #self.generateEnvironmentScript(workdir)

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

        

    

    def getDirectoryName(self, job):
        """
        Gets a universal name for the directory we're working in.

        """

        name = 'job_%i' %(job['id'])

        return os.path.join(self.collectionDir, name)

    def getMasterName(self):
        """
        Gets a universal name for the jobGroup directory
        Return the uid as the name if none available (THIS SHOULD NEVER HAPPEN)

        """

        if self.workflow.spec.find('/') == -1:
            return os.path.join(self.startDir, self.jobGroup.uid), os.path.join(self.startDir, self.jobGroup.uid)
        else:
            workload = self.workflow.spec.split('/')[0]
            task     = self.workflow.spec.split('/')[1]
            return os.path.join(self.startDir, workload), os.path.join(self.startDir, workload, task)


    def cleanUpAll(self):
        """

        Deletes everything

        """

        workloadDir, taskDir = self.getMasterName()

        taskPath = '%s/%s' %(workloadDir, taskDir)
        
        os.chdir(self.startDir)
        os.chdir(taskPath)
        for dir in os.listdir(os.getcwd()):
            if not os.path.isdir(dir):
                os.remove(dir)
            else:
                for file in os.listdir(dir):
                    file_path = os.path.join(dir, file)
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        elif os.path.isdir(file_path):
                            for item in os.listdir(file_path):
                                if os.path.isdir(file_path+'/'+item):
                                    os.rmdir(file_path+'/'+item)
                                else:
                                    os.remove(file_path+'/'+item)
                            os.rmdir(file_path)
                    except Exception, ex:
                        msg = 'Failed to delete file %s with error %s' %(file_path, ex)
                        logging.error(msg)
                        raise Exception(msg)

            try:
                os.rmdir(dir)
            except Exception, ex:
                msg = 'Failed to delete directory %s with error %s' %(dir, ex)
                logging.error(msg)
                print os.listdir(dir)
                raise Exception(msg)
            

        os.chdir(self.startDir)

        os.rmdir(taskPath)

        return
