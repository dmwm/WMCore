import os
import pickle
import logging
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMComponent.RetryManager.Modifier.BaseModifier import BaseModifier
from WMCore.FwkJobReport.Report import Report

class MemoryModifier(BaseModifier):

    def __init__(self):
        BaseModifier.__init__(self)

    def getWorkload():
        """
        _getWorkload_

        
        """
        # Creates copy of original sandbox, unzips it, and retrieves the path to the uncompressed sandbox
        sandboxPath=self.getJobSandbox()

        pklPath=sandboxPath+'WMSandbox/WMWorkload.pkl'

        configHandle = open(pklPath, "rb")
        workload = pickle.load(configHandle)
        configHandle.close()

        return workload

    def setWorkload(self, workload):
        """
        _setWorkload_

        
        """
        pklPath=self.sandboxPath+'WMSandbox/WMWorkload.pkl'

        #Pkl the modified object
        with open(pklPath, 'wb') as pf:
            pickle.dump(worker, pf)
        
        self.updateSandbox()

    ### German ###
    def changeSandbox(self, job, newMemory):
        """
        _changeSandbox_

        Modifies the parameter maxPSS in the sandbox. This is a change that applies for all jobs in that workflow that remain to be submitted 
        """

        # figure how to get the path
        sandboxPath = ""

        workload = self.getWorkload(sandboxPath)
        workHelper = WMWorkloadHelper(workload)

        for task in workHelper.getAllTasks():
            task.setMaxPSS(newMemory)

        self.setWorkload(workload)

        return

    ### Antonio ###
    def getCacheDirectory(self, job):
        """
        returns the cache directory of a job
        """
        pass
    
    def getJobPkl(self, cacheDir):
        """
        returns the jobPkl file in a job's cache directory 
        """
        pass
    
    def changeJobPkl(self, jobPkl):
        """
        Modifies the pkl_file job.pkl by changing the estimatedMemoryUsage to a new_memory value
        """     
        
        with open(jobPkl, 'rb') as file:
            data = pickle.load(file)

        if data['estimatedMemoryUsage'] >= self.maxMemory:
            pass
        elif data['estimatedMemoryUsage'] + self.addedMemory > self.maxMemory:
            data['estimatedMemoryUsage'] = self.maxMemory
        else:
            data['estimatedMemoryUsage'] += self.addedMemory

        with open('job.pkl', 'wb') as file:
            pickle.dump(data, file)

        return
    
    def changeMemory(self, job, newMemory):
        """
        The "main" function in charge of modifying the memory before a retry. 
        It needs to modify the job.pkl file and the workflow sandbox
        It gets the cachedir from the database. There it has the sandbox and the job.pkl file accessible
        It only does it if the configuration has the line config.RetryManager.MemoryModifier.default.requiresModify = True
        """
        if self.requiresModify:
            cacheDir = self.getCacheDirectory(job=job)
            jobPkl = self.getJobPkl(cacheDir=cacheDir) 

            self.changeJobPkl(jobPkl, newMemory)
            self.changeSandbox(newMemory=newMemory)
            return
        else:
            return

    def isReady(self, job):
        
        # Memory will not be modified  unless explicitely specified in the configuration
        requiresModify = self.getModifierParam(job['jobType'], param=requiresModify, defaultReturn = False) 
        maxMemory = self.getModifierParam(job['jobType'], param=maxMemory, defaultReturn = '16000')
        addedMemory = self.getModifierParam(job['jobType'], param=addedMemory, defaultReturn = '2000')  

        try:
            report     = Report()
            reportPath = os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count'])
            report.load(reportPath)
        except:
            # If we're here, then the FWJR doesn't exist.
            # Give up, run it again
            return True

        if report.getExitCode() == self.exitCode:
            self.changeMemory(job=job, newMemory=newMemory)
            return True
        else:
            logging.info('Not changing memory for job without exit code 50660')
            return True



    #def checkNewJobPkl(jobPkl):
        #with open(jobPkl, 'rb') as file:
        #   data = pickle.load(file)
        #print (data['estimatedMemoryUsage'])

    

    ### Alternativa? ###
    
    #from WMCore.WMBS.Job import Job

    #def addMemory(self, jobList, additionalMemory):
        #for job in jobList:
        #    job.addResourceEstimates(memory = additionalMemory)
        
        #return
    


    