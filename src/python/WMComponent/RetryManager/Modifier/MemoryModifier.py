#!/usr/bin/env python

"""
_MemoryModifier_

config.RetryManager.section_('MemoryModifier')
config.RetryManager.MemoryModifier.section_('default')
config.RetryManager.MemoryModifier.default.maxMemory = 2000 # Memory/cpu in MB
config.RetryManager.MemoryModifier.section_('merge')
config.RetryManager.MemoryModifier.merge.maxMemory = 3000 # Memory/cpu in MB
"""

import pickle
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMComponent.RetryManager.Modifier.BaseModifier import BaseModifier
import pickle


class MemoryModifier(BaseModifier):

    def changeSandbox(self, jobPKL, newMemory):
        """
        _changeSandbox_

        Modifies the parameter maxPSS in the sandbox. This is a change that applies for all jobs in that workflow that remain to be submitted 
        """

        workload = self.getWorkload(jobPKL)
        workHelper = WMWorkloadHelper(workload)

        for task in workHelper.getAllTasks():
            task.setMaxPSS(newMemory)

        self.setWorkload(workload)

        return

    def changeJobPkl(self, jobPKL, newMemory):
        """
        Modifies the pklFile job.pkl by changing the estimatedMemoryUsage to a new_memory value

        """
        jobPKL['estimatedMemoryUsage'] = newMemory
        pklFile = '{}/job.pkl'.format(jobPKL['cache_dir']) 
        self.savePKL(pklFile,data)


    def checkNewJobPkl(pklFile):
        with open(pklFile, 'rb') as file:
            data = pickle.load(file)
        print (data['estimatedMemoryUsage'])

    def getNewMemory(self, jobPKL):
        maxMemPerCore = self.getModifierParam(job['jobType'], 'maxMemory')
        #Finds job.pkl file

        currentMem = jobPKL['estimatedMemoryUsage']
        currentMemPerCore = currentMem/jobPKL['numberOfCores']
        newMemPerCore = currentMemPerCore * 1.5
        if newMemPerCore > maxMemPerCore:
            newMemPerCore = maxMemPerCore
        return maxMemPerCore * jobPKL['numberOfCores']

    def changeMemory(self, job):
        """
        The "main" function in charge of modifying the memory before a retry. 
        It needs to modify the job.pkl file and the workflow sandbox
        """
        pklFile = '{}/job.pkl'.format(job['cache_dir']) 
        jobPKL = self.loadPKL(pklFile)
        newMemory = self.getNewMemory(jobPKL)

        self.changeJobPkl(jobPKL, newMemory)
        self.changeSandbox(jobPKL, newMemory)

    def modifyJob(self, job):
        self.changeMemory(job)