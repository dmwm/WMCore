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
import logging
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMComponent.RetryManager.Modifier.BaseModifier import BaseModifier


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

    def changeJobPkl(self, pklFile, jobPKL, newMemory):
        """
        Modifies the pklFile job.pkl by changing the estimatedMemoryUsage to a new_memory value

        """
        jobPKL['estimatedMemoryUsage'] = newMemory 
        self.savePKL(pklFile, jobPKL)

    def checkNewJobPkl(pklFile):
        with open(pklFile, 'rb') as file:
            data = pickle.load(file)
        print (data['estimatedMemoryUsage'])

    def getNewMemory(self, jobPKL, settings):
        maxMemPerCore = settings['maxMemory']/jobPKL['numberOfCores']
        currentMem = jobPKL['estimatedMemoryUsage']
        currentMemPerCore = currentMem/jobPKL['numberOfCores']

        if 'multiplyMemory' in settings:
            newMemPerCore = currentMemPerCore * settings['multiplyMemory']
        elif 'addMemory' in settings:
            newMemPerCore = currentMemPerCore + (settings['addMemory']/jobPKL['numberOfCores'])
        else:
            newMemPerCore = currentMemPerCore
            logging.info('No increment values were given in the MemoryModifier parameter')
            logging.info('No memory modification performed')

        if newMemPerCore > maxMemPerCore:
            newMemPerCore = maxMemPerCore
        return newMemPerCore * jobPKL['numberOfCores']
    
    def changeMemory(self, job, settings):
        """
        The "main" function in charge of modifying the memory before a retry. 
        It needs to modify the job.pkl file and the workflow sandbox
        """
        
        pklFile = '{}/job.pkl'.format(job['cache_dir']) 
        jobPKL = self.loadPKL(pklFile)

        newMemory = self.getNewMemory(jobPKL, settings)

        self.changeJobPkl(pklFile, jobPKL, newMemory)
        self.changeSandbox(jobPKL, newMemory)

    def modifyJob(self, job):
        try:
            settings = self.getModifierParam(job['jobType'], 'settings')
        except:
            logging.exception('Error while getting the MemoryModifier settings parameter. Not modifying memory')
            return
        
        if not 'requiresModify' in settings:
            logging.info('requiresModify not specified')
            logging.info('Not performing any modifications')
            return 

        elif not settings['requiresModify']:
            logging.info('requiresModify set to False')
            logging.info('Not performing any modifications')
            return
        
        else:
            self.changeMemory(job, settings)