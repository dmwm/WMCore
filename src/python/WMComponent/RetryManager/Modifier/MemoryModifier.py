#!/usr/bin/env python

"""
_MemoryModifier_

This particular modifier is in charge of incrementing memory to jobs that require it before resuming them.
Note: It does not resume the job.

It takes a failed job with exit code 50660 and it proceeds to modify the estimatedMemoryUsage in job.pkl and the maxPSS in the task
The MemoryModifier will only proceed to modify the memory if its properly enabled for the job type of the job in question.

Example: 
# To enable MemoryModifier:
config.RetryManager.modifiers = {50660: 'MemoryModifier'}
config.RetryManager.section_('MemoryModifier')

# To configure the setup for a specific job type: 
config.RetryManager.MemoryModifier.section_('Processing')
config.RetryManager.MemoryModifier.Processing.settings = {'requiresModify': True, 'multiplyMemoryPerCore': 1.5, 'maxMemoryPerCore': 2000}

# To configure default job type:
config.RetryManager.MemoryModifier.section_('default')
config.RetryManager.MemoryModifier.default.settings = {'requiresModify': True, 'multiplyMemoryPerCore': 1.5, 'maxMemoryPerCore': 2000}

# All together usage:

config.RetryManager.modifiers = {50660: 'MemoryModifier'}
config.RetryManager.section_('MemoryModifier')
config.RetryManager.MemoryModifier.section_('default')
config.RetryManager.MemoryModifier.default.settings = {'requiresModify': False, 'multiplyMemoryPerCore': 1.5, 'maxMemoryPerCore': 2000}
config.RetryManager.MemoryModifier.section_('Processing')
config.RetryManager.MemoryModifier.Processing.settings = {'requiresModify': False, 'multiplyMemoryPerCore': 1.2, 'maxMemoryPerCore': 2000}
config.RetryManager.MemoryModifier.section_('merge')
config.RetryManager.MemoryModifier.merge.settings = {'requiresModify': False, 'multiplyMemoryPerCore': 1.2, 'maxMemoryPerCore': 2000}

dataDict = {'someTask': {
                         'maxPSS': [firstMaxPSS, secondMaxPSS, ...], 
                         'jobs': {
                                  'someJobId': [firstEstimatedMemoryUsage, secondEstimatedMemoryUsage, ...],
                                  'anotherJobId': [...]
                                  }
                         'jobIDs': [firstJobId, secondJobId ...]
                         } 
            }
sample dataDict:
{
    "/Repack_Run382810_StreamALCAPHISYM_Tier0_REPLAY_2024_ID240708022442_v8022442/Repack": {
        "maxPSS": [
            250.0,
            500.0,
            1000.0,
            2000.0
        ],
        "jobs": {
            "102": [
                250.0, 
                500.0, 
                1000.0, 
                2000.0
            ],
            "103": [
                250.0, 
                500.0
            ],
            "1547": [
                250.0, 
                500.0, 
                1000.0
            ]
        },
        "jobIDs": [
            102,
            103,
            1547,
            102,
            1547,
            102
        ]
    },
    ...
}

"""
import time
import os
import pickle
import logging
from WMCore.WMSpec.WMTask import WMTaskHelper
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
            
        self.setWorkload(workload, jobPKL)

        return
    
    def changeMemoryForTask(self, taskPath, jobPKL, newMemory):
        """
        __changeMemoryForTask__

        Gets workload to get access to job task
        Sets new maxPSS at task level
        """
        workload = self.getWorkload(jobPKL)
        workHelper = WMWorkloadHelper(workload)
        task = workHelper.getTaskByPath(taskPath)
        task.setMaxPSS(newMemory)
        self.setWorkload(workload, jobPKL)

    def changeJobPkl(self, pklFile, jobPKL, newMemory):
        """
        __changeJobPkl__

        Modifies the pklFile job.pkl by changing the estimatedMemoryUsage to a new_memory value
        """
        logging.info('Modifying %s job pkl file estimatedMemoryUsage parameter. Previous value: %d. New value: %d', jobPKL['jobType'], jobPKL['estimatedMemoryUsage'], newMemory)
        jobPKL['estimatedMemoryUsage'] = newMemory 
        self.savePKL(pklFile, jobPKL)

    def getNewMemory(self, taskPath, currentMemory, numberOfCores, settings):
        """
        __getNewMemory__

        Determines the new memory for the retry
        New memory should not be greater than max memory in settings parameter
        New memory should be greater than or equal to current memory in task (i.e maxPSS)
        """
        maxMemPerCore = settings['maxMemoryPerCore']
        currentMemPerCore = currentMemory/numberOfCores

        if 'multiplyMemoryPerCore' in settings:
            newMemPerCore = currentMemPerCore * settings['multiplyMemoryPerCore']
        elif 'addMemoryPerCore' in settings:
            newMemPerCore = currentMemPerCore + settings['addMemoryPerCore']
        else:
            newMemPerCore = currentMemPerCore

        if newMemPerCore > maxMemPerCore:
            newMemPerCore = maxMemPerCore
            logging.info('Task %s is now running with the maximum allowed maxPSS by configuration', taskPath)

        newMemory = newMemPerCore * numberOfCores

        if self.dataDict[taskPath]['maxPSS'][-1] > newMemory:
            newMemory = self.dataDict[taskPath]['maxPSS'][-1] 
            
        return newMemory
    
    def changeMemory(self, job, settings):
        """
        __changeMemory__

        The "main" function in charge of modifying the memory before a retry. 
        Modifies job.pkl first
        Modifies task maxPSS second, if newMemory exceeds current maxPSS
        """

        pklFile = '{}/job.pkl'.format(job['cache_dir']) 
        jobPKL = self.loadPKL(pklFile)

        currentMemory = jobPKL['estimatedMemoryUsage']
        numberOfCores = jobPKL['numberOfCores']
        currentJob    = jobPKL['id']
        taskPath      = self.getTaskPath(jobPKL)

        if not taskPath in self.dataDict:
            taskData = {'maxPSS': [currentMemory], 'jobs': {}, 'jobIDs': []}
            self.dataDict[taskPath] = taskData

        newMemory = self.getNewMemory(taskPath, currentMemory, numberOfCores, settings)
        
        ### TESTING ###
        logging.info('Checking task {}'.format(taskPath))
        #logging.info('TEST: Pre-Mod dataDict: {}'.format(self.dataDict))
        ### TESTING ###

        # Changing job.pkl estimatedMemoryUsage
        
        self.changeJobPkl(pklFile, jobPKL, newMemory)
        oldMemory     = currentMemory
        currentMemory = jobPKL['estimatedMemoryUsage'] #i.e. newMemory
        
        if not currentJob in self.dataDict[taskPath]['jobs']:
            self.dataDict[taskPath]['jobs'][currentJob] = [oldMemory, currentMemory]
        else:
            self.dataDict[taskPath]['jobs'][currentJob].append(currentMemory)

        self.dataDict[taskPath]['jobIDs'].append(currentJob)

        ### TESTING ###
        #logging.info('TEST: Post-Mod dataDict: {}'.format(self.dataDict))
        ### TESTING ###

        if self.dataDict[taskPath]['maxPSS'][-1] < newMemory:
            self.changeMemoryForTask(taskPath, jobPKL, newMemory)
            self.dataDict[taskPath]['maxPSS'].append(newMemory)
            logging.info('Successfully modified maxPSS. Old maxPSS: %d. New maxPSS: %d', self.dataDict[taskPath]['maxPSS'][-2], self.dataDict[taskPath]['maxPSS'][-1])
        
        self.writeDataDict(self.dataDictJson, self.dataDict)
        logging.debug('Done handling job.pkl and task. Now updating dataDict.json')

    def modifyJob(self, job):

        try:
            settings = self.getModifierParam(job['jobType'], 'settings')
        except:
            logging.debug('Did not get settings parameter for job type %s. Not modifying memory', job['jobType'])
            return

        if not 'requiresModify' in settings:
            logging.debug('Configuration for job type %s does not specify requiresModify. Not modifying memory for job %d', job['jobType'], job['id'])
            return 

        elif not settings['requiresModify']:
            logging.debug('Configuration for job type %s has requiresModify set to False. Not modifying memory for job %d', job['jobType'], job['id'])
            return
        
        else:
            logging.info('Modifying memory for job %d of job type %s', job['id'], job['jobType'])
            self.changeMemory(job, settings)
