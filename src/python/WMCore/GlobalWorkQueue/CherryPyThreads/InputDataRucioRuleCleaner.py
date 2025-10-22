from __future__ import (division, print_function)

from time import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner

from WMCore.Services.Rucio.Rucio import WMRucioDIDNotFoundException

class InputDataRucioRuleCleaner(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(InputDataRucioRuleCleaner, self).__init__(config)
        self.globalQ = globalQueue(logger=self.logger, **config.queueParams)
        self.msRuleCleaner = MSRuleCleaner(config, logger=self.logger)  # Initialize MSRuleCleaner

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.cleanRucioRules, 'duration': config.cleanInputDataRucioRuleDuration}]

    def cleanRucioRules(self, config):
        """
        Queries global queue and builds the list of blocklevel Rucio rules of finished elements to be deleted. Calls MSRuleCleaner cleanRucioRules(self, wflow) to delete the rules.
        :config:       The configuration for the task. This uses Rucio account from config to use for querying rules
        :return:       The result of MSRuleCleaner cleanRucioRules(self, wflow) method, which is True if all rules were deleted successfully, False otherwise.
        """
        
        tStart = time()
        
        #statuses = ['Available', 'Done', 'Acquired', 'Failed', 'Canceled']
        #globalQueueElements=self.globalQ.getWork({'Status':'Done'},siteJobCounts={})
        globalQueueElements=self.globalQ.backend.getElements()
            
        #print("Elements in GlobalQueue cleanRucioRules:")
        #print(json.dumps(globalQueueElements,indent=2))

        #to be able to use cleanRules method of MSRuleCleaner
        rulesToClean = {'PlineMarkers':['Current'], 'RulesToClean': {'Current': []}, 'CleanupStatus': {'Current': []}} 

        if globalQueueElements:
            #print(f"Found {len(globalQueueElements)} elements in GlobalQueue")
            for element in globalQueueElements:
                
                requestName = element.get('RequestName')  # Extract the RequestName field
                percentComplete = element.get('PercentComplete', 0)  # Default to 0 if key is missing
                percentSuccess = element.get('PercentSuccess', 0)  # Default to 0 if key is missing

                
                if percentComplete == 100 and percentSuccess == 100:
                                        
                    #'Inputs': {'/MinimumBias/ComissioningHI-v1/RAW#372d624c-089d-11e1-8347-003048caaace':
                    blocks = element.get('Inputs')  # Example key for dataset
                                                       
                    # Fetch rules for blocks
                    if blocks:
                        for block in blocks:
                            print("Adding block ", block, " to RulesToClean")
                            dataCont = block.split('#')[0]  # Extract the container name from the block
                            
                            if dataCont in self.msRuleCleaner.globalLocks:
                                msg = "Found dataset: %s in GlobalLocks. NOT considering it for filling the "
                                msg += "RulesToClean list for both container and block level Rules for workflow: %s!"
                                self.logger.info(msg, dataCont, requestName)
                                continue
                            try:
                                print('Fetching rules for block:', block, "\n", config.rucioAccount, "\n", self.msRuleCleaner.rucio.listDataRules(block, account=config.rucioAccount))
                                for rule in self.msRuleCleaner.rucio.listDataRules(block, account=config.rucioAccount):
                                    msg = "Found %s block-level rule to be deleted for container %s"
                                    self.logger.info(msg, rule['id'], dataCont)
                                    #cleanRules of MSRuleCleaner expects a list of rule ids and always clean the last one in the list of PlineMarkers
                                    rulesToClean['RulesToClean'][rulesToClean['PlineMarkers'][-1]].append(rule['id'])
                            except WMRucioDIDNotFoundException:
                                msg = "Block: %s not found in Rucio for workflow: %s."
                                self.logger.info(msg, block, requestName)
                                continue
            
            self.logger.info("%s executed in %.3f secs.", self.__class__.__name__, time() - tStart)
            return self.msRuleCleaner.cleanRucioRules(rulesToClean)

        else:
            print("No elements with status DONE found in GlobalQueue")

        self.logger.info("%s executed in %.3f secs.", self.__class__.__name__, time() - tStart)
        return