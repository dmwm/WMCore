from __future__ import (division, print_function)

import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner

from WMCore.Services.Rucio.Rucio import WMRucioDIDNotFoundException

def format_timestamp(timestamp_float):
    """Converts a float timestamp (seconds since epoch) to a readable string."""
    # This format gives you: "2025-12-09 19:22:15"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp_float))


class InputDataRucioRuleCleaner(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(InputDataRucioRuleCleaner, self).__init__(config)
        self.globalQ = globalQueue(logger=self.logger, **config.queueParams)
        self.msRuleCleaner = MSRuleCleaner(config.msRuleCleaner, logger=self.logger)  # Initialize MSRuleCleaner

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
        
        tStart = time.time()
        
        #statuses = ['Available', 'Done', 'Acquired', 'Failed', 'Canceled']
        #globalQueueElements=self.globalQ.getWork({'Status':'Done'},siteJobCounts={})
        globalQueueElements=self.globalQ.backend.getElements()
            
        #print("Elements in GlobalQueue cleanRucioRules:")
        #print(json.dumps(globalQueueElements,indent=2))
        
        do_cleaning = False

        if globalQueueElements:
            #print(f"Found {len(globalQueueElements)} elements in GlobalQueue")
            current_time = format_timestamp(time.time())
            self.logger.info(f"{current_time}: Found {len(globalQueueElements)} globalqueue elements.")

            for element in globalQueueElements:
                
                requestName = element.get('RequestName')  # Extract the RequestName field
                percentComplete = element.get('PercentComplete', 0)  # Default to 0 if key is missing
                percentSuccess = element.get('PercentSuccess', 0)  # Default to 0 if key is missing

                if percentComplete == 100 and percentSuccess == 100:

                    #to be able to use cleanRules method of MSRuleCleaner
                    rulesToClean = {'PlineMarkers':['Current'], 'RulesToClean': {'Current': []}, 'CleanupStatus': {'Current': []}}

                    #'Inputs': {'/MinimumBias/ComissioningHI-v1/RAW#372d624c-089d-11e1-8347-003048caaace':
                    blocks = element.get('Inputs')  # Example key for dataset
                                                       
                    # Fetch rules for blocks
                    cleanedRules_info = {}
                    if blocks:
                        for block in blocks:
                            #print("Adding block ", block, " to RulesToClean")
                            dataCont = block.split('#')[0]  # Extract the container name from the block
                            
                            if dataCont in self.msRuleCleaner.globalLocks:
                                msg = "Found dataset: %s in GlobalLocks. NOT considering it for filling the "
                                msg += "RulesToClean list for both container and block level Rules for workflow: %s!"
                                self.logger.info(msg, dataCont, requestName)
                                continue
                            try:
                                #print('Fetching rules for block:', block, "\n", config.rucioAccount, "\n", self.msRuleCleaner.rucio.listDataRules(block, account=config.rucioAccount))
                                rules = self.msRuleCleaner.rucio.listDataRules(block, account=config.msRuleCleaner['rucioAccount'])
                                #found rules for this block. If the rules of this block already cleaned there is no rules found
                                if rules:
                                    cleanedRules_info[block] = {}
                                    #one block can have multiple rules
                                    cleanedRules_info[block]['id'] = []
                                    cleanedRules_info[block]['bytes'] = []
                                    for rule in rules:
                                        #msg = "Found %s block-level rule to be deleted for container %s"
                                        #self.logger.info(msg, rule['id'], dataCont)
                                        #current_time = format_timestamp(time.time())
                                        #self.logger.info(f"{current_time}: Rule {rule['id']} {block} {rule['bytes']} {requestName} to be cleaned")
                                        cleanedRules_info[block]['id'].append(rule['id'])
                                        cleanedRules_info[block]['bytes'].append(rule['bytes'])
                                        #cleanRules of MSRuleCleaner expects a list of rule ids and always clean the last one in the list of PlineMarkers
                                        rulesToClean['RulesToClean'][rulesToClean['PlineMarkers'][-1]].append(rule['id'])
                            except WMRucioDIDNotFoundException:
                                msg = "Block: %s not found in Rucio for workflow: %s."
                                self.logger.info(msg, block, requestName)
                                continue
                    
                    if cleanedRules_info:
                        current_time = format_timestamp(time.time())
                        self.logger.info(f"{current_time}: Start cleaning rules for completed element {element.id}")
                        
                        do_cleaning = True

                        for block, info in cleanedRules_info.items():
                            for rule_id, size in zip(info["id"], info["bytes"]):
                                self.logger.info(f"{current_time} Rule to clean: {rule_id} {block} {size} {requestName}")
                        
                        self.msRuleCleaner.cleanRucioRules(rulesToClean)
                        
                        current_time = format_timestamp(time.time())
                        self.logger.info(f"{current_time}: End cleaning rules for completed element {element.id}")
            
            if not do_cleaning:
                current_time = format_timestamp(time.time())
                self.logger.info(f"{current_time} No cleaning happened: There are no completed workqueue elements or rules already cleaned")    
            
            #current_time = format_timestamp(time.time())
            #self.logger.info(f"{current_time}: {self.__class__.__name__} executed in {(time.time() - tStart):.3f} secs.")
            #tmp = rulesToClean['RulesToClean'][rulesToClean['PlineMarkers'][-1]]
            #ids = ''
            #for rid in tmp:
            #    ids += rid + ', '
            #    rulesToClean['CleanupStatus']['Current'].append({'RuleID': rid, 'Status': 'Pending'})
            #self.logger.info('Rules to be cleaned: %s', ids)
            #return rulesToClean
            #return self.msRuleCleaner.cleanRucioRules(rulesToClean)

        else:
            current_time = format_timestamp(time.time())
            self.logger.info(f"{current_time} No elements found in GlobalQueue")
        
        current_time = format_timestamp(time.time())
        self.logger.info(f"{current_time} {self.__class__.__name__} executed in {(time.time() - tStart):.3f} secs.")
        return do_cleaning