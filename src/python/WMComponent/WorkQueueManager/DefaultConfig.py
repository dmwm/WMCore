#!/usr/bin/env python

"""
Defines default config values for JobAccountant specific
parameters.
"""
__all__ = []



from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("WorkQueueManager")

# set type of work queue
config.WorkQueueManager.level = "GlobalQueue"
#config.WorkQueueManager.level = "LocalQueue"

# In general can be left alone
config.WorkQueueManager.componentDir = config.General.WorkDir + "/WorkQueueManager"
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
#config.WorkQueueManager.logLevel = 'INFO'
#config.WorkQueueManager.pollInterval = 600


# RequestManager config
config.WorkQueueManager.reqMgrConfig = {}
# uncomment to override default reqMgr url
#config.WorkQueueManager.reqMgrConfig['endpoint'] = 'http://cmssrv49.fnal.gov:8585'
config.WorkQueueManager.reqMgrConfig['teamName'] = 'Dodgers'


# add parameters for global or local queue if default param is not what you want
config.WorkQueueManager.queueParams = {'LocationRefreshInterval': 10}
# uncomment to change CacheDir from default
#config.WorkQueueManager.queueParams['CacheDir'] = os.path.join(config.WorkQueueManager.componentDir, 'wf')

# Fill for local queue
if config.WorkQueueManager.level != "GlobalQueue":
    # url for the global queue
    config.WorkQueueManager.queueParams['ParentQueue'] = 'http://example.com:8080/wq'
# used to identify & contact this queue.
# if not provided will attempt to get from REST configuration
#config.WorkQueueManager.queueParams['QueueURL'] = 'http://%s' % os.uname()[1]
