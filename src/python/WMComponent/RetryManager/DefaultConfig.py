#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []




from WMCore.Agent.Configuration import Configuration
import os
import os.path
import WMCore.WMInit
config = Configuration()
config.component_("RetryManager")
config.RetryManager.logLevel = 'DEBUG'
config.RetryManager.namespace = 'WMComponent.RetryManager.RetryManager'
config.RetryManager.maxRetries = 10
config.RetryManager.pollInterval = 10
#These are the cooloff times for the RetryManager, the times it waits
#Before attempting resubmission
config.RetryManager.coolOffTime  = {'create': 120, 'submit': 120, 'job': 120}
#Path to plugin directory
config.RetryManager.pluginPath = 'WMComponent.RetryManager.PlugIns'
config.RetryManager.pluginName = ''
config.RetryManager.WMCoreBase = WMCore.WMInit.getWMBASE()


jsm = config.component_('JobStateMachine')

if (os.getenv('COUCHURL') != None):
    jsm.couchurl = os.getenv('COUCHURL')
else:
    jsm.couchurl = 'cmssrv48.fnal.gov:5984'

jsm.default_retries = 1
