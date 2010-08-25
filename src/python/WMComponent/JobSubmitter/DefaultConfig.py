#!/usr/bin/env python

"""
Defines default config values for JobSubmitter specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2009/10/07 19:33:30 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.Configuration import Configuration
import os
import os.path


config = Configuration()

config.component_("WMAgent")
config.WMAgent.WMSpecDirectory = os.getcwd()

config.component_("JobSubmitter")
#The log level of the component. 
config.JobSubmitter.logLevel = 'INFO'
# maximum number of threads we want to deal
# with messages per pool.
config.JobSubmitter.maxThreads = 1
#
# JobSubmitter
#
config.JobSubmitter.pollInterval = 10
config.JobSubmitter.pluginName   = 'TestPlugin'
config.JobSubmitter.pluginDir    = 'WMComponent.JobSubmitter.Plugins'
config.JobSubmitter.submitDir    = os.path.join(os.getcwd(), 'submit')
config.JobSubmitter.submitNode   = os.getenv("HOSTNAME", 'badtest.fnal.gov')

jsm = config.component_('JobStateMachine')

if (os.getenv('COUCHURL') != None):
    couchurl = os.getenv('COUCHURL')
else:
    couchurl = 'cmssrv48.fnal.gov:5984'

jsm.couchurl = couchurl
jsm.default_retries = 1
