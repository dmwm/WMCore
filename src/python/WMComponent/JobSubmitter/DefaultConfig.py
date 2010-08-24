#!/usr/bin/env python

"""
Defines default config values for JobSubmitter specific
parameters.
"""
__all__ = []




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
config.JobSubmitter.pollInterval  = 10
config.JobSubmitter.pluginName    = 'TestPlugin'
config.JobSubmitter.pluginDir     = 'JobSubmitter.Plugins'
config.JobSubmitter.submitDir     = os.path.join(os.getcwd(), 'submit')
config.JobSubmitter.submitNode    = os.getenv("HOSTNAME", 'badtest.fnal.gov')
config.JobSubmitter.submitScript  = os.path.join(os.getcwd(), 'submit.sh')
config.JobSubmitter.componentDir  = os.path.join(os.getcwd(), 'Components')
config.JobSubmitter.workerThreads = 1
config.JobSubmitter.jobsPerWorker = 100
config.JobSubmitter.inputFile     = os.path.join(os.getcwd(), 'FrameworkJobReport-4540.xml')

jsm = config.component_('JobStateMachine')

if (os.getenv('COUCHURL') != None):
    couchurl = os.getenv('COUCHURL')
else:
    couchurl = 'cmssrv48.fnal.gov:5984'

jsm.couchurl = couchurl
jsm.default_retries = 1
