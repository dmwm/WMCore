#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.7 2009/07/28 21:27:38 mnorman Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.Configuration import Configuration

config = Configuration()

config.component_("Agent")
config.Agent.componentName = "ErrorHandler"


config.component_("ErrorHandler")
#The log level of the component. 
config.ErrorHandler.logLevel = 'DEBUG'
#The namespace of the component
config.ErrorHandler.namespace = 'WMComponent.ErrorHandler.ErrorHandler'
# maximum number of threads we want to deal
# with messages per pool.
config.ErrorHandler.maxThreads = 30
# maximum number of retries we want for job
config.ErrorHandler.maxRetries = 10
# The poll interval at which to look for failed jobs
config.ErrorHandler.pollInterval = 60


# depending on the application an operator can reconfigure what we use.
# but these are the default settings.



jsm = config.component_('JobStateMachine')

jsm.couchurl = 'cmssrv48.fnal.gov:5984'
jsm.default_retries = 1


