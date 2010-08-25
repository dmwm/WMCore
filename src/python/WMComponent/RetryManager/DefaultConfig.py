#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2009/05/11 16:49:04 afaq Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("RetryManager")
#The log level of the component. 
config.RetryManager.logLevel = 'DEBUG'
#The namespace of the component
config.RetryManager.namespace = 'WMComponent.RetryManager.RetryManager'
# maximum number of threads we want to deal
# with messages per pool.
config.RetryManager.maxThreads = 30
# maximum number of retries we want for job
config.RetryManager.maxRetries = 10
# depending on the application an operator can reconfigure what we use.
# but these are the default settings.

config.RetryManager.createCooloffHandler = \
    'WMComponent.RetryManager.Handler.DefaultCreate'
config.RetryManager.submitCooloffHandler = \
    'WMComponent.RetryManager.Handler.DefaultSubmit'
config.RetryManager.jobCooloffHandler = \
    'WMComponent.RetryManager.Handler.DefaultJob'


