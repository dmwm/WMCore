#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.5 2009/05/12 11:13:12 afaq Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.Configuration import Configuration

config = Configuration()
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
# The poll interval at which to look for new filesets
config.ErrrorHandler.pollInterval = 60


# depending on the application an operator can reconfigure what we use.
# but these are the default settings.


