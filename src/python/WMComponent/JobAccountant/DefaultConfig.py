#!/usr/bin/env python

"""
Defines default config values for JobAccountant specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2009/07/28 21:41:18 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("JobAccountant")
#The log level of the component. 
config.JobAccountant.logLevel = 'INFO'

config.JobAccountant.pollInterval = 10


config.component_("DBSBuffer")
#The log level of the component. 
config.DBSBuffer.logLevel = 'INFO'
#The namespace of the buffer. Used to load the module as deamon
config.DBSBuffer.namespace = 'WMComponent.DBSBuffer.DBSBuffer'
# maximum number of threads we want to deal
# with messages per pool.
config.DBSBuffer.maxThreads = 1
#
# JobSuccess Handler
#
config.DBSBuffer.jobSuccessHandler = \
    'WMComponent.DBSBuffer.Handler.JobSuccess'

jsm = config.component_('JobStateMachine')

#if (os.getenv('COUCHURL') != None):
#    couchurl = os.getenv('COUCHURL')
#else:
#    couchurl = 'localhost:5984'

jsm.couchurl = 'cmssrv48.fnal.gov:5984'
jsm.default_retries = 1
