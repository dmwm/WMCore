#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for DBSUpload specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.2 2009/07/17 21:18:30 sryu Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Agent.Configuration import Configuration
import os


config = Configuration()
config.component_("JobCreator")
config.JobCreator.namespace = 'WMComponent.JobCreator.JobCreator'
#The log level of the component. 
config.JobCreator.logLevel = 'INFO'

# maximum number of threads we want to deal
# with messages per pool.
config.JobCreator.maxThreads = 1
#
# JobCreator
#

config.JobCreator.pollInterval = 10



jsm = config.component_('JobStateMachine')

if (os.getenv('COUCHURL') != None):
    couchurl = os.getenv('COUCHURL')
else:
    couchurl = 'localhost:5984'

jsm.couchurl = couchurl
jsm.default_retries = 1
