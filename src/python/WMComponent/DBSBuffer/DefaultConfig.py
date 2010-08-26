#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for DBSBuffer specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.3 2010/02/05 14:16:14 meloam Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("DBSBuffer")
#The log level of the component. 
config.DBSBuffer.logLevel = 'INFO'
#The namespace of the buffer. Used to load the module as daemon
config.DBSBuffer.namespace = 'WMComponent.DBSBuffer.DBSBuffer'
# maximum number of threads we want to deal
# with messages per pool.
config.DBSBuffer.maxThreads = 1
#
# JobSuccess Handler
#
config.DBSBuffer.jobSuccessHandler = \
    'WMComponent.DBSBuffer.Handler.JobSuccess'

