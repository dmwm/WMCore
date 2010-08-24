#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for DBSUpload specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2008/10/08 21:19:33 afaq Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("DBSUpload")
#The log level of the component. 
config.DBSUpload.logLevel = 'INFO'

# maximum number of threads we want to deal
# with messages per pool.
config.DBSUpload.maxThreads = 1
#
# JobSuccess Handler
#
config.DBSUpload.bufferSuccessHandler = \
    'WMComponent.DBSUpload.Handler.BufferSuccess'

