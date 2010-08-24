#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for DBSUpload specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.4 2009/01/14 22:06:57 afaq Exp $"
__version__ = "$Revision: 1.4 $"


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
    
config.DBSUpload.newWorkflowHandler = \
    'WMComponent.DBSUpload.Handler.NewWorkflowHandler'

config.DBSUpload.dbsurl = \
    'http://cmssrv17.fnal.gov:8989/DBSAnzar/servlet/DBSServlet'

config.DBSUpload.dbsversion = \
    'DBS_2_0_4'
# Number of files taht will be Batch inserted into DBS
config.DBSUpload.uploadFileMax = 10

