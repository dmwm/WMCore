#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for DBSUpload specific
parameters.
"""
__all__ = []



import os

from WMCore.Agent.Configuration import Configuration


config = Configuration()

config.section_("CoreDatabase")
if (os.getenv('DATABASE') == None):
    raise RuntimeError, \
          "You must set the DATABASE environment variable to run tests"
config.CoreDatabase.connectUrl = os.getenv("DATABASE")
config.CoreDatabase.dialect = os.getenv('DIALECT', None)
config.CoreDatabase.socket = os.getenv("DBSOCK")


config.component_("DBSUpload")
#The log level of the component.
config.DBSUpload.logLevel = 'DEBUG'

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

#config.DBSUpload.pollThread = \
#    'WMComponent.DBSUpload.Handler.PollDBSUpload'

config.DBSUpload.dbsurl = 'http://cmssrv49.fnal.gov:8989/DBS209P5_2/servlet/DBSServlet'
#    'http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet'
#    'http://cmssrv17.fnal.gov:8989/DBSAnzar/servlet/DBSServlet'

config.DBSUpload.dbsversion = \
                            'DBS_2_0_9'
#    'DBS_2_0_4'
# Number of files taht will be Batch inserted into DBS
config.DBSUpload.uploadFileMax = 500

config.DBSUpload.pollInterval = 10
config.DBSUpload.globalDBSUrl = 'http://cmssrv49.fnal.gov:8989/DBS209P5_2/servlet/DBSServlet'
#config.DBSUpload.globalDBSUrl = 'http://cmssrv49.fnal.gov:8989/DBS209/servlet/DBSServlet'
#config.DBSUpload.globalDBSUrl = 'http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet'
config.DBSUpload.globalDBSVer = 'DBS_2_0_9'

#Config variables for block sizes in DBS
config.DBSUpload.DBSMaxSize      = 999999999
config.DBSUpload.DBSMaxFiles     = 2
config.DBSUpload.DBSBlockTimeout = 10000000
