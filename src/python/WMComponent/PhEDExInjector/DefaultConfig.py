#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for DBSUpload specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.3 2009/08/24 09:53:19 meloam Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("PhEDExInjector")
#The log level of the component. 
config.PhEDExInjector.logLevel = 'INFO'

#
# JobSuccess Handler
#
config.PhEDExInjector.pollInterval = 60 * 30 # half hour
config.PhEDExInjector.newInjectionHandler = \
    'WMComponent.PhEDExInjector.NewInjectionHandler'
config.PhEDExInjector.phedexurl = \
    'https://localhost:9999/phedex/datasvc/json/tbedi/'
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

#config.DBSUpload.pollThread = \
#    'WMComponent.DBSUpload.Handler.PollDBSUpload'

config.DBSUpload.dbsurl = \
                        'http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet'
#    'http://cmssrv17.fnal.gov:8989/DBSAnzar/servlet/DBSServlet'

config.DBSUpload.dbsversion = \
                            'DBS_2_0_6'
#    'DBS_2_0_4'
# Number of files taht will be Batch inserted into DBS
config.DBSUpload.uploadFileMax = 10

config.DBSUpload.pollInterval = 10


##config.DBSUpload.pollThread = \
##    'WMComponent.DBSUpload.Handler.PollDBSUpload'
#
#config.DBSUpload.dbsurl = \
#                        'http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet'
##    'http://cmssrv17.fnal.gov:8989/DBSAnzar/servlet/DBSServlet'
#
#config.DBSUpload.dbsversion = \
#                            'DBS_2_0_6'
##    'DBS_2_0_4'
## Number of files taht will be Batch inserted into DBS
#config.DBSUpload.uploadFileMax = 10
#
#config.DBSUpload.pollInterval = 10

