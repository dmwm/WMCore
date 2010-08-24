import socket
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.8 2009/01/26 23:48:58 rpw Exp $"
__version__ = "$Revision: 1.8 $"


from WMCore.Configuration import Configuration

config = Configuration()
config.component_("HTTPFrontEnd")
#The log level of the component. 
config.HTTPFrontEnd.logLevel = 'DEBUG'
# maximum number of threads we want to deal
# with messages per pool.
config.HTTPFrontEnd.maxThreads = 30
# maximum number of retries we want for job
config.HTTPFrontEnd.maxRetries = 10
config.HTTPFrontEnd.Port = 8585
config.HTTPFrontEnd.Logfile = None
config.HTTPFrontEnd.HTTPLogfile = None
config.HTTPFrontEnd.Host = socket.gethostname()
config.HTTPFrontEnd.ThreadPool = 10
config.HTTPFrontEnd.JobCreatorCache = None
#config.HTTPFrontEnd.components = ['ReqMgr.Component.AssignmentManager.AssignmentManager',
#                                  'ReqMgr.Component.RequestDataService.RequestDataService']
config.HTTPFrontEnd.ComponentDir = '/home/rpw/work/install/HTTPFrontend'

import WMCore.WebTools.DefaultConfig
config += WMCore.WebTools.DefaultConfig.config
active = WMCore.WebTools.DefaultConfig.active

active.section_('download')
active.download.object = 'WMCore.HTTPFrontEnd.Downloader'
active.download.dir = '/home/rpw/work'

active.section_('assignmentManager')
active.assignmentManager.object = 'ReqMgr.Component.AssignmentManager.AssignmentManager'
active.assignmentManager.requestSpecDir= active.download.dir
active.assignmentManager.Host = config.HTTPFrontEnd.Host
active.assignmentManager.Port = config.HTTPFrontEnd.Port

active.section_('requestDataService')
active.requestDataService.object = 'ReqMgr.Component.RequestDataService.RequestDataService'
