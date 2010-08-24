import socket
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2008/10/30 03:06:21 rpw Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.Configuration import Configuration
from HTTPFrontend.JobQueueMonitor import JobQueueMonitor


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
config.HTTPFrontEnd.components = ['HTTPFrontend.JobQueueMonitor']
config.HTTPFrontEnd.ComponentDir = '/home/rpw/work/install/HTTPFrontend'
