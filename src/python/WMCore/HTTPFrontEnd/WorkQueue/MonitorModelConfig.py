"""
WMCore/HTTPFrontEnd/WorkQueue/MonitorModelConfig.py

Configuration for WorkQueue REST monitoring application
WMCore/HTTPFrontEnd/WorkQueue/Services/WorkQueueMonitorService.py

The purpose is just for testing configuration when wanted to test a 
stand-along REST application ; may be removed later.

2010-02-04 - it doesn't work since WorkQueueMonitorService no longer
    inherits from WorkQueueRESTModel ...

"""

__revision__ = "$Id: MonitorModelConfig.py,v 1.2 2010/02/06 01:16:29 maxa Exp $"
__version__ = "$Revision: 1.2 $"


import os
from WMCore.Configuration import Configuration


config = Configuration()
config.component_("Webtools")
# application's name
config.Webtools.application = "WorkQueueMonitor"
config.component_("WorkQueueMonitor")

# the application will be mounted on / if index is provided
config.WorkQueueMonitor.title = "REST Monitoring for WorkQueue"
config.WorkQueueMonitor.description = "REST Monitoring for WorkQueue"
config.WorkQueueMonitor.admin = "your@email.com"
config.WorkQueueMonitor.templates = os.environ["WTBASE"] + "/templates/WMCore/WebTools/"

config.WorkQueueMonitor.section_("views")
# these are all the active pages that Root.py should instantiate
active = config.WorkQueueMonitor.views.section_("active")

workqueuemonitor = active.section_("workqueuemonitor")
workqueuemonitor.object = "WMCore.WebTools.RESTApi"

# database configuration - use env. setting "DATABASE" if set or local SQLite
sqliteDbUrl = "sqlite:////workqueuemonitor.sqlite"
workqueuemonitor.database = os.environ["DATABASE"] or sqliteDbUrl
# in case of MySQL running in a non-default socket, it should be picked up in
# DatabasePage from DBSOCK env. variable, optionally set explicitly:
# workqueuemonitor.dbsocket = os.environ["DBSOCK"]

workqueuemonitor.templates = os.environ["WTBASE"] + "/templates/WMCore/WebTools/"
workqueuemonitor.section_("model")
workqueuemonitor.model.object = "WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueMonitorService"

workqueuemonitor.section_("formatter")

workqueuemonitor.formatter.object = "WMCore.WebTools.RESTFormatter"