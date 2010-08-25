"""
WMCore/HTTPFrontEnd/WorkQueue/MonitorModelConfig.py

Configuration for WorkQueue REST monitoring application
WMCore/HTTPFrontEnd/WorkQueue/WorkQueueMonitoringModel.py

"""

__revision__ = "$Id: MonitorModelConfig.py,v 1.1 2010/01/26 15:14:41 maxa Exp $"
__version__ = "$Revision: 1.1 $"


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
sqliteDbUrl = "sqlite:///" + os.environ["WTBASE"] + "/python/WMCore/HTTPFrontEnd/WorkQueue/dbworkqueue.sqlite"
workqueuemonitor.database = os.environ["DATABASE"] or sqliteDbUrl
# in case of MySQL running in a non-default socket, it should be picked up in
# DatabasePage from DBSOCK env. variable, optionally set explicitly:
# workqueuemonitor.dbsocket = os.environ["DBSOCK"]

workqueuemonitor.templates = os.environ["WTBASE"] + "/templates/WMCore/WebTools/"
workqueuemonitor.section_("model")
workqueuemonitor.model.object = "WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorModel"

workqueuemonitor.section_("formatter")

# TODO
# 'WMCore.WebTools.DASRESTFormatter' will be used
workqueuemonitor.formatter.object = "WMCore.WebTools.RESTFormatter"