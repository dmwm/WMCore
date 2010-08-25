from WMCore.Configuration import Configuration
from os import environ, path
import WMCore.WMInit

config = Configuration()

config.webapp_('WorkQueueService')
config.WorkQueueService.server.port = 8080
config.WorkQueueService.server.host = "hostname.fnal.gov"

config.WorkQueueService.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
config.WorkQueueService.admin = 'your@email.com'
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'

config.WorkQueueService.section_('views')
# These are all the active pages that Root.py should instantiate
active = config.WorkQueueService.views.section_('active')
workqueue = active.section_('workqueue')
# The class to load for this view/page
workqueue.object = 'WMCore.WebTools.RESTApi'
workqueue.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
workqueue.section_('database')
workqueue.database.connectUrl = 'mysql://username@hostname.fnal.gov:3306/TestDB'
#If you want to set up engineParameters take a look at 
# http://www.sqlalchemy.org/docs/reference/sqlalchemy/connections.html
#workqueue.database.engineParameters = {'pool_size': 10, 'max_overflow': 0}
#only needs to specify when mysql db is used
#if it is not specified gets the value from environment variable. (DBSOCK)
#workqueue.dbsocket = '/var//mysql.sock'
workqueue.section_('model')
workqueue.model.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
workqueue.section_('formatter')
workqueue.formatter.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTFormatter'
workqueue.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService',
                            'WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueMonitorService']

# take queueParams from WorkQueueManager - specify here to override
workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})

workqueuemonitor = active.section_('workqueuemonitor')
# The class to load for this view/page
workqueuemonitor.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'
workqueuemonitor.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
workqueuemonitor.javascript = path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
workqueuemonitor.html = path.join(WMCore.WMInit.getWMBASE(), 'src/html/')