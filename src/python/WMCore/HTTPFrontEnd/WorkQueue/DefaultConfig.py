from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

config.component_('Webtools')
config.Webtools.application = 'WorkQueueService'
config.Webtools.port = 6660
config.Webtools.host = "cmssrv18.fnal.gov"
config.component_('WorkQueueService')

config.WorkQueueService.templates = environ['WTBASE'] + '/templates/WMCore/WebTools'
config.WorkQueueService.admin = 'your@email.com'
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'

config.WorkQueueService.section_('views')
# These are all the active pages that Root.py should instantiate
active = config.WorkQueueService.views.section_('active')
workqueue = active.section_('workqueue')
# The class to load for this view/page
workqueue.object = 'WMCore.WebTools.RESTApi'
workqueue.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
workqueue.database = 'mysql://user@cmssrv188.fnal.gov:3307/TestDB'
workqueue.section_('model')
workqueue.model.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
workqueue.section_('formatter')
workqueue.formatter.object = 'WMCore.WebTools.DASRESTFormatter'