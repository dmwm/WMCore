from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

config.component_('Webtools')
config.Webtools.application = 'WorkQueueService'
config.component_('WorkQueueService')

config.WorkQueueServicetemplates = environ['WTBASE'] + '/templates/WMCore/WebTools'
config.WorkQueueService.admin = 'your@email.com'
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'

config.WorkQueueService.section_('views')
# These are all the active pages that Root.py should instantiate
active = config.WorkQueueService.views.section_('active')
wmbs = active.section_('workqueue')
# The class to load for this view/page
wmbs.object = 'WMCore.WebTools.RESTApi'
wmbs.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
wmbs.database = 'oracle://user:passwd@db:1521'

wmbs.section_('model')
wmbs.model.object = 'WMCore.HTTPFrontEnd.WorQueue.WorkQueueRESTModel'
wmbs.section_('formatter')
wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'