from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

config.component_('Webtools')
config.Webtools.application = 'WMBSMonitoring'
config.component_('WMBSMonitoring')

config.WMBSMonitoring.templates = environ['WTBASE'] + '/templates/WMCore/WebTools'
config.WMBSMonitoring.admin = 'your@email.com'
config.WMBSMonitoring.title = 'WMBS Monitoring'
config.WMBSMonitoring.description = 'Monitoring of a WMBS instance'

config.WMBSMonitoring.section_('views')
# These are all the active pages that Root.py should instantiate
active = config.WMBSMonitoring.views.section_('active')
wmbs = active.section_('wmbs')
# The class to load for this view/page
wmbs.object = 'WMCore.WebTools.RESTApi'
wmbs.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
wmbs.database = 'mysql://metson@localhost/wmbs'

wmbs.section_('model')
wmbs.model.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
wmbs.section_('formatter')
wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'