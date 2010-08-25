from WMCore.Configuration import Configuration
from os import environ, path
import WMCore.WMInit

config = Configuration()

config.component_('Webtools')
config.Webtools.application = 'WMBSMonitoring'
config.component_('WMBSMonitoring')

config.WMBSMonitoring.templates = path.join( WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools' )
config.WMBSMonitoring.admin = 'your@email.com'
config.WMBSMonitoring.title = 'WMBS Monitoring'
config.WMBSMonitoring.description = 'Monitoring of a WMBS instance'

config.WMBSMonitoring.section_('views')
# These are all the active pages that Root.py should instantiate
active = config.WMBSMonitoring.views.section_('active')
wmbs = active.section_('wmbs')
# The class to load for this view/page
wmbs.object = 'WMCore.WebTools.RESTApi'
wmbs.templates = path.join( WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
wmbs.section_('database')
wmbs.database.connectUrl = 'mysql://metson@localhost/wmbs'
# http://www.sqlalchemy.org/docs/reference/sqlalchemy/connections.html
#wmbs.database.database.engineParameters = {'pool_size': 10, 'max_overflow': 0}

wmbs.section_('model')
wmbs.model.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
wmbs.section_('formatter')
wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'