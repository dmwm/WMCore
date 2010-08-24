########################
# the default config should work out of the box with minimal change
# Under the '## User specific parameter' line need to be changed to make the config correctly
########################

from WMCore.Configuration import Configuration
from os import environ, path
import WMCore.WMInit

config = Configuration()

config.component_('Webtools')
config.Webtools.application = 'AgentMonitoring'
config.component_('AgentMonitoring')

config.AgentMonitoring.templates = path.join( WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools' )
## User specific parameter:
config.AgentMonitoring.admin = 'your@email.com'
config.AgentMonitoring.title = 'WMAgent Monitoring'
config.AgentMonitoring.description = 'Monitoring of a WMAgentMonitoring'

config.AgentMonitoring.section_('views')
# These are all the active pages that Root.py should instantiate
active = config.AgentMonitoring.views.section_('active')
wmagent = active.section_('wmagent')
# The class to load for this view/page
wmagent.object = 'WMCore.WebTools.RESTApi'
wmagent.templates = path.join( WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
wmagent.section_('database')
## User specific parameter:
wmagent.database.connectUrl = 'mysql://metson@localhost/wmagent'
# http://www.sqlalchemy.org/docs/reference/sqlalchemy/connections.html
#wmagent.database.database.engineParameters = {'pool_size': 10, 'max_overflow': 0}

wmagent.section_('model')
wmagent.model.object = 'WMCore.HTTPFrontEnd.Agent.AgentRESTModel'
wmagent.section_('formatter')
wmagent.formatter.object = 'WMCore.WebTools.RESTFormatter'

wmagentmonitor = active.section_('wmagentmonitor')
# The class to load for this view/page
wmagentmonitor.object = 'WMCore.HTTPFrontEnd.Agent.AgentMonitorPage'
wmagentmonitor.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
wmagentmonitor.javascript = path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
wmagentmonitor.html = path.join(WMCore.WMInit.getWMBASE(), 'src/html/')
