#
# This is an example configuration which loads the documentation classes for
# the webtools package. In general your application should have it's own 
# configuration and not use this, other than as a guideline.
#
from WMCore.Configuration import Configuration
from os import environ

config = Configuration()
config.component_('Webtools')
config.Webtools.application = 'webtools'
config.Webtools.templates = environ['WTBASE'] + '/templates/WMCore/WebTools'
config.Webtools.index = 'welcome'

config.Webtools.section_('views')
active = config.Webtools.views.section_('active')
config.Webtools.views.section_('maintenance')

active.section_('documentation')
active.documentation.object = 'WMCore.WebTools.Documentation'

active.section_('controllers')
active.controllers.object = 'WMCore.WebTools.Controllers'
active.controllers.css = {'reset': environ['YUIHOME'] + '/reset/reset.css', 
                          'cms_reset': '../../../css/WMCore/WebTools/cms_reset.css', 
                          'style': '../../../css/WMCore/WebTools/style.css'}
active.controllers.js = {}

active.section_('welcome')
active.welcome.object = 'WMCore.WebTools.Welcome'