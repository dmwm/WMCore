from WMCore.Agent.Configuration import Configuration
config = Configuration()
config.component_('Webtools')
config.Webtools.application = 'webtools'
config.Webtools.templates = '/Users/metson/WMCORE/src/templates/WMCore/WebTools'
config.Webtools.index = 'welcome'

config.Webtools.section_('views')
active = config.Webtools.views.section_('active')
config.Webtools.views.section_('maintenance')

active.section_('documentation')
active.documentation.object = 'WMCore.WebTools.Documentation'

active.section_('controllers')
active.controllers.object = 'WMCore.WebTools.Controllers'
active.controllers.css = {'reset': '/Users/metson/WT_Devel/osx105_ia32_gcc401/external/yui/2.2.2-wt/build/reset/reset.css', 
                          'cms_reset': '../../../css/WMCore/WebTools/cms_reset.css', 
                          'style': '../../../css/WMCore/WebTools/style.css'}
active.controllers.js = {}

active.section_('welcome')
active.welcome.object = 'WMCore.WebTools.Welcome'

active.section_('downloader')
active.downloader.object = 'WMComponent.HTTPFrontend.Downloader'
