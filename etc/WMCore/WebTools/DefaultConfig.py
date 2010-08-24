from WMCore.Configuration import Configuration
config = Configuration()
config.component_('Webtools')
config.Webtools.application = 'webtools'
config.Webtools.templates = '/Users/metson/WMCORE/src/templates/WMCore/WebTools'
config.Webtools.index = 'welcome'

config.Webtools.section_('views')
config.Webtools.views.section_('active')
config.Webtools.views.section_('maintenance')

config.Webtools.views.active.section_('documentation')
config.Webtools.views.active.documentation.object = 'WMCore.WebTools.Documentation'

config.Webtools.views.active.section_('controllers')
config.Webtools.views.active.controllers.object = 'WMCore.WebTools.Controllers'
config.Webtools.views.active.controllers.css = {'reset': '/Users/metson/WT_Devel/osx105_ia32_gcc401/external/yui/2.2.2-wt/build/reset/reset.css', 
                                                'cms_reset': '../../../css/WMCore/WebTools/cms_reset.css', 
                                                'style': '../../../css/WMCore/WebTools/style.css'}
config.Webtools.views.active.controllers.js = {}

config.Webtools.views.active.section_('welcome')
config.Webtools.views.active.welcome.object = 'WMCore.WebTools.Welcome'