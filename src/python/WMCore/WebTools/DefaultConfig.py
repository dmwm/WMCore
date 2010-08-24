#
# This is an example configuration which loads the documentation classes for
# the webtools package. In general your application should have it's own 
# configuration and not use this, other than as a guideline.
#
from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')

# This is the application
config.Webtools.application = 'WebtoolsDocs'
# This is the config for the application
config.component_('WebtoolsDocs')
# Define the default location for templates for the app
config.WebtoolsDocs.templates = environ['WTBASE'] + '/templates/WMCore/WebTools'
config.WebtoolsDocs.admin = 'your@email.com'
config.WebtoolsDocs.title = 'CMS WMCore/WebTools Documentation'
config.WebtoolsDocs.description = 'Documentation on the WMCORE/WebTools'
# Define the class that is the applications index
#config.WebtoolsDocs.index = 'welcome'

# Views are all pages 
config.WebtoolsDocs.section_('views')
# These are all the active pages that Root.py should instantiate 
active = config.WebtoolsDocs.views.section_('active')
# The section name is also the location the class will be located
# e.g. http://localhost:8080/documentation
active.section_('documentation')
# The class to load for this view/page
active.documentation.object = 'WMCore.WebTools.Documentation'
# I could add a variable to the documenation object if I wanted to as follows:
# active.documentation.foo = 'bar'

#active.section_('welcome')
#active.welcome.object = 'WMCore.WebTools.Welcome'

# Controllers are standard way to return minified gzipped css and js
active.section_('controllers')
# The class to load for this view/page
active.controllers.object = 'WMCore.WebTools.Controllers'
# The configuration for this object - the location of css and js
active.controllers.css = {'reset': environ['YUIHOME'] + '/reset/reset.css', 
                          'cms_reset': '../../../css/WMCore/WebTools/cms_reset.css', 
                          'style': '../../../css/WMCore/WebTools/style.css'}
active.controllers.js = {}
# These are pages in "maintenance mode" - to be completed
maint = config.WebtoolsDocs.views.section_('maintenance')

active.section_('masthead')
active.masthead.object = 'WMCore.WebTools.Masthead'
active.masthead.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/Masthead'

active.section_('rest')
active.rest.object = 'WMCore.WebTools.RESTApi'
active.rest.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
active.rest.database = 'sqlite:////Users/metson/Documents/Workspace/GenDB/gendb.lite'
active.rest.section_('model')
active.rest.model.object = 'RESTModel'
active.rest.model.database = 'sqlite:////Users/metson/Documents/Workspace/GenDB/gendb.lite'
active.rest.model.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
