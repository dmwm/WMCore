#
# This is an example configuration which loads the documentation classes for
# the webtools package and shows you how to configure the various different 
# classes available. Your application should have it's own configuration in it's
# CVS area and not use this, other than as a guideline. Applications deployed at
# CERN will need to meet the CERN web service SLA:
#     https://twiki.cern.ch/twiki/bin/view/CMS/DMWTServiceLevelAgreement 
# This includes committing configuration files to appropriate locations in CVS.
#
from WMCore.Configuration import Configuration
from WMCore.WMBase import getWMBASE
import os.path
from os import environ

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')
# We could change port, set logging etc here like so:
#config.Webtools.port = 8011
#config.Webtools.environment = development
# etc. Check Root.py for all configurables

# This is the application
config.Webtools.application = 'WebtoolsDocs'

# This is the config for the application
config.component_('WebtoolsDocs')
# Define the default location for templates for the app
config.WebtoolsDocs.templates = os.path.join(getWMBASE(), '/src/templates/WMCore/WebTools')
config.WebtoolsDocs.admin = 'your@email.com'
config.WebtoolsDocs.title = 'CMS WMCore/WebTools Documentation'
config.WebtoolsDocs.description = 'Documentation on the WMCORE/WebTools'

# We could define the class that is the applications index
#config.WebtoolsDocs.index = 'welcome'
# but instead we'll leave it blank and use the default (Welcome.py) which 
# inspects the pages that are loaded and auto-generates a page based on the 
# classes doc strings. You can hide a view from the welcome page by setting 
# hidden=True in its configuration - useful for "admin" pages.

# Views are all pages 
config.WebtoolsDocs.section_('views')
# These are all the active pages that Root.py should instantiate 
active = config.WebtoolsDocs.views.section_('active')
# This is the Security config the application will use
config.component_('SecurityModule')
#config.SecurityModule.app_url = 'https://cmsweb.cern.ch/myapp'
#config.SecurityModule.app_url = 'http://%s:%d' % (config.Webtools.host,config.Webtools.port)
config.SecurityModule.mount_point = 'auth'
config.SecurityModule.store = 'filestore'
config.SecurityModule.store_path = '/tmp/security-store'
#config.CernOpenID.store.database = 'sqlite://'
config.SecurityModule.session_name = 'SecurityModule'
config.SecurityModule.oid_server = 'http://localhost:8400/'
config.SecurityModule.handler = 'WMCore.WebTools.OidDefaultHandler'
# I could secure all the pages in the web app to these settings like so
#default = config.SecurityModule.section_('default')
#default.role = ['Admin']
#default.group = ['CMS']
#default.site = ['T2_BR_UERJ']

# The section name is also the location the class will be located
# e.g. http://localhost:8080/documentation
active.section_('documentation')
# The class to load for this view/page
active.documentation.object = 'WMCore.WebTools.Documentation'
# I could add a variable to the documenation object if I wanted to as follows:
# active.documentation.foo = 'bar'

# I can reuse a class
active.section_('secretdocumentation')
active.secretdocumentation.object = 'WMCore.WebTools.Documentation'
# I don't want the world to see the secret documents on the welcome page. 
active.secretdocumentation.hidden = True

# I can use an openID secured class
active.section_('securedocumentation')
active.securedocumentation.object = 'WMCore.WebTools.SecureDocumentation'

#active.section_('welcome')
#active.welcome.object = 'WMCore.WebTools.Welcome'

# These are pages in "maintenance mode" - to be completed
maint = config.WebtoolsDocs.views.section_('maintenance')

# This is how you would configure a RESTful service
# You need to install py2-sqlalchemy to be able to use it. Put it on the
# spec file of your webtools package
#active.section_('rest')
#active.rest.object = 'WMCore.WebTools.RESTApi'
#active.rest.templates =os.path.join(WMCore.WMInit.getWMBASE(), '/src/templates/WMCore/WebTools/' )
# Dummy in memory SQLite DB
#active.rest.database = 'sqlite://'
#active.rest.section_('model')
#active.rest.model.object = 'RESTModel'
#active.rest.section_('formatter')
#active.rest.formatter.object = 'RESTFormatter'
# You could override the templates/database here, for instance:
#active.rest.formatter.templates = os.path.join(WMCore.WMInit.getWMBASE(), '/src/templates/WMCore/WebTools/')
