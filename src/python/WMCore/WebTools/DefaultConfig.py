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
import logging
config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')
# We could change port, set logging etc here like so:
#config.Webtools.port = 8011
#config.Webtools.show_tracebacks = True
config.Webtools.autoreload = True
config.Webtools.environment = 'development'
config.Webtools.log_screen = True
config.Webtools.error_log_level = logging.INFO
#config.Webtools.thread_pool = 10
# etc. Check Root.py for all configurables
# The above short-hand can be replaced with explicit namespaced configuration
# variables as described in http://www.cherrypy.org/wiki/ConfigAPI
# for example
config.Webtools.section_('server')
config.Webtools.server.socket_timeout = 30
# Shorthand configurations take precedence over explicit ones, e.g. if you have
#config.Webtools.server.socket_port = 8010
#config.Webtools.port = 8011
# your server will start on 8011
# This is the application
config.Webtools.application = 'WebtoolsDocs'

# This is the config for the application
config.component_('WebtoolsDocs')
# Define the default location for templates for the app
config.WebtoolsDocs.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools')
config.WebtoolsDocs.admin = 'your@email.com'
config.WebtoolsDocs.title = 'CMS WMCore/WebTools Documentation'
config.WebtoolsDocs.description = 'Documentation on the WMCORE/WebTools'

# If we want an application to run on multiple for multiple instances confiure
# it as follows
config.WebtoolsDocs.instances = ['foo', 'bar']
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
# You can turn off security by setting
config.SecurityModule.dangerously_insecure = True
# There should be a proper HMAC key file here, use this file as an
# example
#config.SecurityModule.key_file = os.path.join(getWMBASE(), 'src/python/WMCore/WebTools/DefaultConfig.py')
#
# I could secure all the pages in the web app to these settings using a default
# configuration. This can still be over-ridden on a per page basis.
#default = config.SecurityModule.section_('default')
#default.role = ''
#default.group = ''
#default.site = ''

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
# for more option check
# http://www.sqlalchemy.org/docs/reference/sqlalchemy/connections.html
#active.rest.database.engineParameters = {'pool_size': 10, 'max_overflow': 10, 'pool_timeout': 30}
#active.rest.section_('model')
#active.rest.model.object = 'RESTModel'
#active.rest.section_('formatter')
#active.rest.formatter.object = 'RESTFormatter'
# You could override the templates/database here, for instance:
#active.rest.formatter.templates = os.path.join(WMCore.WMInit.getWMBASE(), '/src/templates/WMCore/WebTools/')
