from WMCore.Configuration import Configuration
from WMCore.WebTools.DefaultConfig import config
from os import environ, path
import sys
import WMCore.WMInit
#docconfig = config

# We have the document application config in doconfig, now start again...
#config = Configuration()

# This component has all the configuration of CherryPy
#config.component_('Webtools')

# This is the application
config.Webtools.application = 'Plotter'
config.Webtools.port = 8010
# This is the config for the application
config.component_('Plotter')
config.Plotter.admin = 'your@email.com'
config.Plotter.title = 'CMS Plotter'
config.Plotter.description = 'A tool to plot data.'
config.Plotter.templates = config.WebtoolsDocs.templates

# Annoyingly the yui config needs to be global
# TODO: make it localised to the controllers class some how...
#yui = config.Plotter.section_('yui')
#yui.root = "build"
#yui.base = "http://localhost:8080/controllers/yui/"
#yui.path = environ['YUI_ROOT']
#yui.concat = True
# Views are all pages 
config.Plotter.section_('views')
# These are all the active pages that Root.py should instantiate 
active = config.Plotter.views.section_('active')

#active += config.WebtoolsDocs.views.section_('active')

#active.section_('controllers') 
#active.controllers = config.WebtoolsDocs.views.active.controllers
active.section_('plotfairy')
active.plotfairy.object = 'WMCore.WebTools.RESTApi'
active.plotfairy.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
# Bit of a hack here we don't have a database so the baseclass throws
# an AssertionError - passing in an empty sqlite one is harmless here.
active.plotfairy.section_('database')
active.plotfairy.database.connectUrl = 'sqlite://'
# http://www.sqlalchemy.org/docs/reference/sqlalchemy/connections.html
#active.plotfairy.database.engineParameters = {'pool_size': 10, 'max_overflow': 0}
active.plotfairy.section_('model')
active.plotfairy.model.object = 'WMCore.HTTPFrontEnd.PlotFairy.Plotter'
active.plotfairy.section_('formatter')
active.plotfairy.formatter.object = 'WMCore.HTTPFrontEnd.PlotFairy.PlotFormatter'
