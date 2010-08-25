from WMCore.Configuration import Configuration
from os import environ, path
import sys
config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')

# This is the application
config.Webtools.application = 'Plotter'
config.Webtools.port = 8010
# This is the config for the application
config.component_('Plotter')
config.Plotter.admin = 'your@email.com'
config.Plotter.title = 'CMS Plotter'
config.Plotter.description = 'A tool to plot data.'

# Annoyingly the yui config needs to be global
# TODO: make it localised to the controllers class some how...
yui = config.Plotter.section_('yui')
yui.root = "2.7.0b/build"
yui.base = "http://localhost:8080/controllers/yui/"
yui.path = environ['YUI_ROOT']
yui.concat = True
# Views are all pages 
config.Plotter.section_('views')
# These are all the active pages that Root.py should instantiate 
active = config.Plotter.views.section_('active')

# Controllers are standard way to return minified, gzipped css and js
# Should probably be renamed base
active.section_('controllers')
# The class to load for this view/page
active.controllers.object = 'WMCore.WebTools.Controllers'
# The configuration for this object - the location of css and js
active.controllers.css = {'reset': environ['WTBASE'] +  '/reset/reset.css', 
                          'cms_reset': environ['WTBASE'] + '/css/WMCore/WebTools/cms_reset.css', 
                          'style': environ['WTBASE'] + '/css/WMCore/WebTools/style.css',
                          'sites': environ['SITEDBBASE'] + '/css/sites.css',
                          'fonts-min': environ['YUI_ROOT'] + '/fonts/fonts-min.css',
                          'container': environ['YUI_ROOT'] + 'container/assets/container.css'}
active.controllers.js = {'sitestatus': environ['SITEDBBASE'] + '/js/sitestatus.js',
                         'yahoo-dom-event': environ['YUI_ROOT'] + '/yahoo-dom-event/yahoo-dom-event.js',
                         'json': environ['YUI_ROOT'] + '/json/json-min.js',
                         'connection': environ['YUI_ROOT'] + '/connection/connection-min.js',
                         }

active.section_('plotfairy')
active.plotfairy.object = 'WMCore.WebTools.RESTApi'
active.plotfairy.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
# Bit of a hack here we don't have a database so the baseclass throws
# an AssertionError - passing in an empty sqlite one is harmless here.
active.plotfairy.database = 'sqlite://'
active.plotfairy.section_('model')
active.plotfairy.model.object = 'WMCore.HTTPFrontEnd.PlotFairy.Plotter'
active.plotfairy.section_('formatter')
active.plotfairy.formatter.object = 'WMCore.HTTPFrontEnd.PlotFairy.PlotFormatter'