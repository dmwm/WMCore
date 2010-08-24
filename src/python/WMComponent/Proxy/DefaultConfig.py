#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for proxy specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2008/09/19 15:34:33 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"

import cPickle
import os
from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("Proxy")
#The log level of the component. 
config.Proxy.logLevel = 'DEBUG'
# maximum number of messages this proxy
# can retrieve from another proxy.
config.Proxy.maxMsg = 10
# how much delay between checking from remote
# proxies.
config.Proxy.contactIn = '00:00:3'
# specification of what the target
# proxies are (contact info) what
# we want from them (subscribe)

# below some examples of proxy information. We can define
# as many proxies as we want provided the parameter is prefixed
# with PXY_


# details will be a pickled dictionary
#details = {}
# these are default values for testing with format
# mysql://user:pass@....
#details['contact'] = os.getenv('PROXYDATABASE')
# subscription contains the default diagnostic messages
# Stop, Logging.Debug,.... and some special messages such
# as ProxySubscribe the latter is a signal to this proxy
# to subscribe the sender of this message to its payload message
#details['subscription'] = ['Logging.DEBUG','Logging.INFO','Logging.ERROR','Logging.NOTSET','LogState','Stop','JobSuccess','JobFailure','JobCreate']
#config.Proxy.PXY_Classic_1 = cPickle.dumps(details)

# details will be a pickled dictionary
#details = {}
# these are default values for testing with format
# mysql://user:pass@....
#details['contact'] = os.getenv('PROXYDATABASE')
#details['subscription'] = ['Stop','JobSuccess','JobFailure','JobCreate']
#config.Proxy.PXY_Classic_2= cPickle.dumps(details)



