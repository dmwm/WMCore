#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt
import logging

from WMCore.Agent.Configuration import Configuration

# Find and load the Configuration
config = Configuration()



####################
# General: General Settings Section
####################
config.section_("General")
config.General.workDir = '/tmp/TQ-test/'



######################
# Task Queue Component 
######################
config.component_("TQComp")
config.TQComp.namespace = "TQComp.TQComp"

#The log level of the component.
config.TQComp.logLevel = 'DEBUG'

# maximum number of threads we want to deal
# with messages per pool
config.TQComp.maxThreads = 5

# Component dir (twice due to some bug in wmcoreD)
config.TQComp.componentDir = '/tmp/TQ-test/TQComp'
config.TQComp.ComponentDir = '/tmp/TQ-test/TQComp'

# Where to download specs and sandboxes from
config.TQComp.downloadBaseUrl = 'http://glitece.ciemat.es:20030'

# Where to upload job reports to (uncomment to change default)
#config.TQComp.uploadBaseUrl = config.TQComp.downloadBaseUrl

# Root dir for all specs/sandboxes/reports on disk (PA directory)
# TODO: This should be set at some point by a joint PA+TQ installation
config.TQComp.specBasePath = \
         "/pool/TaskQueue/cms_code/work/pa/workdir/JobCreator/"
config.TQComp.sandboxBasePath = \
         "/pool/TaskQueue/cms_code/work/pa/workdir/JobCreator/"
config.TQComp.reportBasePath = \
         "/pool/TaskQueue/cms_code/work/pa/workdir/JobCreator/"

# Message handlers could be added as well
# (but there are none at this time)


#####################
# Task Queue Listener 
#####################
# Technically is not a different component, but part of TQComp
# However, we give it its own config section.
config.section_('TQListener')
config.TQListener.componentDir = '/tmp/TQ-test/TQComp'
config.TQListener.logLevel = 'DEBUG'

# Additional config for HTTP server, uncomment only if needed
#config.TQListener.httpServerConfig = '/pool/TaskQueue/my_code/http.conf'

# Log file for HTTP server (although it logs to component log file as well)
config.TQListener.httpServerLogFile = '/tmp/TQ-test/TQComp/httpServer.log'

# Listener port 
config.TQListener.httpServerPort = '20030'

# Following are for authentication (set the same in clients)
# If commented, no authentication is performed
#config.TQListener.httpServerUser = 'a'
#config.TQListener.httpServerPwd = 'pwda'
#config.TQListener.httpServerRealm = 'TQListener'

# Max number of threads for the listener (including its handlers)
config.TQListener.maxThreads = 20

# The formatter for messages (json).
# Do not change (uncomment) unless really sure!
#config.TQListener.formatter = "TQComp.DefaultFormatter"

# TQListener DB setting:
# In case we want to have a separate DB for this
# This is not yet implemented...
