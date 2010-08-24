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

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from PhEDExNotifier.PhEDExNotifierComponent import PhEDExNotifierComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("PhEDExNotifier")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg




compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
#  //
# // Initialise and start the component
#//

createDaemon(compCfg['ComponentDir'])
component = PhEDExNotifierComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])
