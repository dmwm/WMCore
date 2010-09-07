#!/usr/bin/env python
"""
Defines default config values for GetOutputLite specific parameters.
"""

import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_('GetOutputLite')
config.GetOutputLite.namespace       = 'WMComponent.GetOutputLite.GetOutputLite'
config.GetOutputLite.componentDir    = os.path.join(os.getcwd(), 'Components')
config.GetOutputLite.logLevel        = 'DEBUG'
config.GetOutputLite.loadlimit       = 200
config.GetOutputLite.pollInterval    = 10
config.GetOutputLite.processes       = 5
