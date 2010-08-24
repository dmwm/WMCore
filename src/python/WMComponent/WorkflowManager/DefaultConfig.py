#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for WorkflowManager specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.3 2009/02/05 23:26:34 jacksonj Exp $"
__version__ = "$Revision: 1.3 $"

import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("WorkflowManager")
config.WorkflowManager.logLevel = "INFO"
config.WorkflowManager.componentName = "WorkflowManager"
config.WorkflowManager.componentDir = \
    os.path.join(os.getenv("TESTDIR"), "WorkflowManager")

# The maximum number of threads to process each message type
config.WorkflowManager.maxThreads = 10

# The poll interval at which to look for new filesets
config.WorkflowManager.pollInterval = 60