#!/usr/bin/env python

"""
Defines default config values for JobTracker specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2009/10/02 21:28:45 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import os
import os.path

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("JobTracker")
config.JobTracker.logLevel      = 'INFO'
config.JobTracker.pollInterval  = 10
config.JobTracker.trackerName   = 'TestTracker'
config.JobTracker.pluginDir     = 'WMComponent.JobTracker.Plugins'
config.JobTracker.runTimeLimit  = 7776000 #Jobs expire after 90 days
config.JobTracker.idleTimeLimit = 7776000
config.JobTracker.heldTimeLimit = 7776000
config.JobTracker.unknTimeLimit = 7776000


config.component_('JobStateMachine')
config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'cmssrv48.fnal.gov:5984')
config.JobStateMachine.default_retries = 1


