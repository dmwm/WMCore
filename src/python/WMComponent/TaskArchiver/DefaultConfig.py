#!/usr/bin/env python

"""
Defines default config values for JobAccountant specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2009/10/30 13:43:05 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("JobArchiver")
config.JobArchiver.logLevel = 'INFO'
config.JobArchiver.pollInterval = 10


config.component_('JobStateMachine')
config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'cmssrv48.fnal.gov:5984')
config.JobStateMachine.default_retries = 1


