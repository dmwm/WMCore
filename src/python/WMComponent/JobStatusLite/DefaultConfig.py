#!/usr/bin/env python

"""
Defines default config values for JobStatusLite specific parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2010/05/13 15:55:46 mcinquil Exp $"
__version__ = "$Revision: 1.1 $"

import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_('JobStatusLite')
config.JobStatusLite.namespace       = 'WMComponent.JobStatusLite.JobStatusLite'
config.JobStatusLite.componentDir    = '/home/cinquilli/Development/DMWM/WMAgent/work/JobStatusLite'
config.JobStatusLite.ComponentDir    = '/tmp/application/JobStatusLite'
config.JobStatusLite.logLevel        = 'INFO'
config.JobStatusLite.pollInterval    = 10
config.JobStatusLite.queryInterval   = 2
config.JobStatusLite.jobLoadLimit    = 100
config.JobStatusLite.maxJobQuery     = 100
config.JobStatusLite.taskLimit       = 30
config.JobStatusLite.processes       = 5
