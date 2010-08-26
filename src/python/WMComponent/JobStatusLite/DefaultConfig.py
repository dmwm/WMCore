#!/usr/bin/env python

"""
Defines default config values for JobStatusLite specific parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.4 2010/07/26 17:06:36 mcinquil Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_('JobStatusLite')
config.JobStatusLite.namespace       = 'WMComponent.JobStatusLite.JobStatusLite'
config.JobStatusLite.componentDir    = os.path.join(os.getcwd(), 'Components')
config.JobStatusLite.logLevel        = 'INFO'
config.JobStatusLite.pollInterval    = 180
config.JobStatusLite.queryInterval   = 120
config.JobStatusLite.jobLoadLimit    = 100
config.JobStatusLite.maxJobQuery     = 100
config.JobStatusLite.taskLimit       = 30
config.JobStatusLite.processes       = 5
