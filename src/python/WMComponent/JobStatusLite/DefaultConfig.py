#!/usr/bin/env python

"""
Defines default config values for JobStatusLite specific parameters.
"""

import os

from WMCore.Agent.Configuration import Configuration

__all__ = []

config = Configuration()
config.component_('JobStatusLite')
config.JobStatusLite.namespace = 'WMComponent.JobStatusLite.JobStatusLite'
config.JobStatusLite.componentDir = os.path.join(os.getcwd(), 'Components')
config.JobStatusLite.logLevel = 'INFO'
config.JobStatusLite.pollInterval = 180
config.JobStatusLite.queryInterval = 120
config.JobStatusLite.jobLoadLimit = 100
config.JobStatusLite.maxJobQuery = 100
config.JobStatusLite.taskLimit = 30
config.JobStatusLite.maxJobsCommit = 100
config.JobStatusLite.processes = 5
