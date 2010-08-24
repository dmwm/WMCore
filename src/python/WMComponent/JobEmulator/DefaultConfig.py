#!/usr/bin/env python

"""
Auto generated stub be careful with editing,
Inheritance is preferred.
"""

import os


from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("JobEmulator")
#The log level of the component.
config.JobEmulator.logLevel = "INFO"

# a simulation can be direct (submission and tracker are
# included in the jobemulator. Or it can be indirect: submission
# and tracker plugin are inserted in the job submitter and job tracker.
config.JobEmulator.direct = True

#PLUGINS:
# plugin to use. There are two options for running the simulation:
# (1): plugin in the submitter and tracker in their respective components
# (2): Go directly to the jobemulator and plug submitter and tracker there.
config.JobEmulator.submitter = "WMComponent.JobEmulator.Plugin.Submitter.Default"
config.JobEmulator.tracker = "WMComponent.JobEmulator.Plugin.Tracker..."
# there are other plugins for scheduling and runtime behaviour of the job.
config.JobEmulator.scheduler = "WMComponent.JobEmulator.Plugin.Scheduler.LoadBalance"
config.JobEmulator.completion = "WMComponent.JobEmulator.Plugin.Completion.Random"
config.JobEmulator.tracker = "WMComponent.JobEmulator.Plugin.Tracker.Default"
config.JobEmulator.report = "WMComponent.JobEmulator.Plugin.Report.Default"

# PARAMETERS:
config.JobEmulator.avgCompletionTime = "00:00:00"
config.JobEmulator.avgSuccessRate = "0.9"
# the rate indicates the rate of the number of jobs completing all the events
# among all the successful jobs.
# The number of incomplete events among the jobs follows the gauss distribution
# with maximun number (totalEvent -1, minimun 1)
# the mean value (70% of total event) and standard deviation (15% of the width of total event)
# is hard coded. If it is necessary they can be parameterized.
config.JobEmulator.avgEventProcessRate = "0.95"
config.JobEmulator.thresholdForMerge = "0"

# This goes into the general section. As long as the type here is the
# same as the defined somewhere else it will just be overwritten.
config.section_("General")
# JobCache location. This is a non jobemulator specific parameter.
config.General.jobCache = os.path.join(os.getenv('TESTDIR'), 'JobCache')
