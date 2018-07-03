#!/usr/bin/env python
"""
_Startup_

Runtime environment startup script.
Just a FYI, there are basically 3 important directories:
 1. the pilot home area, where the condor wrapper logs are
 2. the job space area, where the sandbox and the runtime log is created
 3. the task space area, where the steps and cmsRun logs are
"""
from __future__ import print_function

import logging
import os
import sys

import WMCore.WMRuntime.Bootstrap as Bootstrap

if __name__ == '__main__':
    logging.info("This log line goes to a parallel universe, but ... setting up logging")
    # WMAgent log has to be written to the pilot area in order to be transferred back
    Bootstrap.setupLogging(os.path.join(os.getcwd(), '../'))
    logging.info("Process id: %s\tCurrent working directory: %s", os.getpid(), os.getcwd())

    logging.info("Loading job definition")
    job = Bootstrap.loadJobDefinition()

    logging.info("Loading task")
    task = Bootstrap.loadTask(job)

    logging.info("Setting up monitoring")
    reportName = "Report.%i.pkl" % job['retry_count']

    Bootstrap.createInitialReport(job=job, reportName=reportName)
    monitor = Bootstrap.setupMonitoring(logName=reportName)

    logging.info("Building task at directory: %s", os.getcwd())
    task.build(os.getcwd())

    logging.info("Executing task at directory: %s", os.getcwd())
    task.execute(job)

    logging.info("Completing task at directory: %s", os.getcwd())
    finalReport = task.completeTask(jobLocation=os.getcwd(), reportName=reportName)
    logging.info("Shutting down monitor")
    os.fchmod(1, 0o664)
    os.fchmod(2, 0o664)
    if monitor.isAlive():
        monitor.shutdown()
    sys.exit(finalReport.getExitCode())
