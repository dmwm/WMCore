#!/usr/bin/env python
"""
_Startup_

Runtime environment startup script
"""

import os

import WMCore.WMRuntime.Bootstrap as Bootstrap

if __name__ == '__main__':
    job = Bootstrap.loadJobDefinition()
    task = Bootstrap.loadTask(job)
    monitor = Bootstrap.setupMonitoring()

    Bootstrap.setupLogging(os.getcwd())

    task.build(os.getcwd())
    task.execute(job)

    task.completeTask(jobLocation = os.getcwd(),
                      logLocation = "Report.pkl")

    if monitor.isAlive():
        monitor.shutdown()

