#!/usr/bin/env python
"""
_Startup_

Runtime environment startup script
"""

import os
import WMCore.WMRuntime.Bootstrap as Bootstrap

if __name__ == '__main__':
    print "Startup.py : loading job definition"
    job = Bootstrap.loadJobDefinition()
    print "Startup.py : loading task"
    task = Bootstrap.loadTask(job)
    print "Startup.py : setting up monitoring"
    logLocation = "Report.%i.pkl" % job['retry_count']
    Bootstrap.createInitialReport(job = job,
                                  task = task,
                                  logLocation = logLocation)
    monitor = Bootstrap.setupMonitoring(logPath = logLocation)

    print "Startup.py : setting up logging"
    Bootstrap.setupLogging(os.getcwd())

    print "Startup.py : building task"
    task.build(os.getcwd())
    print "Startup.py : executing task"
    task.execute(job)
    print "Startup.py : completing task"
    task.completeTask(jobLocation = os.getcwd(),
                      logLocation = logLocation)
    print "Startup.py : shutting down monitor"
    os.fchmod(1, 0664)
    os.fchmod(2, 0664)
    if monitor.isAlive():
        monitor.shutdown()

