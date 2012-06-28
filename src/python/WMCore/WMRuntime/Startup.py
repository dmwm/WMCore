#!/usr/bin/env python
"""
_Startup_

Runtime environment startup script
"""

import os
import datetime
import WMCore.WMRuntime.Bootstrap as Bootstrap

if __name__ == '__main__':
    print "Startup.py : %s : loading job definition" % datetime.isoformat()
    job = Bootstrap.loadJobDefinition()
    print "Startup.py : %s : loading task" % datetime.isoformat()
    task = Bootstrap.loadTask(job)
    print "Startup.py : %s : setting up monitoring" % datetime.isoformat()
    logLocation = "Report.%i.pkl" % job['retry_count']
    Bootstrap.createInitialReport(job = job,
                                  task = task,
                                  logLocation = logLocation)
    monitor = Bootstrap.setupMonitoring(logPath = logLocation)

    print "Startup.py : %s : setting up logging" % datetime.isoformat()
    Bootstrap.setupLogging(os.getcwd())

    print "Startup.py : %s : building task" % datetime.isoformat()
    task.build(os.getcwd())
    print "Startup.py : %s : executing task" % datetime.isoformat()
    task.execute(job)
    print "Startup.py : %s : completing task" % datetime.isoformat()
    task.completeTask(jobLocation = os.getcwd(),
                      logLocation = logLocation)
    print "Startup.py : %s : shutting down monitor" % datetime.isoformat()
    os.fchmod(1, 0664)
    os.fchmod(2, 0664)
    if monitor.isAlive():
        monitor.shutdown()

