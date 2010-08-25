#!/usr/bin/env python
"""
_Startup_

Runtime environment startup script
"""

import os

import WMCore.WMRuntime.Bootstrap as Bootstrap
import WMCore.FwkJobReport.Report as Report

if __name__ == '__main__':
    job = Bootstrap.loadJobDefinition()
    task = Bootstrap.loadTask(job)
    monitor = Bootstrap.setupMonitoring()

    Bootstrap.setupLogging(os.getcwd())

    task.build(os.getcwd())
    task.execute(job)

    if monitor.isAlive():
        monitor.shutdown()

    finalReport = Report.Report()
    taskSteps = task.listAllStepNames()
    for taskStep in taskSteps:
        if os.path.exists("./%s/Report.pkl" % taskStep):
            stepReport = Report.Report(taskStep)
            stepReport.unpersist("./%s/Report.pkl" % taskStep)
            finalReport.setStep(taskStep, stepReport.retrieveStep(taskStep))

    finalReport.persist("Report.pkl")
