#!/usr/bin/env python
"""
_couch-trash_

"""

import os
import random
import time
import sys

from WMCore.Configuration import loadConfigurationFile
from WMCore.WMInit import connectToDB
from WMCore.WMInit import getWMBASE

from WMCore.Services.UUIDLib import makeUUID
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.File import File
from WMCore.FwkJobReport.Report import Report

lastJobID = int(sys.argv[1])

def createFile():
    """
    _createFile_

    Create a file with some random metdata.
    """
    newFile = File(lfn = makeUUID(), size = random.randrange(1024, 1048576, 1024),
                   events = random.randrange(10, 100000, 50),
                   parents = [File(lfn = makeUUID())],
                   locations = makeUUID())
    newFile["first_event"] = 0
    newFile["last_event"] = 0
    newFile["id"] = 1
    return newFile

def createJobs(totalJobs = 100):
    """
    _createJobs_

    """
    global lastJobID

    newJobs = []
    for i in range(totalJobs):
        newJob = Job(name = makeUUID(), files = [createFile(), createFile()])
        newJob["task"] = "SomeTask"
        newJob["workflow"] = "SomeWorkflow"
        newJob["owner"] = "sfoulkes@fnal.gov"
        newJob["id"] = lastJobID
        newJob["couch_record"] = None
        newJobs.append(newJob)
        lastJobID += 1

    return newJobs

def thrashCouch():
    """
    _thrashCouch_

    """
    jobs = {"new": set(), "created": set(), "executing": set(),
            "complete": set(), "success": set(), "cleanout": set()}

    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])
    changeState = ChangeState(config)

    myReport = Report()
    myReport.unpersist(os.path.join(getWMBASE(), "test/python/WMComponent_t/JobAccountant_t/fwjrs/LoadTest00.pkl"))

    for i in range(500):
        jobs = createJobs()
        changeState.recordInCouch(jobs, "created", "new")
        changeState.recordInCouch(jobs, "executing", "created")
        changeState.recordInCouch(jobs, "complete", "executing")

        for job in jobs:
            job["fwjr"] = myReport

        changeState.recordInCouch(jobs, "success", "complete")

        for job in jobs:
            job["fwjr"] = None

        changeState.recordInCouch(jobs, "cleanout", "success")
        #time.sleep(10)
    return

connectToDB()
thrashCouch()
