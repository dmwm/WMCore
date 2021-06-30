#!/usr/bin/env python
from __future__ import division, print_function

import unittest

import logging

from WMCore.ReqMgr.CherryPyThreads.StatusChangeTasks import moveForwardStatus
from WMCore.ReqMgr.DataStructs.RequestError import InvalidStateTransition
from WMCore.ReqMgr.DataStructs.RequestStatus import check_allowed_transition


class MockReqMgr():
    def __init__(self, logger):
        self.logger = logger
        self.currentStatus = None
        self.cacheWflowStatus = {}
        self.logger.info("MockReqMgr: Started local MockReqMgr")

    def getRequestByStatus(self, statusList, detail=True):
        """ Mock the 'getRequestByStatus' method"""
        logging.info("MockReqMgr: getting requests by status: %s, with detail= %s",
                     statusList, detail)
        requestsDict = {"staged": ["wflow1", "wflow2"],
                        "acquired": ["wflow3", "wflow4"],
                        "running-open": ["wflow5", "wflow6"],
                        "running-closed": ["wflow7", "wflow8"],
                        "force-complete": ["wflow9", "wflow10"],
                        "aborted": ["wflow11", "wflow12"]}
        requests = []
        for status in statusList:
            # self.logger.info("MockReqMgr: Setting currentStatus to: %s", status)
            self.currentStatus = status
            if status in requestsDict:
                requests.extend(requestsDict[status])
        return requests

    def updateRequestStatus(self, requestName, statusName):
        """ Mock the 'updateRequestStatus' method"""
        currentStatus = self.cacheWflowStatus.get(requestName, self.currentStatus)
        logging.info("MockReqMgr: updating workflow %s to status: %s",
                     requestName, statusName)
        if check_allowed_transition(currentStatus, statusName):
            self.cacheWflowStatus[requestName] = statusName
            return True
        else:
            raise InvalidStateTransition(requestName, currentStatus, statusName)


class StatusChangeTasksTests(unittest.TestCase):

    def setUp(self):
        """
        Setup the ReqMgr emulator and logger object
        """
        self.logger = logging.getLogger()
        self.reqmgr = MockReqMgr(self.logger)

    def tearDown(self):
        """
        Nothing to tear down
        """
        pass

    def testMoveForwardStatus(self):
        """
        Test the 'moveForwardStatus' function of the CherryPy thread
        An exception should be raised if any invalid status transition is attempted
        """
        # no workflows to perform status transition
        statusFromWQE = dict()
        moveForwardStatus(self.reqmgr, statusFromWQE, self.logger)

        # reqmgr2 and workqueue status in sync, no transition!
        self.reqmgr.cacheWflowStatus.clear()
        statusFromWQE = dict(wflow3='acquired', wflow5='running-open')
        moveForwardStatus(self.reqmgr, statusFromWQE, self.logger)

        # FIXME: an invalid case? I guess workqueue marks it as failed
        # wflow1 transition: staged --> acquired
        # wflow3 transition: acquired --> failed
        self.reqmgr.cacheWflowStatus.clear()
        statusFromWQE = dict(wflow1='failed', wflow3='failed')
        moveForwardStatus(self.reqmgr, statusFromWQE, self.logger)

        # workflow in aborted with completed GQEs, it goes to aborted-completed
        self.reqmgr.cacheWflowStatus.clear()
        statusFromWQE = dict(wflow11='completed', wflow12='completed')
        moveForwardStatus(self.reqmgr, statusFromWQE, self.logger)

        # transition all the way from staged -> acquired -> running-open -> running-closed -> completed
        # and acquired -> running-open -> running-closed
        self.reqmgr.cacheWflowStatus.clear()
        statusFromWQE = dict(wflow1='completed', wflow2='completed',
                             wflow3='running-closed', wflow4='running-closed')
        moveForwardStatus(self.reqmgr, statusFromWQE, self.logger)

        # transition from acquired -> running-open
        self.reqmgr.cacheWflowStatus.clear()
        statusFromWQE = dict(wflow3='running-open', wflow4='running-open')
        moveForwardStatus(self.reqmgr, statusFromWQE, self.logger)


if __name__ == '__main__':
    unittest.main()
