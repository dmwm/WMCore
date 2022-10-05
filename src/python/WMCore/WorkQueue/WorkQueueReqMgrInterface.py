#!/usr/bin/env python
"""
Helper class for RequestManager interaction
"""
from builtins import object
from future.utils import viewvalues

import logging
import os
import socket
import threading
import traceback
from operator import itemgetter

from WMCore import Lexicon
from WMCore.Database.CMSCouch import CouchError, CouchNotFoundError
from WMCore.Database.CouchUtils import CouchConnectionError
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_LIST
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError, TERMINAL_EXCEPTIONS


class WorkQueueReqMgrInterface(object):
    """Helper class for ReqMgr interaction"""

    def __init__(self, **kwargs):
        if not kwargs.get('logger'):
            kwargs['logger'] = logging
        self.logger = kwargs['logger']
        self.rucio = Rucio(kwargs.get("rucioAccount", "wmcore_transferor"),
                           configDict=dict(logger=self.logger))
        # this will break all in one test
        self.reqMgr2 = ReqMgr(kwargs.get("reqmgr2_endpoint", None))

        centralurl = kwargs.get("central_logdb_url", "")
        identifier = kwargs.get("log_reporter", "")

        # set the thread name before creat the log db.
        # only sets that when it is not set already
        myThread = threading.currentThread()
        if myThread.getName() == "MainThread":
            myThread.setName(self.__class__.__name__)

        self.logdb = LogDB(centralurl, identifier, logger=self.logger)

    def __call__(self, queue):
        """Synchronize WorkQueue and RequestManager"""
        # ensure log records go to the correct logger object
        queue.logger = self.logger
        msg = ''
        try:  # Fetch data from ReqMgr and propagate work cancellation to workqueue
            self.logger.info("Canceling aborted and force-completed requests")
            count = self.cancelWork(queue)
            msg += "Work canceled: %s, " % count
        except Exception as ex:
            self.logger.exception("Generic error while canceling work. Details: %s", str(ex))

        try:  # Close requests that no longer need to be open for new data
            self.logger.info("Closing open requests")
            workClosed = queue.closeWork()
            msg += "Work closed: %d, " % len(workClosed)
        except Exception as ex:
            errorMsg = "Generic error while closing open requests. Details: %s"
            self.logger.exception(errorMsg, str(ex))

        try:  # Try to create new work elements for open requests
            self.logger.info("Adding new elements to open requests")
            extraWork = self.addNewElementsToOpenRequests(queue)
            msg += "Work added: %d, " % extraWork
        except Exception as ex:
            errorMsg = "Generic error while adding work to open requests. Details: %s"
            self.logger.exception(errorMsg, str(ex))

        try:  # Pull in work for new requests
            self.logger.info("Queuing work for new requests")
            work = self.queueNewRequests(queue)
            msg += "New Work: %d" % work
        except Exception as ex:
            errorMsg = "Generic error while queuing work for new requests. Details: %s"
            self.logger.exception(errorMsg, str(ex))

        self.logger.info("Summary of %s: %s", self.__class__.__name__, msg)
        queue.backend.recordTaskActivity('reqmgr_sync', msg)

    def queueNewRequests(self, queue):
        """Get requests from regMgr and queue to workqueue"""
        try:
            workLoads = self.getAvailableRequests()
        except Exception as exc:
            msg = "Error contacting RequestManager. Details: %s" % str(exc)
            self.logger.exception(msg)
            return 0

        work = 0
        for team, reqName, workLoadUrl in workLoads:
            try:
                try:
                    Lexicon.couchurl(workLoadUrl)
                except Exception as ex:  # can throw many errors e.g. AttributeError, AssertionError etc.
                    # check its not a local file
                    if not os.path.exists(workLoadUrl):
                        error = WorkQueueWMSpecError(None, "Workflow url validation error: %s" % str(ex))
                        raise error

                self.logger.info("Processing request %s at %s" % (reqName, workLoadUrl))
                units = queue.queueWork(workLoadUrl, request=reqName, team=team)
                self.logdb.delete(reqName, "error", this_thread=True, agent=False)
            except TERMINAL_EXCEPTIONS as ex:
                # fatal error - report back to ReqMgr
                self.logger.critical('Permanent failure processing request "%s": %s' % (reqName, str(ex)))
                self.logger.info("Marking request %s as failed in ReqMgr" % reqName)
                self.reportRequestStatus(reqName, 'Failed', message=str(ex))
                continue
            except (IOError, socket.error, CouchError, CouchConnectionError) as ex:
                # temporary problem - try again later
                msg = 'Error processing request "%s": will try again later.' % reqName
                msg += '\nError: "%s"' % str(ex)
                self.logger.error(msg)
                self.logdb.post(reqName, msg, 'error')
                continue
            except Exception as ex:
                # Log exception as it isnt a communication problem
                msg = 'Error processing request "%s": will try again later.' % reqName
                msg += '\nSee log for details.\nError: "%s"' % str(ex)
                self.logger.exception('Unknown error processing %s' % reqName)
                self.logdb.post(reqName, msg, 'error')
                continue

            self.logger.info('%s units(s) queued for "%s"' % (units, reqName))
            work += units

        self.logger.info("Total of %s element(s) queued for new requests", work)
        return work

    def cancelWork(self, queue):
        requests = self.reqMgr2.getRequestByStatus(['aborted', 'force-complete'], detail=False)
        count = 0
        for req in requests:
            try:
                queue.cancelWork(req)
                count += 1
            except CouchNotFoundError as exc:
                msg = 'Failed to cancel workflow: {} because elements are no '.format(req)
                msg += 'longer exist in CouchDB. Details: {}'.format(str(exc))
            except Exception as ex:
                msg = 'Error to cancel the request "%s": %s' % (req, str(ex))
                self.logger.exception(msg)
        return count

    def deleteFinishedWork(self, queue, elements):
        """Delete work from queue that is finished in ReqMgr"""
        finished = []
        for element in elements:
            if element.inEndState():
                finished.append(element['RequestName'])
        return queue.deleteWorkflows(*finished)

    def getAvailableRequests(self):
        """
        Get available requests and sort by team and priority
        returns [(team, request_name, request_spec_url)]
        """
        thisStatus = "staged"
        self.logger.info("Contacting ReqMgr for workflows in status: %s", thisStatus)
        tempResults = self.reqMgr2.getRequestByStatus(thisStatus)
        filteredResults = []
        for requests in tempResults:
            for request in viewvalues(requests):
                filteredResults.append(request)
        filteredResults.sort(key=itemgetter('RequestPriority'), reverse=True)
        filteredResults.sort(key=lambda r: r["Team"])

        results = [(x["Team"], x["RequestName"], x["RequestWorkflow"]) for x in filteredResults]

        return results

    def reportRequestStatus(self, request, status, message=None):
        """Change state in RequestManager
           Optionally, take a message to append to the request
        """
        if message:
            logType = "error" if status == "Failed" else "info"
            self.logdb.post(request, str(message), logType)
        reqmgrStatus = self._workQueueToReqMgrStatus(status)

        if reqmgrStatus:  # only send known states
            try:
                self.reqMgr2.updateRequestStatus(request, reqmgrStatus)
            except Exception as ex:
                msg = "%s : fail to update status will try later: %s" % (request, str(ex))
                msg += traceback.format_exc()
                self.logdb.post(request, msg, 'warning')
                raise ex
        return

    def _workQueueToReqMgrStatus(self, status):
        """Map WorkQueue Status to that reported to ReqMgr"""
        statusMapping = {'Acquired': 'acquired',
                         'Running': 'running-open',
                         'Failed': 'failed',
                         'Canceled': 'aborted',
                         'CancelRequested': 'aborted',
                         'Done': 'completed'
                         }
        if status in statusMapping:
            # if wq status passed convert to reqmgr status
            return statusMapping[status]
        elif status in REQUEST_STATE_LIST:
            # if reqmgr status passed return reqmgr status
            return status
        else:
            # unknown status
            return None

    def _reqMgrToWorkQueueStatus(self, status):
        """Map ReqMgr status to that in a WorkQueue element, it is not a 1-1 relation"""
        statusMapping = {'acquired': ['Acquired'],
                         'running': ['Running'],
                         'running-open': ['Running'],
                         'running-closed': ['Running'],
                         'failed': ['Failed'],
                         'aborted': ['Canceled', 'CancelRequested'],
                         'force-complete': ['Canceled', 'CancelRequested'],
                         'completed': ['Done']}
        if status in statusMapping:
            return statusMapping[status]
        else:
            return []

    def reportElement(self, element):
        """Report element to ReqMgr"""
        self.reportRequestStatus(element['RequestName'], element['Status'])

    def addNewElementsToOpenRequests(self, queue):
        """
        Add new elements to open requests according to their
        workqueue_inbox element.
        """
        self.logger.info("Fetching open requests from WorkQueue")

        try:
            workInbox = queue.backend.getInboxElements(OpenForNewData=True, loadSpec=True)
        except Exception as exc:
            self.logger.exception("Error retrieving open inbox elements. Details: %s", str(exc))
            return 0

        self.logger.info("Retrieved %d inbox elements open for new data", len(workInbox))
        work = 0
        for elem in workInbox:
            try:
                units = queue.addWork(elem, rucioObj=self.rucio)
                self.logdb.delete(elem['RequestName'], 'error', True, agent=False)
            except (WorkQueueWMSpecError, WorkQueueNoWorkError) as ex:
                # fatal error - but at least it was split the first time. Log and skip.
                msg = 'Error adding further work to request "%s". ' % elem['RequestName']
                msg += 'Will try again later.\nError: "%s"' % str(ex)
                self.logger.error(msg)
                self.logdb.post(elem['RequestName'], msg, 'error')
            except (IOError, socket.error, CouchError, CouchConnectionError) as ex:
                # temporary problem - try again later
                msg = 'Error processing request "%s": will try again later.' % elem['RequestName']
                msg += '\nError: "%s"' % str(ex)
                self.logger.error(msg)
                self.logdb.post(elem['RequestName'], msg, 'error')
            except Exception as ex:
                # Log exception as it isnt a communication problem
                msg = 'Error processing request "%s". Will try again later. ' % elem['RequestName']
                msg += 'See log for details.\nError: "%s"' % str(ex)
                msg += '\nTraceback: %s' % traceback.format_exc()
                self.logger.exception('Unknown error processing %s' % elem['RequestName'])
                self.logdb.post(elem['RequestName'], msg, 'error')
            else:
                work += units

        self.logger.info("%s element(s) added to open requests" % work)
        return work
