#!/usr/bin/env python
"""
Helper class for RequestManager interaction
"""
import os
import socket
import threading
import time
import traceback
from operator import itemgetter

from WMCore import Lexicon
from WMCore.Database.CMSCouch import CouchError
from WMCore.Database.CouchUtils import CouchConnectionError
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_LIST
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError, TERMINAL_EXCEPTIONS


class WorkQueueReqMgrInterface(object):
    """Helper class for ReqMgr interaction"""

    def __init__(self, **kwargs):
        if not kwargs.get('logger'):
            import logging
            kwargs['logger'] = logging
        self.logger = kwargs['logger']
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
        self.previous_state = {}

    def __call__(self, queue):
        """Synchronize WorkQueue and RequestManager"""
        msg = ''
        try:  # pull in new work
            self.logger.info("queueing new work")
            work = self.queueNewRequests(queue)
            msg += "New Work: %d\n" % work
        except Exception as ex:
            errorMsg = "Error caught during RequestManager pull"
            self.logger.exception("%s: %s", errorMsg, str(ex))

        try:  # get additional open-running work
            self.logger.info("adding new element to open requests")
            extraWork = self.addNewElementsToOpenRequests(queue)
            msg += "Work added: %d\n" % extraWork
        except Exception as ex:
            errorMsg = "Error caught during RequestManager split"
            self.logger.exception("%s: %s", errorMsg, str(ex))

        try:  # report back to ReqMgr
            self.logger.info("cancel aborted requests")
            count = self.cancelWork(queue)
            self.logger.info("finised canceling requests")
            msg += "Work canceled: %s " % count
        except Exception as ex:
            errorMsg = "Error caught during canceling the request"
            self.logger.exception("%s: %s", errorMsg, str(ex))

        queue.backend.recordTaskActivity('reqmgr_sync', msg)

    def queueNewRequests(self, queue):
        """Get requests from regMgr and queue to workqueue"""
        self.logger.info("Contacting Request manager for more work")
        work = 0
        workLoads = []

        try:
            workLoads = self.getAvailableRequests()
        except Exception as ex:
            traceMsg = traceback.format_exc()
            msg = "Error contacting RequestManager: %s" % traceMsg
            self.logger.warning(msg)
            return 0

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
                self.logger.error('Permanent failure processing request "%s": %s' % (reqName, str(ex)))
                self.logger.info("Marking request %s as failed in ReqMgr" % reqName)
                self.reportRequestStatus(reqName, 'Failed', message=str(ex))
                continue
            except (IOError, socket.error, CouchError, CouchConnectionError) as ex:
                # temporary problem - try again later
                msg = 'Error processing request "%s": will try again later.' % reqName
                msg += '\nError: "%s"' % str(ex)
                self.logger.info(msg)
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

            self.logger.info("%s element(s) obtained from RequestManager" % work)
        return work

    def cancelWork(self, queue):
        requests = self.reqMgr2.getRequestByStatus(['aborted', 'force-complete'], detail=False)
        count = 0
        for req in requests:
            try:
                queue.cancelWork(req)
                count += 1
            except Exception as ex:
                msg = 'Error to cancel the request "%s": %s' % (req, str(ex))
                self.logger.exception(msg)
        return count

    def report(self, queue):
        """Report queue status to ReqMgr."""
        new_state = {}
        uptodate_elements = []
        now = time.time()

        elements = queue.statusInbox(dictKey="RequestName")
        if not elements:
            return new_state

        for ele in elements:
            ele = elements[ele][0]  # 1 element tuple
            try:
                request = self.reqMgr2.getRequestByNames(ele['RequestName'])
                if not request:
                    msg = 'Failed to get request "%s" from ReqMgr2. Will try again later.' % ele['RequestName']
                    self.logger.warning(msg)
                    continue
                request = request[0][ele['RequestName']]
                if request['RequestStatus'] in ('failed', 'completed', 'announced',
                                                'closed-out', 'rejected'):
                    # requests can be done in reqmgr but running in workqueue
                    # if request has been closed but agent cleanup actions
                    # haven't been run (or agent has been retired)
                    # Prune out obviously too old ones to avoid build up
                    if queue.params.get('reqmgrCompleteGraceTime', -1) > 0:
                        if (now - float(ele.updatetime)) > queue.params['reqmgrCompleteGraceTime']:
                            # have to check all elements are at least running and are old enough
                            request_elements = queue.statusInbox(WorkflowName=request['RequestName'])
                            if not any(
                                    [x for x in request_elements if x['Status'] != 'Running' and not x.inEndState()]):
                                last_update = max([float(x.updatetime) for x in request_elements])
                                if (now - last_update) > queue.params['reqmgrCompleteGraceTime']:
                                    self.logger.info(
                                        "Finishing request %s as it is done in reqmgr" % request['RequestName'])
                                    queue.doneWork(WorkflowName=request['RequestName'])
                                    continue
                    else:
                        pass  # assume workqueue status will catch up later
                elif request['RequestStatus'] in ['aborted', 'force-complete']:
                    queue.cancelWork(WorkflowName=request['RequestName'])
                # Check consistency of running-open/closed and the element closure status
                elif request['RequestStatus'] == 'running-open' and not ele.get('OpenForNewData', False):
                    self.reportRequestStatus(ele['RequestName'], 'running-closed')
                elif request['RequestStatus'] == 'running-closed' and ele.get('OpenForNewData', False):
                    queue.closeWork(ele['RequestName'])
                # we do not want to move the request to 'failed' status
                elif ele['Status'] == 'Failed':
                    continue
                elif ele['Status'] not in self._reqMgrToWorkQueueStatus(request['RequestStatus']):
                    self.reportElement(ele)

                uptodate_elements.append(ele)
            except Exception as ex:
                msg = 'Error talking to ReqMgr about request "%s": %s' % (ele['RequestName'], str(ex))
                self.logger.exception(msg)

        return uptodate_elements

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
        tempResults = self.reqMgr2.getRequestByStatus("staged")
        filteredResults = []
        for requests in tempResults:
            for request in requests.values():
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
        """Add new elements to open requests which are in running-open state, only works adding new blocks from the input dataset"""
        self.logger.info("Checking Request Manager for open requests and closing old ones")

        work = 0
        requests = []

        try:
            requests = self.reqMgr2.getRequestByStatus("running-open", detail=False)
        except Exception as ex:
            traceMsg = traceback.format_exc()
            msg = "Error contacting RequestManager: %s" % traceMsg
            self.logger.warning(msg)
            return 0

        for reqName in requests:
            try:
                self.logger.info("Processing request %s" % (reqName))
                units = queue.addWork(requestName=reqName)
                self.logdb.delete(reqName, 'error', True, agent=False)
            except (WorkQueueWMSpecError, WorkQueueNoWorkError) as ex:
                # fatal error - but at least it was split the first time. Log and skip.
                msg = 'Error adding further work to request "%s". Will try again later' % reqName
                msg += '\nError: "%s"' % str(ex)
                self.logger.info(msg)
                self.logdb.post(reqName, msg, 'error')
                continue
            except (IOError, socket.error, CouchError, CouchConnectionError) as ex:
                # temporary problem - try again later
                msg = 'Error processing request "%s": will try again later.' % reqName
                msg += '\nError: "%s"' % str(ex)
                self.logger.info(msg)
                self.logdb.post(reqName, msg, 'error')
                continue
            except Exception as ex:
                # Log exception as it isnt a communication problem
                msg = 'Error processing request "%s": will try again later.' % reqName
                msg += '\nSee log for details.\nError: "%s"' % str(ex)
                traceMsg = traceback.format_exc()
                msg = "%s\n%s" % (msg, traceMsg)
                self.logger.exception('Unknown error processing %s' % reqName)
                self.logdb.post(reqName, msg, 'error')
                continue

            self.logger.info('%s units(s) queued for "%s"' % (units, reqName))
            work += units

        self.logger.info("%s element(s) added to open requests" % work)
        return work
