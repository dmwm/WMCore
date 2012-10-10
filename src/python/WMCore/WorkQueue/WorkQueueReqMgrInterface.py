#!/usr/bin/env python
"""Helper class for RequestManager interaction
"""

from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from WMCore.Database.CMSCouch import CouchError
from WMCore.Database.CouchUtils import CouchConnectionError
from WMCore import Lexicon
import os
import time
import socket

class WorkQueueReqMgrInterface():
    """Helper class for ReqMgr interaction"""
    def __init__(self, **kwargs):
        if not kwargs.get('logger'):
            import logging
            kwargs['logger'] = logging
        self.logger = kwargs['logger']
        self.reqMgr = RequestManager(kwargs)
        self.previous_state = {}

    def __call__(self, queue):
        """Synchronize WorkQueue and RequestManager"""
        msg = ''
        try:    # pull in new work
            work = self.queueNewRequests(queue)
            msg += "New Work: %d\n" % work
        except Exception:
            self.logger.exception("Error caught during RequestManager pull")
        try:    # report back to ReqMgr
            uptodate_elements = self.report(queue)
            msg += "Updated ReqMgr status for: %s" % ", ".join([x['RequestName'] for x in uptodate_elements])
        except:
            self.logger.exception("Error caught during RequestManager update")
        else:
            try:    # Delete finished requests from WorkQueue
                self.deleteFinishedWork(queue, uptodate_elements)
            except:
                self.logger.exception("Error caught during work deletion")
            else:
                queue.backend.recordTaskActivity('reqmgr_sync', msg)

    def queueNewRequests(self, queue):
        """Get requests from regMgr and queue to workqueue"""
        self.logger.info("Contacting Request manager for more work")
        work = 0
        workLoads = []

        if queue.params['DrainMode']:
            self.logger.info('Draining queue: Skip requesting work from ReqMgr')
            return 0

        try:
            workLoads = self.getAvailableRequests(*queue.params['Teams'])
        except Exception, ex:
            msg = "Error contacting RequestManager: %s" % str(ex)
            self.logger.warning(msg)
            return 0

        for team, reqName, workLoadUrl in workLoads:
#            try:
#                self.reportRequestStatus(reqName, "negotiating")
#            except Exception, ex:
#                self.logger.error("""
#                    Unable to update ReqMgr state to negotiating: %s
#                    Ignoring this request: %s""" % (str(ex), reqName))
#                continue

            try:
                try:
                    Lexicon.couchurl(workLoadUrl)
                except Exception, ex: # can throw many errors e.g. AttributeError, AssertionError etc.
                    # check its not a local file
                    if not os.path.exists(workLoadUrl):
                        error = WorkQueueWMSpecError(None, "Workflow url validation error: %s" % str(ex))
                        raise error

                self.logger.info("Processing request %s at %s" % (reqName, workLoadUrl))
                units = queue.queueWork(workLoadUrl, request = reqName, team = team)
            except (WorkQueueWMSpecError, WorkQueueNoWorkError), ex:
                # fatal error - report back to ReqMgr
                self.logger.info('Permanent failure processing request "%s": %s' % (reqName, str(ex)))
                self.logger.info("Marking request %s as failed in ReqMgr" % reqName)
                self.reportRequestStatus(reqName, 'Failed', message = str(ex))
                continue
            except (IOError, socket.error, CouchError, CouchConnectionError), ex:
                # temporary problem - try again later
                msg = 'Error processing request "%s": will try again later.' \
                '\nError: "%s"' % (reqName, str(ex))
                self.logger.info(msg)
                self.sendMessage(reqName, msg)
                continue
            except Exception, ex:
                # Log exception as it isnt a communication problem
                msg = 'Error processing request "%s": will try again later.' \
                '\nSee log for details.\nError: "%s"' % (reqName, str(ex))
                self.logger.exception('Unknown error processing %s' % reqName)
                self.sendMessage(reqName, msg)
                continue

            try:
                self.markAcquired(reqName, queue.params.get('QueueURL', 'No Queue'))
            except Exception, ex:
                self.logger.warning("Unable to update ReqMgr state: %s" % str(ex))
                self.logger.warning('Will try again later')

            self.logger.info('%s units(s) queued for "%s"' % (units, reqName))
            work += units

            self.logger.info("%s element(s) obtained from RequestManager" % work)
        return work


    def report(self, queue):
        """Report queue status to ReqMgr."""
        new_state = {}
        uptodate_elements = []
        now = time.time()

        elements = queue.statusInbox(dictKey = "RequestName")
        if not elements:
            return new_state

        for ele in elements:
            ele = elements[ele][0] # 1 element tuple
            try:
                request = self.reqMgr.getRequest(ele['RequestName'])
                if request['RequestStatus'] in ('failed', 'completed', 'announced',
                                                'epic-FAILED', 'closed-out', 'rejected'):
                    # requests can be done in reqmgr but running in workqueue
                    # if request has been closed but agent cleanup actions
                    # haven't been run (or agent has been retired)
                    # Prune out obviously too old ones to avoid build up
                    if queue.params.get('reqmgrCompleteGraceTime', -1) > 0:
                        if (now - float(ele.updatetime)) > queue.params['reqmgrCompleteGraceTime']:
                            # have to check all elements are at least running and are old enough
                            request_elements = queue.statusInbox(WorkflowName = request['RequestName'])
                            if not any([x for x in request_elements if x['Status'] != 'Running' and not x.inEndState()]):
                                last_update = max([float(x.updatetime) for x in request_elements])
                                if (now - last_update) > queue.params['reqmgrCompleteGraceTime']:
                                    self.logger.info("Finishing request %s as it is done in reqmgr" % request['RequestName'])
                                    queue.doneWork(WorkflowName=request['RequestName'])
                                    continue
                    else:
                        pass # assume workqueue status will catch up later
                elif request['RequestStatus'] == 'aborted':
                    queue.cancelWork(WorkflowName=request['RequestName'])
                elif not ele['Status'] == self._reqMgrStatus(request['RequestStatus']):
                    self.reportElement(ele)
                # check if we need to update progress, only update if we have progress
                elif ele['PercentComplete'] > request['percent_complete'] + 1 or \
                     ele['PercentSuccess'] > request['percent_success'] + 1:
                    self.reportProgress(ele['RequestName'], percent_complete = ele['PercentComplete'],
                                                            percent_success = ele['PercentSuccess'])
                uptodate_elements.append(ele)
            except Exception, ex:
                msg = 'Error talking to ReqMgr about request "%s": %s'
                self.logger.error(msg % (ele['RequestName'], str(ex)))

        return uptodate_elements

    def deleteFinishedWork(self, queue, elements):
        """Delete work from queue that is finished in ReqMgr"""
        finished = []
        for element in elements:
            if self._reqMgrStatus(element['Status']) in ('aborted', 'failed', 'completed', 'announced',
                                                         'epic-FAILED', 'closed-out', 'rejected') \
                                                         and element.inEndState():
                finished.append(element['RequestName'])
        return queue.deleteWorkflows(*finished)

    def getAvailableRequests(self, *teams):
        """Get requests for the given teams"""
        results = []
        if not teams:
            teams = self.reqMgr.getTeam()
        for team in teams:
            try:
                reqs = self.reqMgr.getAssignment(team)
                results.extend([(team, req, spec_url) for req, spec_url in reqs])
            except Exception, ex:
                self.logger.error('Error getting work for team "%s": %s' % (team, str(ex)))
        return results

    def reportRequestStatus(self, request, status, message = None):
        """Change state in RequestManager
           Optionally, take a message to append to the request
        """
        if message:
            self.sendMessage(request, str(message))
        if self._reqMgrStatus(status): # only send known states
            self.reqMgr.reportRequestStatus(request, self._reqMgrStatus(status))

    def sendMessage(self, request, message):
        """Attach a message to the request"""
        try:
            self.reqMgr.sendMessage(request, message)
        except Exception, ex:
            self.logger.error('Error sending message to reqmgr: %s' % str(ex))

    def markAcquired(self, request, url = None):
        """Mark request acquired"""
        self.reqMgr.putWorkQueue(request, url)

    def _reqMgrStatus(self, status):
        """Map WorkQueue Status to that reported to ReqMgr"""
        statusMapping = {'Acquired' : 'acquired',
                         'Running' : 'running',
                         'Failed' : 'failed',
                         'Canceled' : 'aborted',
                         'CancelRequested' : 'aborted',
                         'Done' : 'completed'
                         }
        if statusMapping.has_key(status):
            return statusMapping[status]
        else:
            return None

    def reportProgress(self, request, **args):
        """report progress for the request"""
        try:
            return self.reqMgr.reportRequestProgress(request, **args)
        except Exception, ex:
            # metrics are nice to have but not essential
            self.logger.warning("Error updating reqmgr metrics for %s: %s" % (request, str(ex)))

    def reportElement(self, element):
        """Report element to ReqMgr"""
        self.reportRequestStatus(element['RequestName'], element['Status'])
        if element['PercentComplete'] or element['PercentSuccess']:
            args = {'percent_complete' : element['PercentComplete'],
                    'percent_success' : element['PercentSuccess']}
            self.reportProgress(element['RequestName'], **args)
