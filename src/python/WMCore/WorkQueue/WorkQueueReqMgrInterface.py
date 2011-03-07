#!/usr/bin/env python
"""Helper class for RequestManager interaction
"""

from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError

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
        try:    # pull in new work
            self.queueNewRequests(queue)
        except Exception:
            self.logger.exception("Error caught during RequestManager pull")
        try:    # report back to ReqMgr
            uptodate_elements = self.report(queue)
        except:
            self.logger.exception("Error caught during RequestManager update")
        else:
            try:    # Delete finished requests from WorkQueue
                self.deleteFinishedWork(queue, uptodate_elements)
            except:
                self.logger.exception("Error caught during work deletion")

    def queueNewRequests(self, queue):
        """Get requests from regMgr and queue to workqueue"""
        self.logger.info("Contacting Request manager for more work")
        work = 0
        workLoads = []

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

            self.logger.info("Processing request %s at %s" % (reqName, workLoadUrl))

            try:
                units = queue.queueWork(workLoadUrl, request = reqName, team = team)
            except (WorkQueueWMSpecError, WorkQueueNoWorkError), ex:
                # fatal error - report back to ReqMgr
                self.logger.error('Permanent failure processing request "%s": %s' % (reqName, str(ex)))
                self.logger.error("Marking request %s as failed in ReqMgr" % reqName)
                self.reportRequestStatus(reqName, 'Failed', message = str(ex))
                continue
            except Exception, ex:
                msg = 'Error processing request "%s": will try again later.' \
                '\nError: "%s"' % (reqName, str(ex))
                self.logger.error(msg)
                #self.reportRequestStatus(reqName, 'failed', message = str(ex))
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

        elements = queue.statusInbox(dictKey = "RequestName")
        if not elements:
            return new_state

        for ele in elements:
            ele = elements[ele][0] # 1 element tuple
            # check if element has progressed since last report
            if getattr(ele, 'updatetime', getattr(ele, 'timestamp', 0)) <= self.previous_state.get(ele['RequestName'], 0):
                # no updates for this element
                new_state[ele['RequestName']] = self.previous_state.get(ele['RequestName'], 0)
                continue

            try:
                self.reportElement(ele)
            except Exception, ex:
                msg = 'Error updating ReqMgr about request "%s": %s'
                self.logger.error(msg % (ele['RequestName'], str(ex)))
            else:
                new_state[ele['RequestName']] = ele.updatetime
        self.previous_state = new_state
        return elements

    def deleteFinishedWork(self, queue, elements):
        """Delete work from queue that is finished in ReqMgr"""
        finished = []
        for request in elements:
            element = elements[request][0] # 1 element tuple
            if self._reqMgrStatus(element['Status']) in ['failed', 'aborted', 'completed']:
                finished.append(element['RequestName'])
        return queue.deleteWorkflows(*finished)

    def getAvailableRequests(self, *teams):
        """Get requests for the given teams"""
        results = []
        for team in teams:
            reqs = self.reqMgr.getAssignment(team)
            results.extend([(team, req, spec_url) for req, spec_url in reqs.items()])
        return results
    
    def reportRequestStatus(self, request, status, message = None):
        """Change state in RequestManager
           Optionally, take a message to append to the request"""
        self.reqMgr.reportRequestStatus(request, self._reqMgrStatus(status))
        if message:
            self.sendMessage(request, str(message))

    def sendMessage(self, request, message):
        """Attach a message to the request"""
        return self.reqMgr.sendMessage(request, message)

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
            return status

    def reportProgress(self, request, **args):
        """report progress for the request"""
        return self.reqMgr.reportRequestProgress(request, **args)

    def reportElement(self, element):
        """Report element to ReqMgr"""
        self.reportRequestStatus(element['RequestName'], element['Status'])
        if element['PercentComplete'] or element['PercentSuccess']:
            args = {'percent_complete' : element['PercentComplete'],
                    'percent_success' : element['PercentSuccess']}
            self.reportProgress(element['RequestName'], **args)
