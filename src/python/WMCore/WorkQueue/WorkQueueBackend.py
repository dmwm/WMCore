#!/usr/bin/env python
"""
WorkQueueBackend

Interface to WorkQueue persistent storage
"""

from builtins import object

from future.utils import viewitems

import json
import random
import time

from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError, Document
from WMCore.Lexicon import sanitizeURL
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement, fixElementConflicts
from WMCore.WorkQueue.DataStructs.WorkQueueElement import possibleSites
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueNoMatchingElements, WorkQueueError


def formatReply(answer, *items):
    """Take reply from couch bulk api and format labeling errors etc
    """
    result, errors = [], []
    for ans in answer:
        if 'error' in ans:
            errors.append(ans)
            continue
        for item in items:
            if item.id == ans['id']:
                item.rev = ans['rev']
                result.append(item)
                break
    return result, errors


def sortAvailableElements(elementsList):
    """
    Given a list of workqueue elements dictionary, this function will
    sort them in place, first by their creation time; secondly by their
    priority. In other words, higher priority and older requests will
    be first in the list
    :param elementsList: a list of elements dictionary
    :return: nothing, list is updated in place
    """
    elementsList.sort(key=lambda element: element['CreationTime'])
    elementsList.sort(key=lambda element: element['Priority'], reverse=True)


class WorkQueueBackend(object):
    """
    Represents persistent storage for WorkQueue
    """

    def __init__(self, db_url, db_name='workqueue',
                 inbox_name=None, parentQueue=None,
                 queueUrl=None, logger=None):
        if logger:
            self.logger = logger
        else:
            import logging
            self.logger = logging

        if inbox_name is None:
            inbox_name = "%s_inbox" % db_name

        self.server = CouchServer(db_url)
        self.parentCouchUrlWithAuth = parentQueue
        if parentQueue:
            self.parentCouchUrl = sanitizeURL(parentQueue)['url']
        else:
            self.parentCouchUrl = None
        self.db = self.server.connectDatabase(db_name, create=False, size=10000)
        self.hostWithAuth = db_url
        self.inbox = self.server.connectDatabase(inbox_name, create=False, size=10000)
        self.queueUrlWithAuth = queueUrl or (db_url + '/' + db_name)
        self.queueUrl = sanitizeURL(queueUrl or (db_url + '/' + db_name))['url']
        self.eleKey = 'WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'

    def forceQueueSync(self):
        """Setup CouchDB replications - used only in tests"""
        self.pullFromParent(continuous=True)
        self.sendToParent(continuous=True)

    def pullFromParent(self, continuous=True, cancel=False):
        """Replicate from parent couch - blocking: used only in unit tests"""
        try:
            if self.parentCouchUrlWithAuth and self.queueUrlWithAuth:
                self.logger.info("Forcing pullFromParent from parentCouch: %s to queueUrl %s/%s",
                                 self.parentCouchUrlWithAuth, self.queueUrlWithAuth, self.inbox.name)
                self.server.replicate(source=self.parentCouchUrlWithAuth,
                                      destination="%s/%s" % (self.hostWithAuth, self.inbox.name),
                                      filter='WorkQueue/queueFilter',
                                      query_params={'childUrl': self.queueUrl,
                                                    'parentUrl': self.parentCouchUrl},
                                      continuous=continuous,
                                      cancel=cancel,
                                      sleepSecs=6)
        except Exception as ex:
            self.logger.warning('Replication from %s failed: %s' % (self.parentCouchUrlWithAuth, str(ex)))

    def sendToParent(self, continuous=True, cancel=False):
        """Replicate to parent couch - blocking: used only int test"""
        try:
            if self.parentCouchUrlWithAuth and self.queueUrlWithAuth:
                self.logger.info("Forcing sendToParent from queueUrl %s/%s to parentCouch: %s",
                                 self.queueUrlWithAuth, self.inbox.name, self.parentCouchUrlWithAuth)
                self.server.replicate(source="%s" % self.inbox.name,
                                      destination=self.parentCouchUrlWithAuth,
                                      filter='WorkQueue/queueFilter',
                                      query_params={'childUrl': self.queueUrl,
                                                    'parentUrl': self.parentCouchUrl},
                                      continuous=continuous,
                                      cancel=cancel)
        except Exception as ex:
            self.logger.warning('Replication to %s failed: %s' % (self.parentCouchUrlWithAuth, str(ex)))

    def getElementsForSplitting(self):
        """Returns the elements from the inbox that need to be split,
        if WorkflowName specified only return elements to split for that workflow"""
        elements = self.getInboxElements(status='Negotiating')
        specs = {}  # cache as may have multiple elements for same spec
        for ele in elements:
            if ele['RequestName'] not in specs:
                wmspec = WMWorkloadHelper()
                wmspec.load(self.parentCouchUrlWithAuth + "/%s/spec" % ele['RequestName'])
                specs[ele['RequestName']] = wmspec
            ele['WMSpec'] = specs[ele['RequestName']]
        del specs
        return elements

    def insertWMSpec(self, wmspec):
        """
        Insert WMSpec to backend
        """
        # Can't save spec to inbox, it needs to be visible to child queues
        # Can't save empty dict so add dummy variable
        dummyValues = {'name': wmspec.name()}
        # change specUrl in spec before saving (otherwise it points to previous url)
        wmspec.setSpecUrl(self.db['host'] + "/%s/%s/spec" % (self.db.name, wmspec.name()))
        return wmspec.saveCouch(self.hostWithAuth, self.db.name, dummyValues)

    def getWMSpec(self, name):
        """Get the spec"""
        wmspec = WMWorkloadHelper()
        wmspec.load(self.hostWithAuth + "/%s/%s/spec" % (self.db.name, name))
        return wmspec

    def insertElements(self, units, parent=None):
        """
        Insert element to database

        @param parent is the parent WorkQueueObject these element's belong to.
                                            i.e. a workflow which has been split
        """
        if not units:
            return []
        # store spec file separately - assume all elements share same spec
        self.insertWMSpec(units[0]['WMSpec'])
        newUnitsInserted = []
        for unit in units:
            # cast to couch
            if not isinstance(unit, CouchWorkQueueElement):
                unit = CouchWorkQueueElement(self.db, elementParams=dict(unit))

            if parent:
                unit['ParentQueueId'] = parent.id
                unit['TeamName'] = parent['TeamName']
                unit['WMBSUrl'] = parent['WMBSUrl']

            if unit._couch.documentExists(unit.id):
                self.logger.info('Element "%s" already exists, skip insertion.' % unit.id)
                continue

            newUnitsInserted.append(unit)
            unit.save()
            # FIXME: this is not performing bulk request, but single document commits(!)
            unit._couch.commit()

        return newUnitsInserted

    def createWork(self, spec, **kwargs):
        """Return the Inbox element for this spec.

        This does not persist it to the database.
        """
        kwargs.update({'WMSpec': spec,
                       'RequestName': spec.name(),
                       'StartPolicy': spec.startPolicyParameters(),
                       'EndPolicy': spec.endPolicyParameters(),
                       'OpenForNewData': True
                       })
        unit = CouchWorkQueueElement(self.inbox, elementParams=kwargs)
        unit.id = spec.name()
        return unit

    def getElements(self, status=None, elementIDs=None, returnIdOnly=False,
                    db=None, loadSpec=False, WorkflowName=None, **elementFilters):
        """Return elements that match requirements

        status, elementIDs & filters are 'AND'ed together to filter elements.
        returnIdOnly causes the element not to be loaded and only the id returned
        db is used to specify which database to return from
        loadSpec causes the workflow for each spec to be loaded.
        WorkflowName may be used in the place of RequestName
        """
        key = []
        if not db:
            db = self.db
        if elementFilters.get('RequestName') and not WorkflowName:
            WorkflowName = elementFilters.pop('RequestName')

        if elementIDs:
            if elementFilters or status or returnIdOnly:
                msg = "Can't specify extra filters (or return id's) when using element id's with getElements()"
                raise ValueError(msg)
            elements = [CouchWorkQueueElement(db, i).load() for i in elementIDs]
        else:
            options = {'include_docs': True, 'filter': elementFilters, 'idOnly': returnIdOnly, 'reduce': False}
            # filter on workflow or status if possible
            filterName = 'elementsByWorkflow'
            if WorkflowName:
                key.append(WorkflowName)
            elif status:
                filterName = 'elementsByStatus'
                key.append(status)
            elif elementFilters.get('SubscriptionId'):
                key.append(elementFilters['SubscriptionId'])
                filterName = 'elementsBySubscription'
            # add given params to filters
            if status:
                options['filter']['Status'] = status
            if WorkflowName:
                options['filter']['RequestName'] = WorkflowName

            view = db.loadList('WorkQueue', 'filter', filterName, options, key)
            view = json.loads(view)
            if returnIdOnly:
                return view
            elements = [CouchWorkQueueElement.fromDocument(db, row) for row in view]

        if loadSpec:
            specs = {}  # cache as may have multiple elements for same spec
            for ele in elements:
                if ele['RequestName'] not in specs:
                    wmspec = self.getWMSpec(ele['RequestName'])
                    specs[ele['RequestName']] = wmspec
                ele['WMSpec'] = specs[ele['RequestName']]
            del specs
        return elements

    def getInboxElements(self, *args, **kwargs):
        """
        Return elements from Inbox, supports same semantics as getElements()
        """
        return self.getElements(*args, db=self.inbox, **kwargs)

    def getElementsForWorkflow(self, workflow):
        """Get elements for a workflow"""
        elements = self.db.loadView('WorkQueue', 'elementsByWorkflow',
                                    {'key': workflow, 'include_docs': True, 'reduce': False})
        return [CouchWorkQueueElement.fromDocument(self.db,
                                                   x['doc'])
                for x in elements.get('rows', [])]

    def getElementsForParent(self, parent):
        """Get elements with the given parent"""
        elements = self.db.loadView('WorkQueue', 'elementsByParent', {'key': parent.id, 'include_docs': True})
        return [CouchWorkQueueElement.fromDocument(self.db,
                                                   x['doc'])
                for x in elements.get('rows', [])]

    def saveElements(self, *elements):
        """Persist elements

        Returns elements successfully saved, user must verify to catch errors
        """
        result = []
        if not elements:
            return result
        for element in elements:
            element.save()
        answer = elements[0]._couch.commit()
        result, failures = formatReply(answer, *elements)
        msg = 'Couch error saving element: "%s", error "%s", reason "%s"'
        for failed in failures:
            self.logger.error(msg % (failed['id'], failed['error'], failed['reason']))
        return result

    def _raiseConflictErrorAndLog(self, conflictIDs, updatedParams, dbName="workqueue"):
        errorMsg = "Need to update this element manually from %s\n ids:%s\n, parameters:%s\n" % (
            dbName, conflictIDs, updatedParams)
        self.logger.error(errorMsg)
        raise WorkQueueError(errorMsg)

    def updateElements(self, *elementIds, **updatedParams):
        """Update given element's (identified by id) with new parameters"""
        if not elementIds:
            return
        eleParams = {}
        eleParams[self.eleKey] = updatedParams
        conflictIDs = self.db.updateBulkDocumentsWithConflictHandle(elementIds, eleParams)
        if conflictIDs:
            self._raiseConflictErrorAndLog(conflictIDs, updatedParams)
        return

    def updateInboxElements(self, *elementIds, **updatedParams):
        """Update given inbox element's (identified by id) with new parameters"""
        if not elementIds:
            return
        eleParams = {}
        eleParams[self.eleKey] = updatedParams
        conflictIDs = self.inbox.updateBulkDocumentsWithConflictHandle(elementIds, eleParams)
        if conflictIDs:
            self._raiseConflictErrorAndLog(conflictIDs, updatedParams, "workqueue_inbox")
        return

    def deleteElements(self, *elements):
        """Delete elements"""
        if not elements:
            return
        specs = {}
        for i in elements:
            i.delete()
            specs[i['RequestName']] = None
        answer = elements[0]._couch.commit()
        _, failures = formatReply(answer, *elements)
        msg = 'Couch error deleting element: "%s", error "%s", reason "%s"'
        for failed in failures:
            # only count delete as failed if document still exists
            if elements[0]._couch.documentExists(failed['id']):
                self.logger.error(msg % (failed['id'], failed['error'], failed['reason']))
        # delete specs if no longer used
        for wf in specs:
            try:
                if not self.db.loadView('WorkQueue', 'elementsByWorkflow',
                                        {'key': wf, 'limit': 1, 'reduce': False})['rows']:
                    self.db.delete_doc(wf)
            except CouchNotFoundError:
                pass

    def calculateAvailableWork(self, thresholds, siteJobCounts):
        """
        A short version of the `availableWork` method, which is used only to calculate
        the amount of work already available at the local workqueue.
        :param thresholds: a dictionary key'ed by the site name, values representing the
            maximum number of jobs allowed at that site.
        :param siteJobCounts: a dictionary-of-dictionaries key'ed by the site name; value
            is a dictionary with the number of jobs running at a given priority.
        :return: a tuple with the elements accepted and an overview of job counts per site
        """
        # NOTE: this method can be less verbose as well
        elements = []
        # If there are no sites, punt early.
        if not thresholds:
            self.logger.error("No thresholds is set: Please check")
            return elements, siteJobCounts

        self.logger.info("Calculating available work from queue %s", self.queueUrl)

        options = {}
        options['include_docs'] = True
        options['descending'] = True
        options['resources'] = thresholds
        options['num_elem'] = 9999999  # magic number!
        result = self.db.loadList('WorkQueue', 'workRestrictions', 'availableByPriority', options)
        result = json.loads(result)
        self.logger.info("Retrieved %d elements from workRestrictions list for: %s",
                         len(result), self.queueUrl)

        # Convert python dictionary into Couch WQE objects
        # And sort them by creation time and priority, such that highest priority and
        # oldest elements come first in the list
        sortedElements = []
        for item in result:
            element = CouchWorkQueueElement.fromDocument(self.db, item)
            sortedElements.append(element)
        sortAvailableElements(sortedElements)

        for element in sortedElements:
            commonSites = possibleSites(element)
            prio = element['Priority']
            # shuffle list of common sites all the time to give everyone the same chance
            random.shuffle(commonSites)
            possibleSite = None
            for site in commonSites:
                if site in thresholds:
                    # Count the number of jobs currently running of greater priority, if they
                    # are less than the site thresholds, then accept this element
                    curJobCount = sum([x[1] if x[0] >= prio else 0 for x in viewitems(siteJobCounts.get(site, {}))])
                    self.logger.debug("Job Count: %s, site: %s thresholds: %s", curJobCount, site, thresholds[site])
                    if curJobCount < thresholds[site]:
                        possibleSite = site
                        break

            if possibleSite:
                self.logger.debug("Meant to accept workflow: %s, with prio: %s, element id: %s, for site: %s",
                                  element['RequestName'], prio, element.id, possibleSite)
                elements.append(element)
                siteJobCounts.setdefault(possibleSite, {})
                siteJobCounts[possibleSite][prio] = siteJobCounts[possibleSite].setdefault(prio, 0) + \
                                                    element['Jobs'] * element.get('blowupFactor', 1.0)
            else:
                self.logger.debug("No available resources for %s with localdoc id %s",
                                  element['RequestName'], element.id)

        self.logger.info("And %d elements passed location and siteJobCounts restrictions for: %s",
                         len(elements), self.queueUrl)
        return elements, siteJobCounts

    def availableWork(self, thresholds, siteJobCounts, team=None,
                      excludeWorkflows=None, numElems=9999999):
        """
        Get work - either from local or global queue - which is available to be run.

        :param thresholds: a dictionary key'ed by the site name, values representing the
            maximum number of jobs allowed at that site.
        :param siteJobCounts: a dictionary-of-dictionaries key'ed by the site name; value
            is a dictionary with the number of jobs running at a given priority.
        :param team: a string with the team name we want to pull work for
        :param excludeWorkflows: list of (aborted) workflows that should not be accepted
        :param numElems: integer with the maximum number of elements to be accepted (default
            to a very large number when pulling work from local queue, read unlimited)
        :return: a tuple with the elements accepted and an overview of job counts per site
        """
        excludeWorkflows = excludeWorkflows or []
        elements = []
        # If there are no sites, punt early.
        if not thresholds:
            self.logger.error("No thresholds is set: Please check")
            return elements, siteJobCounts

        self.logger.info("Current siteJobCounts:")
        for site, jobsByPrio in viewitems(siteJobCounts):
            self.logger.info("    %s : %s", site, jobsByPrio)

        self.logger.info("Getting up to %d available work from %s", numElems, self.queueUrl)
        self.logger.info("  for team name: %s", team)
        self.logger.info("  with excludeWorkflows: %s", excludeWorkflows)
        self.logger.info("  for thresholds: %s", thresholds)

        # FIXME: magic numbers
        docsSliceSize = 1000
        options = {}
        options['include_docs'] = True
        options['descending'] = True
        options['resources'] = thresholds
        options['limit'] = docsSliceSize
        # FIXME: num_elem option can likely be deprecated, but it needs synchronization
        # between agents and global workqueue... for now, make sure it can return the slice size
        options['num_elem'] = docsSliceSize
        if team:
            options['team'] = team

        # Fetch workqueue elements in slices, using the CouchDB "limit" and "skip"
        # options for couch views. Conditions to stop this loop are:
        #  a) have a hard stop at 50k+1 (we might have to make this configurable)
        #  b) stop as soon as an empty slice is returned by Couch (thus all docs have
        #     already been retrieve)
        #  c) or, once "numElems" elements have been accepted
        numSkip = 0
        breakOut = False
        while True:
            if breakOut:
                # then we have reached the maximum number of elements to be accepted
                break
            self.logger.info("  with limit docs: %s, and skip first %s docs", docsSliceSize, numSkip)
            options['skip'] = numSkip

            result = self.db.loadList('WorkQueue', 'workRestrictions', 'availableByPriority', options)
            result = json.loads(result)
            if result:
                self.logger.info("Retrieved %d elements from workRestrictions list for: %s",
                                 len(result), self.queueUrl)
            else:
                self.logger.info("All the workqueue elements have been exhausted for: %s ", self.queueUrl)
                break
            # update number of documents to skip in the next cycle
            numSkip += docsSliceSize

            # Convert python dictionary into Couch WQE objects, skipping aborted workflows
            # And sort them by creation time and priority, such that highest priority and
            # oldest elements come first in the list
            sortedElements = []
            for i in result:
                element = CouchWorkQueueElement.fromDocument(self.db, i)
                # make sure not to acquire work for aborted or force-completed workflows
                if element['RequestName'] in excludeWorkflows:
                    msg = "Skipping aborted/force-completed workflow: %s, work id: %s"
                    self.logger.info(msg, element['RequestName'], element._id)
                else:
                    sortedElements.append(element)
            sortAvailableElements(sortedElements)

            for element in sortedElements:
                if numElems <= 0:
                    msg = "Reached maximum number of elements to be accepted, "
                    msg += "configured to: {}, from queue: {}".format(len(elements), self.queueUrl)
                    self.logger.info(msg)
                    breakOut = True  # get out of the outer loop as well
                    break
                commonSites = possibleSites(element)
                prio = element['Priority']
                # shuffle list of common sites all the time to give everyone the same chance
                random.shuffle(commonSites)
                possibleSite = None
                for site in commonSites:
                    if site in thresholds:
                        # Count the number of jobs currently running of greater priority, if they
                        # are less than the site thresholds, then accept this element
                        curJobCount = sum([x[1] if x[0] >= prio else 0 for x in viewitems(siteJobCounts.get(site, {}))])
                        self.logger.debug(
                            "Job Count: %s, site: %s thresholds: %s" % (curJobCount, site, thresholds[site]))
                        if curJobCount < thresholds[site]:
                            possibleSite = site
                            break

                if possibleSite:
                    self.logger.info("Accepting workflow: %s, with prio: %s, element id: %s, for site: %s",
                                     element['RequestName'], prio, element.id, possibleSite)
                    numElems -= 1
                    elements.append(element)
                    siteJobCounts.setdefault(possibleSite, {})
                    siteJobCounts[possibleSite][prio] = siteJobCounts[possibleSite].setdefault(prio, 0) + \
                                                        element['Jobs'] * element.get('blowupFactor', 1.0)
                else:
                    self.logger.debug("No available resources for %s with doc id %s",
                                      element['RequestName'], element.id)

        self.logger.info("And %d elements passed location and siteJobCounts restrictions for: %s",
                         len(elements), self.queueUrl)
        return elements, siteJobCounts

    def getActiveData(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'activeData', {'reduce': True, 'group': True})
        return [{'dbs_url': x['key'][0],
                 'name': x['key'][1]} for x in data.get('rows', [])]

    def getActiveParentData(self):
        """Get data items we have work in the queue for with parent"""
        data = self.db.loadView('WorkQueue', 'activeParentData', {'reduce': True, 'group': True})
        return [{'dbs_url': x['key'][0],
                 'name': x['key'][1]} for x in data.get('rows', [])]

    def getActivePileupData(self):
        """Get data items we have work in the queue for with pileup"""
        data = self.db.loadView('WorkQueue', 'activePileupData', {'reduce': True, 'group': True})
        return [{'dbs_url': x['key'][0],
                 'name': x['key'][1]} for x in data.get('rows', [])]

    def getElementsForData(self, data):
        """Get active elements for this dbs & data combo"""
        elements = self.db.loadView('WorkQueue', 'elementsByData', {'key': data, 'include_docs': True})
        return [CouchWorkQueueElement.fromDocument(self.db,
                                                   x['doc'])
                for x in elements.get('rows', [])]

    def getElementsForParentData(self, data):
        """Get active elements for this data """
        elements = self.db.loadView('WorkQueue', 'elementsByParentData', {'key': data, 'include_docs': True})
        return [CouchWorkQueueElement.fromDocument(self.db,
                                                   x['doc'])
                for x in elements.get('rows', [])]

    def getElementsForPileupData(self, data):
        """Get active elements for this data """
        elements = self.db.loadView('WorkQueue', 'elementsByPileupData', {'key': data, 'include_docs': True})
        return [CouchWorkQueueElement.fromDocument(self.db,
                                                   x['doc'])
                for x in elements.get('rows', [])]

    def isAvailable(self):
        """Is the server available, i.e. up and not compacting"""
        try:
            compacting = self.db.info()['compact_running']
            if compacting:
                self.logger.info("CouchDB compacting - try again later.")
                return False
        except Exception as ex:
            self.logger.error("CouchDB unavailable: %s" % str(ex))
            return False
        return True

    def getWorkflows(self, includeInbox=False, includeSpecs=False):
        """Returns workflows known to workqueue"""
        result = set([x['key'] for x in self.db.loadView('WorkQueue', 'elementsByWorkflow', {'group': True})['rows']])
        if includeInbox:
            result = result | set(
                    [x['key'] for x in self.inbox.loadView('WorkQueue', 'elementsByWorkflow', {'group': True})['rows']])
        if includeSpecs:
            result = result | set([x['key'] for x in self.db.loadView('WorkQueue', 'specsByWorkflow')['rows']])
        return list(result)

    def queueLength(self):
        """Return number of available elements"""
        return self.db.loadView('WorkQueue', 'availableByPriority', {'limit': 0})['total_rows']

    def fixConflicts(self):
        """Fix elements in conflict

        Each local queue runs this to resolve its conflicts with global,
        resolution propagates up to global.

        Conflicting elements are merged into one element with others deleted.

        This will fail if elements are modified during the resolution -
        if this happens rerun.
        """
        for db in [self.inbox, self.db]:
            for row in db.loadView('WorkQueue', 'conflicts')['rows']:
                elementId = row['id']
                try:
                    conflicting_elements = [CouchWorkQueueElement.fromDocument(db, db.document(elementId, rev)) \
                                            for rev in row['value']]
                    fixed_elements = fixElementConflicts(*conflicting_elements)
                    if self.saveElements(fixed_elements[0]):
                        self.saveElements(*fixed_elements[1:])  # delete others (if merged value update accepted)
                except Exception as ex:
                    self.logger.error("Error resolving conflict for %s: %s" % (elementId, str(ex)))

    def recordTaskActivity(self, taskname, comment=''):
        """Record a task for monitoring"""
        try:
            record = self.db.document('task_activity')
        except CouchNotFoundError:
            record = Document('task_activity')
        record.setdefault('tasks', {})
        record['tasks'].setdefault(taskname, {})
        record['tasks'][taskname]['timestamp'] = time.time()
        record['tasks'][taskname]['comment'] = comment
        try:
            self.db.commitOne(record)
        except Exception as ex:
            self.logger.error("Unable to update task %s freshness: %s" % (taskname, str(ex)))

    def getWMBSInjectStatus(self, request=None):
        """
        This service only provided by global queue except on draining agent
        """
        options = {'group': True, 'reduce': True}
        if request:
            options.update(key=request)
        data = self.db.loadView('WorkQueue', 'wmbsInjectStatusByRequest', options)
        if request:
            if data['rows']:
                injectionStatus = data['rows'][0]['value']
                inboxElement = self.getInboxElements(WorkflowName=request)
                requestOpen = inboxElement[0].get('OpenForNewData', False) if inboxElement else False
                return injectionStatus and not requestOpen
            else:
                raise WorkQueueNoMatchingElements("%s not found" % request)
        else:
            injectionStatus = dict((x['key'], x['value']) for x in data.get('rows', []))
            finalInjectionStatus = []
            for request in injectionStatus:
                inboxElement = self.getInboxElements(WorkflowName=request)
                requestOpen = inboxElement[0].get('OpenForNewData', False) if inboxElement else False
                finalInjectionStatus.append({request: injectionStatus[request] and not requestOpen})

            return finalInjectionStatus

    def getWorkflowNames(self, inboxFlag=False):
        """Get workflow names from workqueue db"""
        if inboxFlag:
            db = self.inbox
        else:
            db = self.db
        data = db.loadView('WorkQueue', 'elementsByWorkflow',
                           {'stale': "update_after", 'reduce': True, 'group': True})
        return [x['key'] for x in data.get('rows', [])]

    def deleteWQElementsByWorkflow(self, workflowNames):
        """
        delete workqueue elements belongs to given workflow names
        it doen't check the status of workflow so need to be careful to use this.
        Pass only workflows which has the end status
        """
        deleted = 0
        dbs = [self.db, self.inbox]
        if not isinstance(workflowNames, list):
            workflowNames = [workflowNames]

        if len(workflowNames) == 0:
            return deleted
        options = {}
        options["stale"] = "update_after"
        options["reduce"] = False

        idsByWflow = {}
        for couchdb in dbs:
            result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options, workflowNames)
            for entry in result["rows"]:
                idsByWflow.setdefault(entry['key'], [])
                idsByWflow[entry['key']].append(entry['id'])
            for wflow, docIds in viewitems(idsByWflow):
                self.logger.info("Going to delete %d documents in *%s* db for workflow: %s. Doc IDs: %s",
                                 len(docIds), couchdb.name, wflow, docIds)
                try:
                    couchdb.bulkDeleteByIDs(docIds)
                except CouchNotFoundError as exc:
                    self.logger.error("Failed to find one of the documents. Error: %s", str(exc))
                deleted += len(docIds)
        # delete the workflow with spec from workqueue db
        for wf in workflowNames:
            self.db.delete_doc(wf)
        return deleted
