#!/usr/bin/env python
"""
WorkQueueBackend

Interface to WorkQueue persistent storage
"""

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
        self.queueUrl = sanitizeURL(queueUrl or (db_url + '/' + db_name))['url']
        self.eleKey = 'WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'

    def forceQueueSync(self):
        """Force a blocking replication - used only in tests"""
        self.pullFromParent(continuous=False)
        self.sendToParent(continuous=False)

    def pullFromParent(self, continuous=True, cancel=False):
        """Replicate from parent couch - blocking: used only int test"""
        try:
            if self.parentCouchUrl and self.queueUrl:
                self.server.replicate(source=self.parentCouchUrl,
                                      destination="%s/%s" % (self.hostWithAuth, self.inbox.name),
                                      filter='WorkQueue/queueFilter',
                                      query_params={'childUrl': self.queueUrl, 'parentUrl': self.parentCouchUrl},
                                      continuous=continuous,
                                      cancel=cancel)
        except Exception as ex:
            self.logger.warning('Replication from %s failed: %s' % (self.parentCouchUrl, str(ex)))

    def sendToParent(self, continuous=True, cancel=False):
        """Replicate to parent couch - blocking: used only int test"""
        try:
            if self.parentCouchUrl and self.queueUrl:
                self.server.replicate(source="%s" % self.inbox.name,
                                      destination=self.parentCouchUrlWithAuth,
                                      filter='WorkQueue/queueFilter',
                                      query_params={'childUrl': self.queueUrl, 'parentUrl': self.parentCouchUrl},
                                      continuous=continuous,
                                      cancel=cancel)
        except Exception as ex:
            self.logger.warning('Replication to %s failed: %s' % (self.parentCouchUrl, str(ex)))

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
        dummy_values = {'name': wmspec.name()}
        # change specUrl in spec before saving (otherwise it points to previous url)
        wmspec.setSpecUrl(self.db['host'] + "/%s/%s/spec" % (self.db.name, wmspec.name()))
        return wmspec.saveCouch(self.hostWithAuth, self.db.name, dummy_values)

    def getWMSpec(self, name):
        """Get the spec"""
        wmspec = WMWorkloadHelper()
        wmspec.load(self.db['host'] + "/%s/%s/spec" % (self.db.name, name))
        return wmspec

    def insertElements(self, units, parent=None):
        """
        Insert element to database

        @param parent is the parent WorkQueueObject these element's belong to.
                                            i.e. a workflow which has been split
        """
        if not units:
            return
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
            else:
                newUnitsInserted.append(unit)
            unit.save()
            unit._couch.commit(all_or_nothing=True)

        return newUnitsInserted

    def createWork(self, spec, **kwargs):
        """Return the Inbox element for this spec.

        This does not persist it to the database.
        """
        kwargs.update({'WMSpec': spec,
                       'RequestName': spec.name(),
                       'StartPolicy': spec.startPolicyParameters(),
                       'EndPolicy': spec.endPolicyParameters(),
                       'OpenForNewData': False
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
                raise ValueError(
                    "Can't specify extra filters (or return id's) when using element id's with getElements()")
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

    def availableWork(self, thresholds, siteJobCounts, team=None, wfs=None,
                      excludeWorkflows=None, numElems=9999999):
        """
        Get work which is available to be run

        Assume thresholds is a dictionary; keys are the site name, values are
        the maximum number of running jobs at that site.

        Assumes site_job_counts is a dictionary-of-dictionaries; keys are the site
        name and task priorities.  The value is the number of jobs running at that
        priority.

        It will pull work until it reaches the number of elements configured (numElems).
        Since it's also used for calculating free resources, default it to "infinity"

        Note: this method will be called with no limit of work elements when it's simply
        calculating the resources available (based on what is in LQ), before it gets work
        from GQ
        """
        self.logger.info("Getting up to %d available work from %s", numElems, self.queueUrl)

        excludeWorkflows = excludeWorkflows or []
        elements = []
        sortedElements = []

        # We used to pre-filter sites, looking to see if there are idle job slots
        # We don't do this anymore, as we may over-allocate
        # jobs to sites if the new jobs have a higher priority.

        # If there are no sites, punt early.
        if not thresholds:
            self.logger.error("No thresholds is set: Please check")
            return elements, thresholds, siteJobCounts

        options = {}
        options['include_docs'] = True
        options['descending'] = True
        options['resources'] = thresholds
        if team:
            options['team'] = team
            self.logger.info("setting team to %s" % team)
        if wfs:
            result = []
            for i in xrange(0, len(wfs), 20):
                options['wfs'] = wfs[i:i + 20]
                data = self.db.loadList('WorkQueue', 'workRestrictions', 'availableByPriority', options)
                result.extend(json.loads(data))
        else:
            result = self.db.loadList('WorkQueue', 'workRestrictions', 'availableByPriority', options)
            result = json.loads(result)
            if len(result) == 0:
                self.logger.info("""No available work in WQ or didn't pass workqueue restriction
                                    - check Pileup, site white list, etc""")
            self.logger.debug("Available Work:\n %s \n for resources\n %s" % (result, thresholds))
        # Iterate through the results; apply whitelist / blacklist / data
        # locality restrictions.  Only assign jobs if they are high enough
        # priority.
        for i in result:
            element = CouchWorkQueueElement.fromDocument(self.db, i)
            # filter out exclude list from abvaling
            if element['RequestName'] not in excludeWorkflows:
                sortedElements.append(element)

        # sort elements to get them in priority first and timestamp order
        sortedElements.sort(key=lambda element: element['CreationTime'])
        sortedElements.sort(key=lambda x: x['Priority'], reverse=True)

        for element in sortedElements:
            if numElems <= 0:
                self.logger.info("Reached the maximum number of elements to be pulled: %d", len(elements))
                break

            if not possibleSites(element):
                self.logger.info("No possible sites for %s with doc id %s", element['RequestName'], element.id)
                continue

            prio = element['Priority']
            possibleSite = None
            sites = thresholds.keys()
            random.shuffle(sites)
            for site in sites:
                if element.passesSiteRestriction(site):
                    # Count the number of jobs currently running of greater priority
                    curJobCount = sum([x[1] if x[0] >= prio else 0 for x in siteJobCounts.get(site, {}).items()])
                    self.logger.debug("Job Count: %s, site: %s thresholds: %s" % (curJobCount, site, thresholds[site]))
                    if curJobCount < thresholds[site]:
                        possibleSite = site
                        break

            if possibleSite:
                numElems -= 1
                self.logger.debug("Possible site exists %s" % str(possibleSite))
                elements.append(element)
                if possibleSite not in siteJobCounts:
                    siteJobCounts[possibleSite] = {}
                siteJobCounts[possibleSite][prio] = siteJobCounts[possibleSite].setdefault(prio, 0) + \
                                                    element['Jobs'] * element.get('blowupFactor', 1.0)
            else:
                self.logger.debug("No available resources for %s with doc id %s", element['RequestName'], element.id)

        return elements, thresholds, siteJobCounts

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
                element_id = row['id']
                try:
                    conflicting_elements = [CouchWorkQueueElement.fromDocument(db, db.document(element_id, rev)) \
                                            for rev in row['value']]
                    fixed_elements = fixElementConflicts(*conflicting_elements)
                    if self.saveElements(fixed_elements[0]):
                        self.saveElements(*fixed_elements[1:])  # delete others (if merged value update accepted)
                except Exception as ex:
                    self.logger.error("Error resolving conflict for %s: %s" % (element_id, str(ex)))

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
            for request in injectionStatus.keys():
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
            for wflow, docIds in idsByWflow.items():
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
