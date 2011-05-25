#!/usr/bin/env python
"""
WorkQueueBackend

Interface to WorkQueue persistent storage
"""

import random
import time

from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError, Document
from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement
from WMCore.Wrappers import JsonWrapper as json
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Lexicon import sanitizeURL

class WorkQueueBackend(object):
    """
    Represents persistent storage for WorkQueue
    """
    def __init__(self, db_url, db_name = 'workqueue',
                 inbox_name = 'workqueue_inbox', parentQueue = None,
                 queueUrl = None, logger = None):
        if logger:
            self.logger = logger
        else:
            import logging
            self.logger = logging
        self.server = CouchServer(db_url)
        self.parentCouchUrlWithAuth = parentQueue
        if parentQueue:
            self.parentCouchUrl = sanitizeURL(parentQueue)['url']
        else:
            self.parentCouchUrl = None
        self.db = self.server.connectDatabase(db_name, create = False)
        self.hostWithAuth = db_url
        self.inbox = self.server.connectDatabase(inbox_name, create = False)
        self.queueUrl = queueUrl or sanitizeURL(db_url)['url']

    def forceQueueSync(self):
        """Force a blocking replication
            - for use mainly in tests"""
        self.pullFromParent()
        self.sendToParent()

    def pullFromParent(self):
        """Replicate from parent couch - blocking"""
        try:
            if self.parentCouchUrl and self.queueUrl:
                self.server.replicate(source = self.parentCouchUrl,
                                      destination = "%s/%s" % (self.hostWithAuth, self.inbox.name),
                                      filter = 'WorkQueue/childQueueFilter',
                                      query_params = {'queueUrl' : self.queueUrl})
        except Exception, ex:
            self.logger.warning('Replication from %s failed: %s' % (self.parentCouchUrl, str(ex)))

    def sendToParent(self):
        """Replicate to parent couch - blocking"""
        try:
            if self.parentCouchUrl and self.queueUrl:
                self.server.replicate(source = "%s/%s" % (self.db['host'], self.inbox.name),
                                      destination = self.parentCouchUrlWithAuth,
                                      filter = 'WorkQueue/childQueueFilter',
                                      query_params = {'queueUrl' : self.queueUrl})
        except Exception, ex:
                self.logger.warning('Replication to %s failed: %s' % (self.parentCouchUrl, str(ex)))


    def getElementsForSplitting(self):
        """Returns the elements from the inbox that need to be split"""
        elements = self.getInboxElements(status = 'Negotiating')
        specs = {} # cache as may have multiple elements for same spec
        for ele in elements:
            if ele['RequestName'] not in specs:
                wmspec = WMWorkloadHelper()
                wmspec.load(self.parentCouchUrl + "/%s/spec" % ele['RequestName'])
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
        dummy_values = {'name' : wmspec.name()}
        # change specUrl in spec before saving (otherwise it points to previous url)
        wmspec.setSpecUrl(self.hostWithAuth + "/%s/%s/spec" % (self.db.name, wmspec.name()))
        return wmspec.saveCouch(self.hostWithAuth, self.db.name, dummy_values)


    def getWMSpec(self, name):
        """Get the spec"""
        wmspec = WMWorkloadHelper()
        wmspec.load(self.db['host'] + "/%s/%s/spec" % (self.db.name, name))
        return wmspec

    def insertElements(self, units, parent = None):
        """
        Insert element to database
        
        @param parent is the parent WorkQueueObject these element's belong to.
                                            i.e. a workflow which has been split
        """
        if not units:
            return
        # store spec file separately - assume all elements share same spec
        self.insertWMSpec(units[0]['WMSpec'])
        for unit in units:

            # cast to couch
            if not isinstance(unit, CouchWorkQueueElement):
                unit = CouchWorkQueueElement(self.db, elementParams = dict(unit))

            if parent:
                unit['ParentQueueId'] = parent.id
                unit['TeamName'] = parent['TeamName']

            unit.save()
        unit._couch.commit(timestamp = True, all_or_nothing = True)
        return

    def createWork(self, spec, team = None):
        """Return the Inbox element for this spec.
        
        This does not persist it to the database.
        """
        params = {'WMSpec' : spec,
                  'RequestName' : spec.name(), 'TeamName' : team,
                  'Status' : 'Acquired'}
        unit = CouchWorkQueueElement(self.inbox, elementParams = params)
        unit.id = spec.name()
        return unit

    def getElements(self, status = None, elementIDs = None, returnIdOnly = False,
                    db = None, loadSpec = False, WorkflowName = None, **elementFilters):
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

        if elementIDs:
            if elementFilters or status or returnIdOnly:
                raise ValueError, "Can't specify extra filters (or return id's) when using element id's with getElements()"
            elements = [CouchWorkQueueElement(db, i).load() for i in elementIDs]
        else:
            options = {'include_docs' : True, 'filter' : elementFilters, 'idOnly' : returnIdOnly}
            if status:
                key.append(status)
            if WorkflowName:
                # Stored in couch as RequestName
                options['filter']['RequestName'] = WorkflowName

            view = db.loadList('WorkQueue', 'filter', 'elementsByStatus', options, key)
            view = json.loads(view)
            if returnIdOnly:
                return view
            elements = [CouchWorkQueueElement.fromDocument(db, row) for row in view]

        if loadSpec:
            specs = {} # cache as may have multiple elements for same spec
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
        return self.getElements(*args, db = self.inbox, **kwargs)

    def getElementsForWorkflow(self, workflow):
        """Get elements for a workflow"""
        elements = self.db.loadView('WorkQueue', 'elementsByWorkflow', {'key' : workflow, 'include_docs' : True})
        return [CouchWorkQueueElement.fromDocument(self.db,
                                                   x['doc'])
                for x in elements.get('rows', [])]

    def getElementsForParent(self, parent):
        """Get elements with the given parent"""
        elements = self.db.loadView('WorkQueue', 'elementsByParent', {'key' : parent.id, 'include_docs' : True})
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
        for ans in answer:
            if 'error' in ans:
                msg = 'Couch error saving element: "%s", error "%s", reason "%s"'
                self.logger.error(msg % (ans['id'], ans['error'], ans['reason']))
                continue
            for element in elements:
                if element.id == ans['id']:
                    element.rev = ans['rev']
                    result.append(element)
                    break
        return result

    def updateElements(self, *elementIds, **updatedParams):
        """Update given element's (identified by id) with new parameters"""
        import urllib
        uri = "/" + self.db.name + "/_design/WorkQueue/_update/in-place/"
        data = {"updates" : json.dumps(updatedParams)}
        for ele in elementIds:
            thisuri = uri + ele + "?" + urllib.urlencode(data)
            answer = self.db.makeRequest(uri = thisuri, type = 'PUT')
        return


    def updateInboxElements(self, *elementIds, **updatedParams):
        """Update given inbox element's (identified by id) with new parameters"""
        import urllib
        uri = "/" + self.inbox.name + "/_design/WorkQueue/_update/in-place/"
        data = {"updates" : json.dumps(updatedParams)}
        for ele in elementIds:
            thisuri = uri + ele + "?" + urllib.urlencode(data)
            self.inbox.makeRequest(uri = thisuri, type = 'PUT')
        return


    def deleteElements(self, *elements):
        """Delete elements"""
        if not elements:
            return
        specs = {}
        for i in elements:
            i.delete()
            specs[i['RequestName']] = None
        elements[0]._couch.commit()
        # delete specs if no longer used
        for wf in specs:
            try:
                if not self.db.loadView('WorkQueue', 'elementsByWorkflow',
                                        {'key' : wf, 'limit' : 0})['total_rows']:
                    self.db.delete_doc(wf)
            except CouchNotFoundError:
                pass


    def availableWork(self, conditions, teams = None):
        """Get work which is available to be run"""
        elements = []
        for site in conditions.keys():
            if not conditions[site] > 0:
                del conditions[site]
        if not conditions:
            return elements, conditions

        options = {}
        options['include_docs'] = True
        options['descending'] = True
        options['resources'] = conditions
        if teams:
            options['teams'] = teams
        result = self.db.loadList('WorkQueue', 'workRestrictions', 'availableByPriority', options)
        result = json.loads(result)
        for i in result:
            element = CouchWorkQueueElement.fromDocument(self.db, i)
            elements.append(element)

            # Remove 1st random site that can run work
            names = conditions.keys()
            random.shuffle(names)
            for site in names:
                if element.passesSiteRestriction(site):
                    slots_left = conditions[site] - element['Jobs']
                    if slots_left > 0:
                        conditions[site] = slots_left
                    else:
                        conditions.pop(site, None)
                    break
        return elements, conditions

    def getActiveData(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'activeData', {'reduce' : True, 'group' : True})
        return [{'dbs_url' : x['key'][0],
                 'name' : x['key'][1]} for x in data.get('rows', [])]

    def getActiveParentData(self):
        """Get data items we have work in the queue for with parent"""
        data = self.db.loadView('WorkQueue', 'activeParentData', {'reduce' : True, 'group' : True})
        return [{'dbs_url' : x['key'][0],
                 'name' : x['key'][1]} for x in data.get('rows', [])]

    def getElementsForData(self, dbs, data):
        """Get active elements for this dbs & data combo"""
        elements = self.db.loadView('WorkQueue', 'elementsByData', {'key' : data, 'include_docs' : True})
        return [CouchWorkQueueElement.fromDocument(self.db,
                                                   x['doc'])
                for x in elements.get('rows', [])]

    def getElementsForParentData(self, data):
        """Get active elements for this data """
        elements = self.db.loadView('WorkQueue', 'elementsByParentData', {'key' : data, 'include_docs' : True})
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
        except Exception, ex:
            self.logger.error("CouchDB unavailable: %s" % str(ex))
            return False
        return True

    def queueLength(self):
        """Return number of available elements"""
        return self.db.loadView('WorkQueue', 'availableByPriority', {'limit' : 0})['total_rows']

    def fixConflicts(self):
        """Fix elements in conflict

        Each local queue runs this to resolve its conflicts with global,
        resolution propagates up to global.

        Conflicting elements are merged into one element with others deleted.

        This will fail if elements are modified during the resolution -
        if this happens rerun.
        """
        ordered_states = ['Available', 'Negotiating', 'Acquired', 'Running',
                          'Done', 'Failed', 'CancelRequested', 'Canceled']
        allowed_keys = ['Status', 'EventsWritten', 'FilesProcessed', 'PercentComplete', 'PercentSuccess']
        for db in [self.inbox, self.db]:
            conflicts = db.loadView('WorkQueue', 'conflicts')
            queue = []
            for row in conflicts['rows']:
                previous_value = None
                element_id = row['id']
                for rev in row['value']: # loop over conflicting revisions
                    ele = CouchWorkQueueElement.fromDocument(db, db.document(element_id, rev))
                    if not previous_value: # 1st will contain merged result and become winner
                        previous_value = ele
                        continue
    
                    for key in previous_value:
                        if previous_value[key] == ele.get(key):
                            continue
                        # we need to merge: Take elements from both that seem most advanced, e.g. status & progress stats
                        if key not in allowed_keys:
                            msg = 'Unable to merge conflicting element keys: field "%s" value 1 "%s" value2 "%s"'
                            raise RuntimeError, msg % (key, previous_value.get(key), ele.get(key))
                        if key == 'Status':
                            if ordered_states.index(ele[key]) > ordered_states.index(previous_value[key]):
                                previous_value[key] = ele[key]
                        elif ele[key] > previous_value[key]:
                            previous_value[key] = ele[key]
                    # once losing element has been merged - queue for deletion
                    queue.append(ele)
                # conflict resolved - save element and delete losers
                msg = 'Resolving conflict for wf "%s", id "%s": Losing rev(s): %s'
                self.logger.info(msg % (str(previous_value['RequestName']),
                                         str(previous_value.id),
                                         ", ".join([x._document['_rev'] for x in queue])))
                if self.saveElements(previous_value):
                    for i in queue:
                        i.delete() # delete others (if merged value update accepted)
                    self.saveElements(*queue)

    def recordTaskActivity(self, taskname, comment = ''):
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
        except StandardError, ex:
            self.logger.error("Unable to update task %s freshness: %s" % (taskname, str(ex)))
