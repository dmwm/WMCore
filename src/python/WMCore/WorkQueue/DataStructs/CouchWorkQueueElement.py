#!/usr/bin/env python
# encoding: utf-8
"""
CouchWorkQueueElement.py

Created by Dave Evans on 2010-10-12.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from future.utils import viewitems

import unittest
import time
import logging

from WMCore.Database.CMSCouch import Document
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement



class CouchWorkQueueElement(WorkQueueElement):
    """
    _CouchWorkQueueElement_

    """
    def __init__(self, couchDB, id = None, elementParams = None):
        elementParams = elementParams or {}
        WorkQueueElement.__init__(self, **elementParams)
        if id:
            self._id = id
        self._document = Document(id = id)
        self._couch = couchDB

    rev = property(
        lambda x: str(x._document[u'_rev']) if u'_rev' in x._document else x._document.__getitem__('_rev'),
        lambda x, newid: x._document.__setitem__('_rev', newid))
    timestamp = property(
        lambda x: str(x._document[u'timestamp']) if u'timestamp' in x._document else x._document.__getitem__('timestamp')
        )
    updatetime = property(
        lambda x: str(x._document[u'updatetime']) if u'updatetime' in x._document else 0
        )


    @classmethod
    def fromDocument(cls, couchDB, doc):
        """Create element from couch document"""
        elementParams = doc.pop('WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement')
        elementParams["CreationTime"] = doc.pop('timestamp')
        element = CouchWorkQueueElement(couchDB = couchDB,
                                        id = doc['_id'],
                                        elementParams = elementParams)
        element._document['_rev'] = doc.pop('_rev')
        element._document['timestamp'] = elementParams["CreationTime"]
        element._document['updatetime'] = doc.pop('updatetime')
        return element

    def save(self):
        """
        _save
        """
        self.populateDocument()
        self._couch.queue(self._document)

    def load(self):
        """
        _load_

        Load the document representing this WQE
        """
        document = self._couch.document(self._document['_id'])
        self.update(document.pop('WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'))
        self._document['_rev'] = document.pop('_rev')
        self._document['timestamp'] = document.pop('timestamp', None)
        self._document['updatetime'] = document.pop('updatetime', None)
        return self

    def delete(self):
        """Delete element"""
        self.populateDocument()
        self._document.delete()
        self._couch.queue(self._document)

    def populateDocument(self):
        """Certain attributed shouldn't be stored"""
        self._document.update(self.__to_json__(None))
        now = time.time()
        self._document['updatetime'] = now
        self._document.setdefault('timestamp', now)
        if not self._document.get('_id') and self.id:
            self._document['_id'] = self.id
        attrs = ['WMSpec', 'Task']
        for attr in attrs:
            self._document['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'].pop(attr, None)



def fixElementConflicts(*elements):
    """Take conflicting elements, fix conflicts and return
    First element returned will contain the merged results,
    the others will be losing revisions
    """
    ordered_states = ['Available', 'Negotiating', 'Acquired', 'Running',
                      'Done', 'Failed', 'CancelRequested', 'Canceled']
    allowed_keys = ['Status', 'EventsWritten', 'FilesProcessed',
                    'PercentComplete', 'PercentSuccess', 'Inputs', 'NumOfFilesAdded',
                    'SubscriptionId', 'Priority', 'SiteWhitelist', 'SiteBlacklist']
    merged_value = None
    updated = set()
    for ele in elements:
        if not merged_value: # 1st will contain merged result and become winner
            merged_value = ele
            continue

        # print differences
        from WMCore.Algorithms.MiscAlgos import dict_diff
        logging.info("Conflict between %s revs %s & %s: %s",
                         ele.id, merged_value.rev, ele.rev,
                         "; ".join("%s=%s" % (x,y) for x,y in viewitems(dict_diff(merged_value, ele)))
                    )
        for key in merged_value:
            if merged_value[key] == ele.get(key):
                continue
            # we need to merge: Take elements from both that seem most advanced, e.g. status & progress stats
            if key not in allowed_keys:
                msg = 'Unable to merge conflicting element %s: field "%s" value 1 "%s" value2 "%s"'
                raise RuntimeError(msg % (ele.id, key, merged_value.get(key), ele.get(key)))
            elif key == 'Status':
                if ordered_states.index(ele[key]) > ordered_states.index(merged_value[key]):
                    merged_value[key] = ele[key]
            elif key == 'Inputs':
                for item in merged_value[key]:
                    # take larger locations list
                    if merged_value[key][item] < ele[key].get(item, []):
                        merged_value[key][item] = ele[key][item]
            elif ele[key] is not None and ele[key] > merged_value[key]:
                merged_value[key] = ele[key]
            updated.add(key)
        # once losing element has been merged - queue for deletion
        ele._document.delete()

    msg = 'Resolving conflict for wf "%s", id "%s": Remove rev(s): %s: Updates: (%s)'
    logging.info(msg, str(merged_value['RequestName']),
                 str(merged_value.id),
                 ", ".join([x._document['_rev'] for x in elements[1:]]),
                 "; ".join("%s=%s" % (x, merged_value[x]) for x in updated)
                 )
    return elements
