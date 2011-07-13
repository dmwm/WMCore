#!/usr/bin/env python
# encoding: utf-8
"""
CouchWorkQueueElement.py

Created by Dave Evans on 2010-10-12.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import time

from WMCore.Database.CMSCouch import Document
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement



class CouchWorkQueueElement(WorkQueueElement):
    """
    _CouchWorkQueueElement_

    """
    def __init__(self, couchDB, id = None, elementParams = None):
        elementParams = elementParams or {}
        WorkQueueElement.__init__(self, **elementParams)
        self._document = Document(id = id)
        self._couch = couchDB

    id = property(
        lambda x: str(x._document[u'_id']) if x._document.has_key(u'_id') else x._document.__getitem__('_id'),
        lambda x, newid: x._document.__setitem__('_id', newid))
    rev = property(
        lambda x: str(x._document[u'_rev']) if x._document.has_key(u'_rev') else x._document.__getitem__('_rev'),
        lambda x, newid: x._document.__setitem__('_rev', newid))
    timestamp = property(
        lambda x: str(x._document[u'timestamp']) if x._document.has_key(u'timestamp') else x._document.__getitem__('timestamp')
        )
    updatetime = property(
        lambda x: str(x._document[u'updatetime']) if x._document.has_key(u'updatetime') else 0
        )


    @classmethod
    def fromDocument(cls, couchDB, doc):
        """Create element from couch document"""
        element = CouchWorkQueueElement(couchDB = couchDB,
                                        id = doc['_id'],
                                        elementParams = doc.pop('WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement')
                                        )
        element._document['_rev'] = doc.pop('_rev')
        element._document['timestamp'] = doc.pop('timestamp')
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
        self._document['timestamp'] = document.pop('timestamp')
        self._document['updatetime'] = document.pop('updatetime')
        return self

    def delete(self):
        """Delete element"""
        self.populateDocument()
        self._document.delete()
        self._couch.queue(self._document)

    def populateDocument(self):
        """Certain attributed shouldn't be stored"""
        self._document.update(self.__to_json__(None))
        self._document['updatetime'] = time.time()
        attrs = ['WMSpec', 'Task']
        for attr in attrs:
            self._document['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'].pop(attr, None)
