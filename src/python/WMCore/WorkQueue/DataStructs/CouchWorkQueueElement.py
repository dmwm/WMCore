#!/usr/bin/env python
# encoding: utf-8
"""
CouchWorkQueueElement.py

Created by Dave Evans on 2010-10-12.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import math
import unittest
import time

from WMCore.DataStructs.Run import Run
from WMCore.Database.CMSCouch import Document, CouchServer
from WMCore.WMFactory import WMFactory
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement

class SequenceFactory(WMFactory):
    """
    _SequenceFactory_

    QND factory for loading sequence implementations

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WorkQueue.Sequences")
_SequenceFactory = SequenceFactory()
getSequence = lambda x: _SequenceFactory.loadObject(x)



class CouchConnectionError(Exception):
    """docstring for CouchConnectionError"""
    def __init__(self, arg):
        super(CouchConnectionError, self).__init__()
        self.arg = arg





def requireCouch(funcRef):
    """decorator to check couch db"""
    def wrapper(self, *args, **opts):
        if self.couch == None:
            if self.cdb_url == None:
                msg = "url for couch service not provided"
                raise CouchConnectionError(msg)
            if self.cdb_database == None:
                msg = "database name for couch service not provided"
                raise CouchConnectionError(msg)
            self.initCouch()
        return funcRef(self, *args, **opts)
    return wrapper


def requireDocId(func):
    """decorator to check for couch doc ID"""
    def wrapper(self, *args, **opts):
        if self.document_id == None:
            msg = "Document ID is not provided for WorkQueueElement"
            raise RuntimeError, msg
        return func(self, *args, **opts)
    return wrapper

def requireSequence(func):
    """decorator to check that the sequence instance has been loaded"""
    def wrapper(self, *args, **opts):
        if self.sequence == None:
            self.sequence = getSequence(self.sequence_name)
        return func(self, *args, **opts)
    return wrapper




class WorkQueueElementUnits(object):
    """
    Namespace container for units used in WQEs
    """
    Run   = "WorkQueueElementUnits.Run"
    Event = "WorkQueueElementUnits.Lumi"
    Lumi  = "WorkQueueElementUnits.Event"
    File  = "WorkQueueElementUnits.File"
    Block = "WorkQueueElementUnits.Block"

    def _validate(self, unitString):
        """
        _validate_
        """
        if not unitString.startswith("WorkQueueElementUnits."):
            raise RuntimeError("Invalid WorkQueueElement Unit: %s" % unitString)
        unit = unitString.split("WorkQueueElementUnits.", 1)[1]
        check = getattr(WorkQueueElementUnits, unit, None)
        if check == None:
            raise RuntimeError("Invalid WorkQueueElement Unit: %s" % unitString)
        return check

    validate = staticmethod(_validate)




class CouchWorkQueueElement_notused(object):
    """
    _CouchWorkQueueElement_

    """
    def __init__(self, couchDB, doc = None, elementParams = None):
        elementParams = elementParams or {}
        self.document = Document(doc)
        self.couch = couchDB
        self.sequence = None
        self.document[u'element'] = WorkQueueElement(**elementParams)
        self.document[u'data'] = {}
        self.document[u'state'] = {}

#    def initCouch(self):
#        """initCouch"""
#        try:
#            self.cdb_server = CouchServer(self.cdb_url)
#            self.couch = self.cdb_server.connectDatabase(self.cdb_database)
#        except Exception, ex:
#            msg = "Unable to connect to Couch Database:\n"
#            msg += str(ex)
#            raise CouchConnectionError(msg)

    document_id = property(
        lambda x: str(x.document[u'_id']) if x.document.has_key(u'_id') else x.document.__getitem__('_id'),
        lambda x, newid: x.document.__setitem__('_id', newid))

    element = property(
        lambda x : x.document.get("element", {})
        )
    element_unit = property(
        lambda x : str(x.element[u'unit']) if x.element.has_key(u'unit') else x.element.get('unit', None),
        lambda x, u : x.element.__setitem__('unit', WorkQueueElementUnits.validate(u) )
        )


    sequence_name = property(
        lambda x: str(x.element[u'sequence']) if x.element.has_key(u'sequence') else x.element.get('sequence', None)
        )

    element_data = property(
        lambda x: x.document.get("data", {})
    )
    element_state = property(
        lambda x: x.document.get("state", {})
    )

    @requireCouch
    @requireDocId
    def load(self):
        """
        _load_

        Load the document representing this WQE
        """
        self.document = self.couch.document(self.document['_id'])
        return

    def create(self):
        """
        _create_

        Create a couch document for this WQE

        """
        retval = self.couch.commitOne(self.document)
        self.document['_id'] = retval[0]['id']
        return

    def newElement(self):
        """
        _newElement_

        create a new WQE that is a sub element of this one

        Can override the couch database being used if required (propagate to a different workqueue)

        """
        wqe = CouchWorkQueueElement(self.couch, None)
        wqe.sequence = getSequence(self.sequence_name)
        wqe.document[u'element'].update(self.element)
        wqe.document[u'element']['parent'] = self.document_id
        wqe.sequence.start(self.sequence)
        return wqe

    @requireCouch
    @requireDocId
    def save(self):
        """
        _save
        """
        self.couch.commitOne(self.document)

    @requireSequence
    def split(self, n):
        """
        _split_

        Split this CouchWorkQueueElement into n CouchWorkQueueElements containing an nth fraction of the
        work in this element where n is measured in the element_unit of this WQE

        """
        self.sequence.init(self)
        subElements = []
        fraction = float(self.size())/float(n)
        fraction = int(math.ceil(fraction))
        count = 0
        thisElement = None
        for unitOfData in self.sequence:
            if count == 0:
                thisElement = self.newElement()

            thisElement.sequence.append(unitOfData)
            count += 1


            if count == fraction:
                thisElement.sequence.end(thisElement)
                subElements.append(thisElement)
                count = 0

        if thisElement.sequence.size() > 0:
            thisElement.sequence.end(thisElement)
            subElements.append(thisElement)
        return subElements

    @requireSequence
    def size(self):
        """
        _size_

        How big is this work queue element?
        What are we actually counting? runs, lumis, events, files?
        Should this be several methods?

        Define the unit of size/splitting as an attribute of the WQE and then use that to calculate
        the size of it, divide it etc
        """
        self.sequence.init(self)
        return self.sequence.size()


class RunSequenceTests(unittest.TestCase):
    def setUp(self):
        self.couchUrl = "http://evansde:Gr33nMan@127.0.0.1:5984"
        self.couchName = "workqueue1"

        pass




    def testB(self):
        """wqe"""

        wqe = CouchWorkQueueElement("c10e784fb9ba922ab83fd8bdbb0004c0", self.couchUrl, self.couchName)


        wqe.load()


        wqe2 = CouchWorkQueueElement("c10e784fb9ba922ab83fd8bdbb000ef1", self.couchUrl, self.couchName)
        wqe2.load()

        #wqe3 = CouchWorkQueueElement(None, self.couchUrl, self.couchName)
        #wqe3.create()

        #print wqe3.element_unit


        print wqe.sequence_name
        print wqe.size()
        print "made ", len(wqe.split(2)), "elements"
        print

        print wqe2.sequence_name
        print wqe2.size()
        print "made", len(wqe2.split(3)) , "elements"
        print

        wqe3 = CouchWorkQueueElement("9782b750675da95573a2be48ad011a5e", self.couchUrl, self.couchName)
        wqe3.load()
        print wqe3.sequence_name
        print wqe3.size()
        print "made ", len(wqe3.split(2)), "elements"
        # print wqe2.sequence_name
        #         print wqe2.size()
        #         for o1 in wqe2.split(2):
        #             o1.create(wqe2.cdb_url, "workqueue2")
        #             for o2 in o1.split(5):
        #                 print o2
        #
        #for o in output:
        #    o.create(wqe2.cdb_url, "workqueue2")


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

if __name__ == '__main__':
    unittest.main()
