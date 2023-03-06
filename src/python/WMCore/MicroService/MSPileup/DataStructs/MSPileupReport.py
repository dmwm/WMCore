#!/usr/bin/env python
"""
File       : MSPileupReport.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileup report module
"""

# WMCore modules
from Utils.Timers import gmtimeSeconds, encodeTimestamp


class MSPileupReport():
    """
    MSPileupReport class represents MSPileup report object(s)
    """
    def __init__(self, autoExpire=3600, autoCleanup=False):
        """
        Constructor for MSPileup object
        """
        self.docs = []
        self.autoExpire = autoExpire
        self.autoCleanup = autoCleanup

    def addEntry(self, task, uuid, entry):
        """
        Add new entry into MSPileup documents

        :param task: task name
        :param uuid: unique id of the entry
        :param entry: entry message or any other object to store
        """
        if self.autoCleanup:
            self.purgeExpired()
        gmtime = gmtimeSeconds()
        report = {'gmtime': gmtime, 'uuid': uuid,
                  'timestamp': encodeTimestamp(gmtime),
                  'entry': entry, 'task': task}
        self.docs.append(report)

    def purgeExpired(self):
        """
        Purge expired records from internal docs
        """
        gmtime = gmtimeSeconds()
        for entry in list(self.docs):
            if gmtime - entry['gmtime'] > self.autoExpire:
                self.docs.remove(entry)

    def getDocuments(self):
        """
        Return report documents
        """
        if self.autoCleanup:
            self.purgeExpired()
        return self.docs

    def getReportByUuid(self):
        """
        Return report documents in dictonary form with uuid's as keys
        """
        if self.autoCleanup:
            self.purgeExpired()
        rdict = {}
        for doc in self.docs:
            uuid = doc['uuid']
            timestamp = doc['timestamp']
            entry = doc['entry']
            task = doc['task']
            record = f"{timestamp} {task} task {uuid} {entry}"
            rdict.setdefault(uuid, []).append(record)
        return rdict
