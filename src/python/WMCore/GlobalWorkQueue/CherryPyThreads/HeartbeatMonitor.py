from __future__ import (division, print_function)

from WMCore.REST.HeartbeatMonitorBase import HeartbeatMonitorBase
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.WorkQueue.DataStructs.WorkQueueElement import STATES


class HeartbeatMonitor(HeartbeatMonitorBase):

    def __init__(self, rest, config):
        super(HeartbeatMonitor, self).__init__(rest, config)
        self.initialStatus = ['Available', 'Negotiating', 'Acquired']
        self.producer = "global_workqueue"
        self.docTypeAMQ = "cms_%s_info" % self.producer

    def addAdditionalMonitorReport(self, config):
        """
        Collect some statistics for Global Workqueue and upload it to WMStats and
        MonIT.
        """
        self.logger.info("Collecting GlobalWorkqueue statistics...")

        # retrieve whole docs for these status in order to create site metrics
        globalQ = globalQueue(**config.queueParams)
        results = globalQ.monitorWorkQueue(self.initialStatus)

        if self.postToAMQ:
            allDocs = self.buildMonITDocs(results)
            self.uploadToAMQ(allDocs)

        return results

    def _fillMissingStatus(self, data):
        """
        Utilitarian method which creates an entry in the stats for each
        status that is missing, such that it can be better used in MonIT
        aggregations
        :param data: list of dicts for one specific metric
        :return: the list updated in-place
        """
        if data:
            defaultStruct = dict(data[0])  # make a copy of it
            for keyName in defaultStruct:
                if keyName == 'agent_name':
                    defaultStruct[keyName] = 'AgentNotDefined'
                else:
                    defaultStruct[keyName] = 0
        else:
            return

        availableStatus = set([item['status'] for item in data])
        missingStatus = set(STATES) - availableStatus
        for st in missingStatus:
            defaultStruct['status'] = st
            data.append(dict(defaultStruct))
        return

    def buildMonITDocs(self, stats):
        """
        Given the statistics that are uploaded to wmstats, create different
        documents to post to MonIT AMQ (aggregation-friendly docs).
        """
        commonInfo = {"agent_url": "global_workqueue"}

        docs = []
        self._fillMissingStatus(stats['workByStatus'])
        for item in stats['workByStatus']:
            doc = dict()
            doc["type"] = "work_info"
            doc["status"] = item['status']
            doc["count"] = item['count']  # total number of workqueue elements
            doc["sum"] = item['sum']  # total number of top level jobs
            doc.update(commonInfo)
            docs.append(doc)

        self._fillMissingStatus(stats['workByStatusAndPriority'])
        for item in stats['workByStatusAndPriority']:
            doc = dict()
            doc["type"] = "work_prio_status"
            doc["status"] = item['status']
            doc["priority"] = item['priority']
            doc["count"] = item['count']  # total number of workqueue elements
            doc["sum"] = item['sum']  # total number of top level jobs
            doc.update(commonInfo)
            docs.append(doc)

        self._fillMissingStatus(stats['workByAgentAndStatus'])
        for item in stats['workByAgentAndStatus']:
            doc = dict()
            doc["type"] = "work_agent_status"
            doc["status"] = item['status']
            doc["agent_name"] = item['agent_name']
            doc["count"] = item['count']  # total number of workqueue elements
            doc["sum"] = item['sum']  # total number of top level jobs
            doc.update(commonInfo)
            docs.append(doc)

        for item in stats['workByAgentAndPriority']:
            doc = dict()
            doc["type"] = "work_agent_prio"
            doc["priority"] = item['priority']
            doc["agent_name"] = item['agent_name']
            doc["count"] = item['count']  # total number of workqueue elements
            doc["sum"] = item['sum']  # total number of top level jobs
            doc.update(commonInfo)
            docs.append(doc)

        # jobs respecting data location constraints
        for status, items in stats['uniqueJobsPerSite'].iteritems():
            for item in items:
                doc = {}
                doc["type"] = "work_site_unique"
                doc["status"] = status
                doc.update(commonInfo)
                doc.update(item)
                docs.append(doc)
        for status, items in stats['possibleJobsPerSite'].iteritems():
            for item in items:
                doc = {}
                doc["type"] = "work_site_possible"
                doc["status"] = status
                doc.update(commonInfo)
                doc.update(item)
                docs.append(doc)

        # jobs NOT respecting any data location, so assuming they can run anywhere on the sitewhitelist
        for status, items in stats['uniqueJobsPerSiteAAA'].iteritems():
            for item in items:
                doc = {}
                doc["type"] = "work_site_uniqueAAA"
                doc["status"] = status
                doc.update(commonInfo)
                doc.update(item)
                docs.append(doc)
        for status, items in stats['possibleJobsPerSiteAAA'].iteritems():
            for item in items:
                doc = {}
                doc["type"] = "work_site_possibleAAA"
                doc["status"] = status
                doc.update(commonInfo)
                doc.update(item)
                docs.append(doc)

        self.logger.info("%i docs created to post to MonIT", len(docs))
        return docs
