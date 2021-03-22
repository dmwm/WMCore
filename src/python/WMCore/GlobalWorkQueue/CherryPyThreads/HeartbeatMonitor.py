from __future__ import (division, print_function)

from future.utils import viewitems

from time import time
from WMCore.REST.HeartbeatMonitorBase import HeartbeatMonitorBase
from WMCore.WorkQueue.WorkQueue import globalQueue


class HeartbeatMonitor(HeartbeatMonitorBase):
    def __init__(self, rest, config):
        super(HeartbeatMonitor, self).__init__(rest, config)
        self.initialStatus = ['Available', 'Negotiating', 'Acquired']
        self.producer = "global_workqueue"
        self.docTypeAMQ = "cms_%s_info" % self.producer
        self.globalQ = globalQueue(logger=self.logger, **config.queueParams)

    def addAdditionalMonitorReport(self, config):
        """
        Collect some statistics for Global Workqueue and upload it to WMStats and
        MonIT.
        """
        tStart = time()
        self.logger.info("Collecting GlobalWorkqueue statistics...")

        # retrieve whole docs for these status in order to create site metrics
        results = self.globalQ.monitorWorkQueue(self.initialStatus)

        if self.postToAMQ:
            allDocs = self.buildMonITDocs(results)
            self.uploadToAMQ(allDocs)
        self.logger.info("%s executed in %.3f secs.", self.__class__.__name__, time() - tStart)

        return results

    def buildMonITDocs(self, stats):
        """
        Given the statistics that are uploaded to wmstats, create different
        documents to post to MonIT AMQ (aggregation-friendly docs).
        """
        mapMetricToType = {'uniqueJobsPerSite': 'work_site_unique',  # respecting data location constraints
                           'possibleJobsPerSite': 'work_site_possible',
                           'uniqueJobsPerSiteAAA': 'work_site_uniqueAAA',  # assume work can run anywhere
                           'possibleJobsPerSiteAAA': 'work_site_possibleAAA'}
        commonInfo = {"agent_url": "global_workqueue"}

        docs = []
        for status, data in viewitems(stats['workByStatus']):
            doc = dict()
            doc["type"] = "work_info"
            doc["status"] = status
            doc["num_elem"] = data.get('num_elem', 0)  # total number of workqueue elements
            doc["sum_jobs"] = data.get('sum_jobs', 0)  # total number of top level jobs
            doc["max_jobs_elem"] = data.get('max_jobs_elem', 0)  # largest # of jobs found in a WQE
            docs.append(doc)

        for status, data in viewitems(stats['workByStatusAndPriority']):
            for item in data:
                doc = dict()
                doc["type"] = "work_prio_status"
                doc["status"] = status
                doc.update(item)
                docs.append(doc)

        for item in stats['workByAgentAndStatus']:
            doc = dict()
            doc["type"] = "work_agent_status"
            doc["status"] = item['status']
            doc["agent_name"] = item['agent_name']
            doc["num_elem"] = item['num_elem']  # total number of workqueue elements
            doc["sum_jobs"] = item['sum_jobs']  # total number of top level jobs
            doc["max_jobs_elem"] = item['max_jobs_elem']  # largest # of jobs found in a WQE
            docs.append(doc)

        for item in stats['workByAgentAndPriority']:
            doc = dict()
            doc["type"] = "work_agent_prio"
            doc["priority"] = item['priority']
            doc["agent_name"] = item['agent_name']
            doc["num_elem"] = item['num_elem']  # total number of workqueue elements
            doc["sum_jobs"] = item['sum_jobs']  # total number of top level jobs
            doc["max_jobs_elem"] = item['max_jobs_elem']  # largest # of jobs found in a WQE
            docs.append(doc)

        # let's remap Jobs --> sum_jobs , and NumElems --> num_elem
        for metric in mapMetricToType:
            for status, sites in viewitems(stats[metric]):
                if not sites:
                    # no work in this status available for any sites, skip!
                    continue
                for site in sites:
                    doc = dict()
                    doc["type"] = mapMetricToType[metric]
                    doc["status"] = status
                    doc["site_name"] = site
                    doc['num_elem'] = sites[site]['num_elem']
                    doc['sum_jobs'] = int(sites[site]['sum_jobs'])
                    docs.append(doc)

        # mark every single document as being from global_workqueue
        for doc in docs:
            doc.update(commonInfo)

        self.logger.info("%i docs created to post to MonIT", len(docs))
        return docs
