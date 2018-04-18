from __future__ import (division, print_function)
import time
from pprint import pformat
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter, convertToServiceCouchDoc
from WMCore.Services.StompAMQ.StompAMQ import StompAMQ


class HeartbeatMonitorBase(CherryPyPeriodicTask):

    def __init__(self, rest, config):
        super(HeartbeatMonitorBase, self).__init__(config)
        self.centralWMStats = WMStatsWriter(config.wmstats_url)
        self.threadList = config.thread_list
        self.userAMQ = getattr(config, "user_amq", None)
        self.passAMQ = getattr(config, "pass_amq", None)
        self.postToAMQ = getattr(config, "post_to_amq", False)
        self.topicAMQ = getattr(config, "topic_amq", None)
        self.hostPortAMQ = getattr(config, "host_port_amq", None)

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.reportToWMStats, 'duration': config.heartbeatCheckDuration}]

    def reportToWMStats(self, config):
        """
        report thread status and heartbeat.
        Also can report additional monitoring information by rewriting addAdditionalMonitorReport method
        """
        self.logger.info("Checking Thread status...")
        downThreadInfo = self.logDB.wmstats_down_components_report(self.threadList)
        monitorInfo = self.addAdditionalMonitorReport(config)
        downThreadInfo.update(monitorInfo)
        wqSummaryDoc = convertToServiceCouchDoc(downThreadInfo, config.log_reporter)
        self.centralWMStats.updateAgentInfo(wqSummaryDoc)

        self.logger.info("Uploaded to WMStats...")

        return

    def addAdditionalMonitorReport(self, config):
        """
        add Additonal report with heartbeat report
        overwite the method with each applications monitoring info. (Need to follow the format displayed in wmstats)
        """
        return {}

    def uploadToAMQ(self, docs, producer=None):
        """
        _uploadToAMQ_

        Sends data to AMQ, which ends up in elastic search.
        :param docs: list of documents/dicts to be posted
        :param producer: service name that's providing this info
        """
        if not docs:
            self.logger.info("There are no documents to send to AMQ")
            return

        producer = producer or self.producer
        self.logger.debug("Sending the following data to AMQ %s", pformat(docs))
        ts = int(time.time())

        try:
            stompSvc = StompAMQ(username=self.userAMQ,
                                password=self.passAMQ,
                                producer=producer,
                                topic=self.topicAMQ,
                                host_and_ports=self.hostPortAMQ,
                                logger=self.logger)

            notifications = stompSvc.make_notification(payload=docs, docType=self.docTypeAMQ,
                                                       docId=producer, ts=ts)

            failures = stompSvc.send(notifications)
            self.logger.info("%i docs successfully sent to Stomp AMQ", len(notifications) - len(failures))
        except Exception as ex:
            self.logger.exception("Failed to send data to StompAMQ. Error %s", str(ex))

        return
