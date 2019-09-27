from __future__ import (division, print_function)
from WMCore.REST.HeartbeatMonitorBase import HeartbeatMonitorBase

class HeartbeatMonitor(HeartbeatMonitorBase):

    def addAdditionalMonitorReport(self, config):

        self.logger.info("Collecting MicroServices statistics...")
        if self.postToAMQ:
            allDocs = []
            doc = dict()
            doc["type"] = "work_info"
            doc'status'] = 'Available'
            self.uploadToAMQ(allDocs)
