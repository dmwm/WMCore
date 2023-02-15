#!/usr/bin/env python
"""
File       : MSPileupTaskManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileupTaskManager handles MSPileupTasks

In particular, it perform the following tasks each polling cycle:
    - fetches pileup sizes for all pileup documents in database back-end
    - update RSE quotas
    - perform monitoring task
    - perform task for active pileups using up-to-date RSE quotas
    - perform task for inactive pileups
"""

# system modules
import os
from threading import current_thread

# WMCore modules
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.MicroService.DataStructs.DefaultStructs import PILEUP_REPORT
from WMCore.MicroService.MSPileup.MSPileupData import MSPileupData
from WMCore.MicroService.MSPileup.MSPileupTasks import MSPileupTasks
from WMCore.MicroService.MSTransferor.DataStructs.RSEQuotas import RSEQuotas
from WMCore.MicroService.Tools.PycurlRucio import getPileupContainerSizesRucio, getRucioToken
from WMCore.Services.Rucio.Rucio import Rucio


class MSPileupTaskManager(MSCore):
    """
    MSPileupTaskManager handles MSPileup tasks
    """

    def __init__(self, msConfig, **kwargs):
        super().__init__(msConfig, **kwargs)
        self.marginSpace = msConfig.get('marginSpace', 1024**4)
        self.rucioAccount = msConfig.get('rucioAccount', 'ms-pileup')
        self.rucioUrl = msConfig.get('rucioHost', 'http://cms-rucio.cern.ch')
        self.rucioAuthUrl = msConfig.get('authHost', 'https://cms-rucio-auth.cern.ch')
        creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                 "client_key": os.getenv("X509_USER_KEY", "Unknown")}
        configDict = {'rucio_host': self.rucioUrl, 'auth_host': self.rucioAuthUrl,
                      'creds': creds, 'auth_type': 'x509'}
        self.rucioClient = Rucio(self.rucioAccount, configDict=configDict)
        self.dataManager = MSPileupData(msConfig)
        self.mgr = MSPileupTasks(self.dataManager, self.logger,
                                 self.rucioAccount, self.rucioClient)
        self.rseQuotas = RSEQuotas(self.rucioAccount, msConfig["quotaUsage"],
                                   minimumThreshold=msConfig["minimumThreshold"],
                                   verbose=msConfig['verbose'], logger=self.logger)

    def status(self):
        """
        Provide MSPileupTaskManager status API.

        :return: status dictionary
        """
        summary = dict(PILEUP_REPORT)
        summary.update({'thread_id': current_thread().name})
        summary.update({'tasks': self.msg.getReport())
        return summary

    def executeCycle(self):
        """
        execute MSPileupTasks polling cycle
        """
        # get pileup sizes and update them in DB
        spec = {}
        docs = self.dataManager.getPileup(spec)
        rucioToken = getRucioToken(self.rucioAuthUrl, self.rucioAccount)
        containers = [r['pileupName'] for r in docs]
        datasetSizes = getPileupContainerSizesRucio(containers, self.rucioUrl, rucioToken)
        for doc in docs:
            pileupSize = datasetSizes.get(doc['pileupName'], 0)
            doc['pileupSize'] = pileupSize
            self.dataManager.updatePileup(doc)

        # fetch all rse quotas
        self.rseQuotas.fetchStorageUsage(self.rucioClient)
        nodeUsage = self.rseQuotas.getNodeUsage()

        # execute all tasks
        self.mgr.monitoringTask()
        self.mgr.activeTask(nodeUsage=nodeUsage, marginSpace=self.marginSpace)
        self.mgr.inactiveTask()
