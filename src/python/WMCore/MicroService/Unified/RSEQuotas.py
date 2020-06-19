"""
Non thread-safe object which provides all the RSE/PNN information
required for automatic data placement.
It can also communicate with other data management tools, like
Detox, Rucio and PhEDEx.
"""
from __future__ import division, print_function
from future.utils import viewitems
from WMCore.MicroService.Unified.Common import getDetoxQuota, getMSLogger, gigaBytes, teraBytes


class RSEQuotas(object):
    """
    Class which represents a list of RSEs, their quota and
    their storage usage
    """

    def __init__(self, dataAcct, quotaFraction, useRucio, **kwargs):
        """
        Executes a basic setup, including proper logging.
        :param dataAcct: string with either the Rucio account or PhEDEx group name
        :param quotaFraction: float point number representing the fraction of the quota
        :param useRucio: boolean flag used to decide between Rucio and PhEDEx data management
        :param kwargs: the supported keyword arguments are:
          minimumThreshold: integer value defining the minimum available space required
          detoxUrl: string with the detox url (to fetch the quota)
          verbose: logger verbosity
          logger: logger object
        """
        self.dataAcct = dataAcct
        self.quotaFraction = quotaFraction
        self.useRucio = useRucio

        self.minimumSpace = kwargs["minimumThreshold"]
        self.detoxUrl = kwargs.get("detoxUrl", "")
        self.logger = getMSLogger(kwargs.get("verbose"), kwargs.get("logger"))
        msg = "RSEQuotas started with parameters: dataAcct=%s, quotaFraction=%s, "
        msg += "minimumThreshold=%s GB, useRucio=%s"
        self.logger.info(msg, dataAcct, quotaFraction, gigaBytes(self.minimumSpace), self.useRucio)

        self.nodeUsage = {}
        self.availableRSEs = set()
        self.outOfSpaceNodes = set()

    def __str__(self):
        """
        Write out useful information for this object
        :return: a stringified dictionary
        """
        res = {'detoxUrl': self.detoxUrl, 'dataAcct': self.dataAcct,
               'useRucio': self.useRucio, 'quotaFraction': self.quotaFraction,
               'minimumSpace': self.minimumSpace}
        return str(res)

    def getNodeUsage(self):
        """
        Return a dictionary of RSEs and a few storage statistics
        """
        return self.nodeUsage

    def getAvailableRSEs(self):
        """
        Return a list of out-of-space RSE/PNNs
        """
        return self.availableRSEs

    def getOutOfSpaceRSEs(self):
        """
        Return a list of out-of-space RSE/PNNs
        """
        return self.outOfSpaceNodes

    def fetchStorageQuota(self, dataSvcObj):
        """
        Fetch the DataOps quota from Detox. At this stage, we do not do
        any manipulation with the quota value (Unified uses 80% of the quota),
        use it as is!
        :param dataSvcObj: object instance for the Rucio data service

        :return: create an instance cache structure to keep track of quota
          and available storage. The structure is as follows:
          {"pnn_name": {"quota": quota in bytes for the rucio account or phedex group,
                        "bytes_limit": total space for the account/group,
                        "bytes": amount of bytes currently used/archived,
                        "bytes_remaining": space remaining for the acct/group,
                        "quota_avail": a fraction of the quota that we will use}

        NOTE: code extracted/modified from Unified, see `fetch_detox_info` in
          https://github.com/CMSCompOps/WmAgentScripts/blob/master/utils.py#L2514
        """
        # FIXME: besides the 1-line below to clear the data structure, this method
        # will be useless once we migrate to Rucio
        self.nodeUsage.clear()
        if self.useRucio:
            response = dataSvcObj.getAccountLimits(self.dataAcct)
            for rse, quota in viewitems(response):
                if rse.endswith("_Tape") or rse.endswith("_Export"):
                    continue
                self.nodeUsage.setdefault(rse, {})
                self.nodeUsage[rse] = dict(quota=int(quota),
                                           bytes_limit=int(quota),
                                           bytes=0,
                                           bytes_remaining=int(quota),  # FIXME: always 0
                                           quota_avail=0)
            self.logger.info("Storage quota filled from Rucio")
        else:
            # FIXME: extremely fragile code that has to be replaced by a proper
            # CRIC/Rucio API in the very near future
            info = getDetoxQuota(self.detoxUrl)

            doRead = False
            for line in info:
                if 'DDM Partition:' in line and self.dataAcct in line:
                    doRead = True
                    continue
                elif 'DDM Partition:' in line:
                    doRead = False
                    continue
                elif line.startswith('#'):
                    continue

                if not doRead:
                    continue

                _, quota, _, _, pnn = line.split()

                if pnn.endswith("_MSS") or pnn.endswith("_Export"):
                    continue
                self.nodeUsage.setdefault(pnn, {})
                # convert from TB to bytes
                self.nodeUsage[pnn] = dict(quota=int(quota) * (1000 ** 4),
                                           bytes_limit=0,
                                           bytes=0,
                                           bytes_remaining=0,
                                           quota_avail=0)
            self.logger.info("Storage quota filled from Detox information")

    def fetchStorageUsage(self, dataSvcObj):
        """
        Fetch the storage usage from either Rucio or PhEDEx, which will then
        be used as part of the data placement mechanism.
        Also calculate the available quota - given the configurable quota
        fraction - and mark RSEs with less than 1TB available as NOT usable.
        :param dataSvcObj: object instance for the data service

        Keys definition is:
         * quota: the PhEDEx group quota provided by Detox
         * bytes_limit: either the PhEDEx quota or the account quota from Rucio
         * bytes: data volume placed by Rucio (or subscribed in PhEDEx)
         * bytes_remaining: storage available for our account/group
         * quota_avail: space left (in bytes) that we can use for data placement
        :return: update our cache in place with up-to-date values, in the format of:
            {"pnn_name": {"bytes_limit": total space for the account/group,
                          "bytes": amount of bytes currently used/archived,
                          "bytes_remaining": space remaining for the acct/group}
        """
        if self.useRucio:
            self.logger.debug("Using Rucio for storage usage, with acct: %s", self.dataAcct)
            for item in dataSvcObj.getAccountUsage(self.dataAcct):
                if item['rse'] not in self.nodeUsage:
                    self.logger.warning("Rucio RSE: %s has data usage but no quota available.", item['rse'])
                    continue
                # bytes_limit is always 0, so skip it and use whatever came from the limits call
                # bytes_remaining is always negative, so calculate it based on the limits
                quota = self.nodeUsage[item['rse']]['quota']
                self.nodeUsage[item['rse']].update({'bytes': item['bytes'],
                                                    'bytes_remaining': quota - item['bytes']})
        else:
            self.logger.debug("Using PhEDEx for storage usage, with acct: %s", self.dataAcct)
            # for PhEDEx, we have also to remap the key's to keep in sync with Rucio
            res = dataSvcObj.getGroupUsage(group=self.dataAcct)
            for item in res['phedex']['node']:
                if item['name'] not in self.nodeUsage:
                    continue
                quota = self.nodeUsage[item['name']]['quota']
                self.nodeUsage[item['name']].update({'bytes_limit': quota,
                                                     'bytes': item['group'][0]['dest_bytes'],
                                                     'bytes_remaining': quota - item['group'][0]['dest_bytes']})

    def evaluateQuotaExceeded(self):
        """
        Goes through every single site, their quota and their remaining
        storage; and mark those with less than X TB available (1TB at the
        moment) as not eligible to receive data
        :return: updates instance structures in place
        """
        self.availableRSEs.clear()
        self.outOfSpaceNodes.clear()
        # given a configurable sub-fraction of our quota, recalculate how much storage is left
        for rse, info in self.nodeUsage.items():
            quotaAvail = info['quota'] * self.quotaFraction
            info['quota_avail'] = min(quotaAvail, info['bytes_remaining'])
            if info['quota_avail'] < self.minimumSpace:
                self.outOfSpaceNodes.add(rse)
            else:
                self.availableRSEs.add(rse)
        self.logger.info("Currently %d nodes are out of space.", len(self.outOfSpaceNodes))

    def printQuotaSummary(self):
        """
        Print a summary of the current quotas, space usage and space available
        """
        self.logger.info("Summary of the current quotas in Terabytes:")
        for node in sorted(self.nodeUsage.keys()):
            msg = "  %s:\t\tbytes_limit: %.2f, bytes_used: %.2f, bytes_remaining: %.2f, "
            msg += "quota: %.2f, quota_avail: %.2f"
            self.logger.debug(msg, node, teraBytes(self.nodeUsage[node]['bytes_limit']),
                              teraBytes(self.nodeUsage[node]['bytes']),
                              teraBytes(self.nodeUsage[node]['bytes_remaining']),
                              teraBytes(self.nodeUsage[node]['quota']),
                              teraBytes(self.nodeUsage[node]['quota_avail']))
        self.logger.info("List of RSE's out of quota: %s", self.outOfSpaceNodes)

    def updateNodeUsage(self, node, dataSize):
        """
        Provided a RSE/PNN name and the data size, in bytes, update the node
        storage usage by subtracting it from the current available quota.
        If it gets a list of nodes, the same dataSize is accounted for all
        of them.
        :param node: string with the PNN/RSE
        :param dataSize: integer with the amount of bytes allocated
        :return: nothing. updates nodeUsage cache
        """
        if isinstance(node, basestring):
            node = [node]
        if not isinstance(dataSize, int):
            self.logger.error("dataSize needs to be integer, not '%s'!", type(dataSize))
        for rse in node:
            self.nodeUsage[rse]['quota_avail'] -= dataSize
