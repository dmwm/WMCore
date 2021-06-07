from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMCore.ReqMgr.DataStructs.RequestStatus import check_allowed_transition

class ReqMgr(dict):

    def __init__(self, *args, **kwargs):
        """
        all the private valuable is defined for test values
        """
        self.specGenerator = WMSpecGenerator()
        self.count = 0
        self.maxWmSpec = kwargs.setdefault('numOfSpecs', 1)
        self.type = kwargs.setdefault("type", 'ReReco')
        if self.type != 'ReReco':
            raise TypeError('unknown request type %s' % self.type)
        self.splitter = kwargs.setdefault('splitter', 'DatasetBlock')
        self.inputDataset = kwargs.setdefault('inputDataset', None)
        self.dbsUrl = kwargs.setdefault('dbsUrl', None)
        self.status = {}
        self.progress = {}
        self.msg = {}
        self.names = []
        self.openRunningTimeout = kwargs.setdefault('openRunningTimeout', 0)
        import logging
        self['logger'] = logging

    def getAssignment(self, teamName=None, request=None):
        if self.count < self.maxWmSpec:
            if self.type == 'ReReco':
                specName = "ReRecoTest_v%sEmulator" % self.count
                specUrl =self.specGenerator.createReRecoSpec(specName, "file",
                                                             self.splitter,
                                                             InputDataset=self.inputDataset,
                                                             DbsUrl=self.dbsUrl,
                                                             OpenRunningTimeout = self.openRunningTimeout)
            self.names.append(specName)
            self.status[specName] = 'assigned'
            #specName = "FakeProductionSpec_%s" % self.count
            #specUrl =self.specGenerator.createProductionSpec(specName, "file")
            #specName = "FakeProcessingSpec_%s" % self.count
            #specUrl =self.specGenerator.createProcessingSpec(specName, "file")

            self.count += 1
            # returns list of list(rquest name, spec url)
            return [[specName, specUrl],]
        else:
            return []

    def getRunningOpen(self, teamName):
        """Returns a list of request names of requests in running-open state"""
        result = []
        for request in self.status:
            if self.status[request] == 'running-open':
                result.append(request)
        return result

    def getRequest(self, requestName):
        """Get request info"""
        if requestName not in self.names:
            raise RuntimeError("unknown request %s" % requestName)

        request = {'RequestName' : requestName,
                   'RequestStatus' : self.status[requestName],
                   'RequestPriority' : 100}
        if requestName in self.progress:
            request.update(self.progress[requestName])
        request.setdefault('percent_complete', 0)
        request.setdefault('percent_success', 0)
        return request

    def putWorkQueue(self, reqName, prodAgentUrl=None):
        self.status[reqName] = 'acquired'

    def reportRequestStatus(self, name, status):
        if not check_allowed_transition(self.status[name], status):
            raise RuntimeError("Invalid status move: %s" % status)
        self.status[name] = status

    def reportRequestProgress(self, name, **args):
        self.progress.setdefault(name, {})
        self.progress[name].update(args)

    def sendMessage(self, request, msg):
        self.msg[request] = msg

    def _removeSpecs(self):
        """
        This is just for clean up not part of emulated function
        """
        self.specGenerator.removeSpecs()

    def getTeam(self):
        """Give a fake team name"""
        return ["The A-Team", "some other bloke"]

    def getRequestByStatus(self, statusList, detail=True):
        # FIXME: this method needs to be fixed accordingly
        if statusList == 'staged':
            specName = "ReRecoTest_v%sEmulator" % self.count
            specUrl =self.specGenerator.createReRecoSpec(specName, "file",
                                                         self.splitter,
                                                         assignKwargs={'SiteWhitelist': ['T2_XX_SiteA']})
            self.names.append(specName)
            self.status[specName] = 'staged'
            #specName = "FakeProductionSpec_%s" % self.count
            #specUrl =self.specGenerator.createProductionSpec(specName, "file")
            #specName = "FakeProcessingSpec_%s" % self.count
            #specUrl =self.specGenerator.createProcessingSpec(specName, "file")

            self.count += 1
            # returns list of list(rquest name, spec url)
            return [[specName, specUrl],]
        else:
            return []