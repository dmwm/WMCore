from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator

class RequestManager(object):
    
    def __init__(self):
        self.specGenerator = WMSpecGenerator()
        self.count = 0
    
    def getAssignment(self, teamName=None, request=None):
        specName = "FakeProductionSpec_" + self.count
        spec, specUrl =self.specGenerator.createProductionSpec(specName)
        self.count += 1
        return {specName:specUrl}
    
    def postAssignment(self, requestName, prodAgentUrl=None):
        # do not thing or return success of fail massage 
        return 