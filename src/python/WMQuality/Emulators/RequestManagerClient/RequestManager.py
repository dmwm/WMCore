from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator

class RequestManager(object):
    
    def __init__(self, *args, **kwargs):
        print "Using RequestManager Emulator"
        self.specGenerator = WMSpecGenerator()
        self.count = 0
    
    def getAssignment(self, teamName=None, request=None):
        #specName = "FakeProductionSpec_%s" % self.count
        #specUrl =self.specGenerator.createProductionSpec(specName, "file")
        #specName = "FakeProcessingSpec_%s" % self.count
        #specUrl =self.specGenerator.createProcessingSpec(specName, "file")
        specName = "MinBiasProcessingSpec_Test_%s" % self.count
        specUrl =self.specGenerator.createReRecoSpec(specName, "file")
        
        self.count += 1
        return {specName:specUrl}
    
    def postAssignment(self, requestName, prodAgentUrl=None):
        # do not thing or return success of fail massage 
        return 