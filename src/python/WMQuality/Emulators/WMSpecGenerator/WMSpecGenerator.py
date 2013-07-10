#!/usr/bin/env python
"""
    WorkQueue tests
"""
import os
import shutil
import tempfile

from WMCore.WMSpec.StdSpecs.ReReco  import ReRecoWorkloadFactory
from Samples.TestMonteCarloWorkload \
    import monteCarloWorkload as TestMCWorkload, getMCArgs
from Samples.BasicProductionWorkload \
    import createWorkload as BasicProductionWorkload
from Samples.BasicProcessingWorkload \
    import createWorkload as BasicProcessingWorkload
from WMCore.Cache.WMConfigCache import ConfigCache

#from Samples.MultiTaskProcessingWorkload import createWorkload as MultiTaskProcessingWorkload
#from Samples.MultiTaskProductionWorkload import createWorkload as MultiTaskProductionWorkload
mcArgs = getMCArgs()

class WMSpecGenerator(object):

    def __init__(self, dirLocation=None):
        if not dirLocation:
            dirLocation = tempfile.mkdtemp()
        if not os.path.exists(dirLocation):
            os.makedirs(dirLocation)
        self.dir = dirLocation

    def _save(self, spec):
        specFile =  os.path.join(self.dir, spec.name() + ".spec")
        # Basic production Spec
        spec.setSpecUrl(specFile)
        # save create pickled file
        spec.save(spec.specUrl())

        return specFile

    def _selectReturnType(self, spec, returnType, splitter = None):
        if splitter != None:
            spec.setStartPolicy(splitter)
        if returnType == "file":
            specUrl = self._save(spec)
            return specUrl
        elif returnType == "spec":
            return spec
        else:
            specUrl = self._save(spec)
            return spec, specUrl

    def createProductionSpec(self, specName, returnType="spec", splitter = None):
        spec = BasicProductionWorkload(specName)
        return self._selectReturnType(spec, returnType, splitter)

    def createProcessingSpec(self, specName, returnType="spec", splitter = None):
        #spec = BasicProcessingWorkload(specName)
        spec = BasicProcessingWorkload(specName)
        return self._selectReturnType(spec, returnType, splitter)

    def createReRecoSpec(self, specName, returnType="spec", splitter = None,
                         inputDataset = None, dbsUrl = None, **additionalArgs):
        # update args, then reset them
        args = ReRecoWorkloadFactory.getTestArguments()
        args.update(additionalArgs)
        args["ConfigCacheID"] = createConfig(args["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        spec =  factory.factoryWorkloadConstruction(specName, args)
        if inputDataset != None:
            spec.taskIterator().next().data.input.dataset.primary = inputDataset
        if dbsUrl != None:
            print dbsUrl
            spec.taskIterator().next().data.input.dataset.dbsurl = dbsUrl
        return self._selectReturnType(spec, returnType, splitter)

    def createMCSpec(self, specName, returnType="spec", splitter = None):
        spec =  TestMCWorkload(specName, mcArgs)
        return self._selectReturnType(spec, returnType, splitter)

    def createRandomProductionSpecs(self, size=10):
        specNameBase = "FakeSpec"
        specs = {}
        for i in range(size):
            specName = specNameBase + "_%s" % i
            sepc, specUrl = self.createProductionSpec(specName)
            specs[specName] = specUrl
        return specs

    def removeSpecs(self):
        #TODO: needs a smarter way to clean up the spec files.
        # avoid accidently deleting other codes
        shutil.rmtree(self.dir)

def createConfig(couchDBName):
    """
    _createConfig_

    Create a config of some sort that we can load out of ConfigCache
    """
    
    PSetTweak = {'process': {'outputModules_': ['RECOoutput', 'ALCARECOoutput'],
                             'RECOoutput': {'dataset': {'dataTier': 'RECO',
                                                         'filterName': 'Filter'}},
                             'ALCARECOoutput': {'dataset': {'dataTier': 'ALCARECO',
                                                            'filterName': 'AlcaFilter'}}}}

    configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = couchDBName)
    configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
    configCache.setPSetTweaks(PSetTweak = PSetTweak)
    configCache.save()

    return configCache.getCouchID()

