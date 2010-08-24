#!/usr/bin/env python
"""
    WorkQueue tests
"""




import unittest
import os
import shutil
import tempfile

from Samples.TestReRecoWorkload import rerecoWorkload as TestReRecoWorkload
from Samples.BasicProductionWorkload import createWorkload as BasicProductionWorkload
from Samples.BasicProcessingWorkload import createWorkload as BasicProcessingWorkload

#from Samples.MultiTaskProcessingWorkload import createWorkload as MultiTaskProcessingWorkload
#from Samples.MultiTaskProductionWorkload import createWorkload as MultiTaskProductionWorkload

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
    
    def _selectReturnType(self, spec, returnType):
        
        if returnType == "file":
            specUrl = self._save(spec)
            return specUrl
        elif returnType == "spec":
            return spec
        else:
            specUrl = self._save(spec)
            return spec, specUrl

    def createProductionSpec(self, specName, returnType="spec"):
        spec = BasicProductionWorkload(specName)
        return self._selectReturnType(spec, returnType)    
    
    def createProcessingSpec(self, specName, returnType="spec"):
        #spec = BasicProcessingWorkload(specName)
        spec = BasicProcessingWorkload(specName)
        return self._selectReturnType(spec, returnType)    
    
    def createReRecoSpec(self, specName, returnType="spec"):
        spec =  TestReRecoWorkload(specName) 
        return self._selectReturnType(spec, returnType)    
    
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
