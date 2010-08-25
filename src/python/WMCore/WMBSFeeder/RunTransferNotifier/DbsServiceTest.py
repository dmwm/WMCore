#!/usr/bin/env python

from DbsQueryHelper import DbsQueryHelper

qh = DbsQueryHelper("cmsweb.cern.ch/dbs_discovery/", 443, "cms_dbs_prod_global")
#qh = DbsQueryHelper("cmsweb.cern.ch/dbs_discovery_test/", 443, "cms_dbs_prod_global")

(files, blocks, fileMap) = qh.getFileInfo(67818,"/Cosmics/Commissioning08-PromptReco-v2/RECO")

print "DIRECT TEST"

# Test block parentage
pds = qh.getParentDataset("/Cosmics/Commissioning08-PromptReco-v2/RECO")
print pds
parentBlocks = qh.getBlockInfo(67818, pds[0])
print parentBlocks
print len(parentBlocks)
