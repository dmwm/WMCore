#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to produce a json file to mock Rucio data
"""
import json
import os
import sys


from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.WMBase import getTestBase


### Here goes a list of data that we want to fetch from Rucio and
### persist in our json file to mock those calls/data
CONTAINERS = ["/MinimumBias/ComissioningHI-v1/RAW",
              "/Cosmics/ComissioningHI-PromptReco-v1/RECO",
              "/GammaGammaToEE_Elastic_Pt15_8TeV-lpair/Summer12-START53_V7C-v1/GEN-SIM"]
BLOCKS = []

### The output file which will contain all the Rucio mock data
OUTPUT_FILE = "RucioMockData.json"

### some default constants (which will probably not change in a long time)
SCOPE = "cms"
RUCIO_URL = 'http://cms-rucio.cern.ch'
RUCIO_AUTH_URL = 'https://cms-rucio-auth.cern.ch'
RUCIO_ACCT = "wmcore_transferor"


def main():
    finalResult = {}
    rucio = Rucio(RUCIO_ACCT,
                  hostUrl=RUCIO_URL,
                  authUrl=RUCIO_AUTH_URL,
                  configDict={"auth_type": "x509"})
    realRucio = rucio.cli
    outFilename = os.path.join(getTestBase(), '..', 'data', 'Mock', OUTPUT_FILE)

    calls = []
    for container in CONTAINERS:
        print("Building call list for container: {}".format(container))
        calls.append(['getBlocksInContainer', {'container': container}])
        calls.append(['getBlocksInContainer', {'container': container, 'scope': SCOPE}])
        calls.append(['isContainer', {'didName': container}])
        calls.append(['getDID', {'didName': container, 'dynamic': False}])
        calls.append(['didExist', {'didName': container}])
        blocks = realRucio.list_content(scope=SCOPE, name=container)
        for block in blocks:
            if block['type'].upper() != 'DATASET':
                continue
            calls.append(['isContainer', {'didName': block['name']}])
            calls.append(['getDID', {'didName': block['name'], 'dynamic': False}])
            calls.append(['didExist', {'didName': block['name']}])
        for block in BLOCKS:
            calls.append(['isContainer', {'didName': block}])
            calls.append(['getDID', {'didName': block, 'dynamic': False}])
            calls.append(['didExist', {'didName': block}])

    nCalls = len(calls)
    print("Need to issue {} calls to Rucio".format(nCalls))
    callsDone = 0
    for call in calls:
        func = getattr(rucio, call[0])
        if len(call) > 1:
            signature = '%s:%s' % (call[0], sorted(call[1].items()))
            result = func(**call[1])
        else:
            result = func()
            signature = call[0]
        callsDone += 1
        if callsDone % 10 == 0:
            print("Performed {} out of {} calls".format(callsDone, nCalls))
        finalResult.update({signature: result})

    print("Done!\n\nWriting out {} file".format(outFilename))
    with open(outFilename, 'w') as fileObj:
        json.dump(finalResult, fileObj, indent=1, separators=(',', ': '), sort_keys=True)


if __name__ == '__main__':
    sys.exit(main())
