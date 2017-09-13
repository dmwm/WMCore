#!/usr/bin/env python
"""
_TestImpl_

Test harness for invoking an Implementation plugin

"""
from __future__ import print_function

import getopt
import sys

from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.StageOutError import StageOutError

valid = ['input-pfn=', 'protocol=', "impl=", "target-pfn=", "cleanup"]
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError as ex:
    print(str(ex))
    sys.exit(1)

inputPfn = None
targetPfn = None
implName = None
protocol = None
doCleanup = False

for opt, arg in opts:
    if opt == "--input-pfn":
        inputPfn = arg
    if opt == "--target-pfn":
        targetPfn = arg
    if opt == "--impl":
        implName = arg
    if opt == "--protocol":
        protocol = arg
    if opt == "--cleanup":
        doCleanup = True

if implName == None:
    msg = "Error: ImplName not provided, you need to provide the --impl option"
    print(msg)
    sys.exit(1)

if inputPfn == None:
    msg = "Error: Input PFN not provided: use the --input-pfn option"
    print(msg)
    sys.exit(1)

if targetPfn == None:
    msg = "Error: Target PFN not provided: use the --target-pfn option"
    print(msg)
    sys.exit(1)

try:
    implInstance = retrieveStageOutImpl(implName)
except StageOutError as ex:
    print("Error retrieving plugin from registry named: %s" % implName)
    print(str(ex))
    sys.exit(1)

print(" Invoking StageOutImpl: %s" % implName)
implInstance(protocol, inputPfn, targetPfn)

if doCleanup:
    #  //
    # // Cleanup implies that we invoke the implementations
    # //  removeFile method on the target PFN after the transfer
    print("Invoking %s.removeFile on %s" % (implName, targetPfn))
    implInstance.removeFile(targetPfn)
