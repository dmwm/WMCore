#!/usr/bin/env python
"""
<testcase classname="WMCore_t.WMSpec_t.StdSpecs_t.MonteCarlo_t.MonteCarloTest" name="WMCore_t.WMSpec_t.StdSpecs_t.MonteCarlo_t.MonteCarloTest.testRelValMCWithPileup" time="0"><error type="DBSAPI.dbsApiException.DbsBadRequest">Traceback (most recent call last):
  File &quot;/jenkins/deploy/0.8.35/sw/slc5_amd64_gcc461/external/python/2.6.4-comp3/lib/python2.6/unittest.py&quot;, line 279, in run
    testMethod()
</error>

Looks at an xml file with the previous format and makes sure that nothing is failing that isn't in standards/allowed_failing_tests.txt
"""
from __future__ import print_function

import sys

testfile = sys.argv[1]
whitelist = sys.argv[2]

from xml.dom.minidom import parse as parseXML

allowedErrors = []
fh = open(whitelist, 'r')
for line in fh.readlines():
    allowedErrors.append(line.strip())
fh.close()

document = parseXML(testfile)

suitePasses = True
# look at test suites
for suite in document.childNodes:
    # look at testcases
    for case in suite.childNodes:
        if case.localName == 'testcase':
            for child in case.childNodes:
                if child.localName == 'error':
                    if case.getAttribute('name') not in allowedErrors:
                        print("Got an unallowed error: %s" % case.getAttribute('name'))
                        suitePasses = False
                    else:
                        print("Got an allowed error (FIXME): %s" % case.getAttribute('name'))

if suitePasses:
    print("Suite is OK")
    sys.exit(0)
else:
    print("Suite FAILED")
    sys.exit(1)
