#!/usr/bin/env python
"""
_Test_


Component that can parse a cvs log
and generate a file for generating test that map
to developers responsible for the test.
"""

__revision__ = "$Id: Test.py,v 1.4 2008/10/16 15:03:19 fvlingen Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "fvlingen@caltech.edu"

import os
import unittest

class Test:
    """
    _Test_

    Component that can parse a cvs log
    and generate a file for generating test that map
    to developers responsible for the test.
    """

    def __init__(self, tests = []):
        self.tests = tests


    def run(self):
        testSuite = unittest.TestSuite()
        for test in self.tests:
            test[0].developer = test[1]
            testSuite.addTest(test[0])
        testResult = unittest.TestResult()
        self.testResult = testSuite.run(testResult)

    def parseCVS(self, cvsLog, pathCut, moduleCut, maxVotes):
        """
        Parses a cvs log to information to generate a style quality
        test.
        """
        self.cvsLog = cvsLog
        # pathCut cuts the path from 
        # e.g. cvmsserver/repositories/CMSSW/WMCore/src/python/WMCore)
        # to src/python/WMCore

        # ensures that non relevant modules are not incorporated. e.g.
        # src/python/WMCore becomes WMCore
        self.moduleCut = moduleCut
        # maxVotes: maximum number of authors for voting.

        # files used for testing.
        self.testFile = {}

        logFile = open(self.cvsLog, 'r')
        nl = logFile.readline()
        state = 'file'
        # reset the vote
        vote = 0
        # a list on who we should vote
        curFile = ''
        while nl:
            # locate the cvs files
            if nl.find('RCS file:') == 0:
                # reset our vote structure
                vote = 0
                # filter the file path:
                path = nl.split(' ')[2].split(',')[0]
                # check if it is .py file
                if path.endswith('_t.py'):
                    # split it and start building modules
                    parts = path.split('/')
                    moduleName = ''
                    # do not include the actual file for style testing
                    # (only modules)
                    for index in xrange(0, len(parts)):
                        # we cut of part of the path
                        if index > pathCut:
                            moduleName = os.path.join(moduleName, parts[index])
                            if not self.testFile.has_key(moduleName) and \
                                index > pathCut + self.moduleCut and\
                                index == (len(parts)-1):
                                self.testFile[moduleName] = {}
                                curFile = moduleName
                    # now we need to find authors and let them vote.
                    state = 'authors'
                    # reset the vote
                vote = 0
            if nl.find('date:') == 0 and state == 'authors':
                author = nl.split(' ')[6].split(';')[0]
                # start voting:
                if not self.testFile[curFile].has_key(author):
                    self.testFile[curFile][author] = 0
                self.testFile[curFile][author] += 1
                # we voted
                vote += 1
                # if we reach maxVotes where done
                if vote < maxVotes:
                    state = 'file'
                    vote = 0
            nl = logFile.readline()
        # we are done voting

    def missingTests(self, filename):
        """
        Parses the cvs log and finds what modules have missing
        tests.
        """
        pass

    def generate(self, filename):
        """
        Generates a python file that uses the result of parsing
        and this class to generate a script that does suite tests.
        """
        testsFile = open(filename, 'w')
        head = """#!/usr/bin/env python

from WMQuality.Test import Test

        """
        testsFile.writelines(head)
        testsFile.writelines('\n')
        winners = {}
        # make the import statements
        for testFile in self.testFile.keys():
            # find the one with the most votes per module:
            votes = 0
            winner = ''
            for voter in self.testFile[testFile].keys():
                if self.testFile[testFile][voter] > votes:
                    votes = self.testFile[testFile][voter]
                    winner = voter
            # make the import:
            parts = testFile.split('/')
            importStmt = 'from '
            for part in xrange(0, len(parts)-1):
                if part > self.moduleCut:
                    importStmt += parts[part]+"."
            importStmt += parts[-1].split('.')[0]
            importStmt += ' import '
            testObject = parts[-1].split('_t.py')[0] + 'Test'
            importStmt += testObject
            winners[testObject] = winner
            testsFile.writelines(importStmt+'\n')

        testsFile.writelines('\n\n')
        testsFile.writelines('tests = [\\\n')
        # make the object instantiations.
        for testObject in winners:
            testsFile.writelines('     (' +\
                testObject+"(),'"+winners[testObject]+"'),\\\n")
        testsFile.writelines('    ]\n')
        tail = """
test = Test(tests)
test.run()
test.summaryText()
        """
        testsFile.writelines(tail)
        testsFile.close()
 
 
    def summaryText(self):
        """
        Summary for the tests result.
        """
        print "********************REPORT********************"
        print "*******************FAILURES*******************"
    
        for i in self.testResult.failures:
            obj, msg= i
            print('==============================')
            print(obj.developer+'--->'+obj.__class__.__name__)
            print('==============================')
            print(str(msg))
    
    
        print "*******************ERRORS********************"
    
        for i in self.testResult.errors:
            obj,msg=i
            print('==============================')
            print(obj.developer+'--->'+obj.__class__.__name__)
            print('==============================')
            print(str(msg))
 
        print "*******************SUMMARY*********************"
        print "Number of tests run: "+str(self.testResult.testsRun)
        print "Number of failures:  "+str(len(self.testResult.failures))
        print "Number of errors  :  "+str(len(self.testResult.errors))

