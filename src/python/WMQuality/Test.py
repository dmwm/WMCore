#!/usr/bin/env python
"""
_Test_


Component that can parse a cvs log
and generate a file for generating test that map
to developers responsible for the test.
"""

__revision__ = "$Id: Test.py,v 1.9 2009/02/11 09:21:01 fvlingen Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "fvlingen@caltech.edu"

import commands
import os
import unittest

class Test:
    """
    _Test_

    Component that can parse a cvs log
    and generate a file for generating test that map
    to developers responsible for the test.
    """

    def __init__(self, tests = [], logFile = 'failures3.log'):
        self.tests = tests
        self.failures = {}
        self.errors = {}
        self.totalTests = 0
        self.totalErrors = 0
        self.totalFailures = 0
        self.logFile = logFile
        # files used for testing.
        self.testFile = {}
        self.cvsLog = None 
        self.moduleCut = None 


    def run(self):
        """
        Run over all the tests added to this suite.
        """

        for test in self.tests:
            print('*********************Test: '+test[0].__class__.__name__+ ' Developer: '+test[1])
            try:
                testSuite = unittest.TestSuite()
                test[0].developer = test[1]
                testSuite.addTest(test[0])
                testResult = unittest.TestResult()
                self.testResult = testSuite.run(testResult)
                self.summarizeTest()
            except Exception, ex:
                customReport = {}
                customReport['developer'] = test[1]
                customReport['class'] = test[0].__class__.__name__ 
                msg = """
Test framework error! Did you use the proper test classes? """
                customReport['msg'] = msg + ' '+str(ex)
                self.summarizeTest(customReport)
            # call the script we use for cleaning the backends:
            # FIXME: need to add something for oracle too.
            print('Cleaning database backends')
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))


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


        logFile = open(self.cvsLog, 'r')
        nl = logFile.readline()
        state = 'file'
        # reset the vote
        vote = 0
        # a list on who we should vote
        curFile = ''
        firstAuthor = True
        while nl:
            # locate the cvs files
            first = 0
            if nl.find('RCS file:') == 0:
                firstAuthor = True
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
                # if this is the first author, check if the file has not been removed.
                if firstAuthor: 
                    firstAuthor = False
                    modState = nl.split(' ')[9].split(';')[0] 
                    # if removed, remove it hear too.
                    if modState == 'dead' :
                        del self.testFile[curFile]
                        state = 'file'
                if state != 'file':
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
        # winners are successful imports
        winners = {}
        # losers are unsuccesful imports which are reported (level 1)
        losers = {}
        losersCum = {}
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
            # test if the import works. If it does not work we report it
            pythonCmd = "python -c '"+importStmt+"'"
            stdout, stdin, stderr = os.popen3(pythonCmd)
            errorLine = stderr.readline()
            # if no error register it
            if not errorLine:
                winners[testObject] = winner
                testsFile.writelines(importStmt+'\n')
            # if error report it
            else:
                errorMsg = errorLine
                while True:
                    errorLine = stderr.readline()
                    if not errorLine:
                        break
                    errorMsg += errorLine
                if not losers.has_key(winner):
                    losers[winner] = []
                    losersCum[winner] = 0
                losers[winner].append( [testFile, importStmt, errorMsg] )
                losersCum[winner] += 1
        # make the object instantiations.
        # it is done with try/except clauses to test instantiation (level 2)
        testsFile.writelines('\nerrors = {}\n')
        testsFile.writelines('tests = []\n')
        testsFile.writelines('\n\n')
        for testObject in winners:
            testsFile.writelines('try:\n')
            testsFile.writelines('   x='+testObject+'()\n')
            testsFile.writelines('   tests.append((x,"'+\
                winners[testObject]+'"))\n')
            testsFile.writelines('except Exception,ex:\n')
            testsFile.writelines('   if not errors.has_key("'+\
                winners[testObject]+'"):\n')
            testsFile.writelines('       errors["'+\
                winners[testObject]+'"] = []\n')
            testsFile.writelines('   errors["'+ \
                str(winners[testObject])+'"].append(("'+\
                str(testObject)+'",str(ex)))\n')
            testsFile.writelines('\n')
        tail = """

print('Writing level 2 failures to file: failures2.log ')
failures = open('failures2.log','w')

failures.writelines('Failed instantiation summary (level 2): \\n')
for author in errors.keys():
    failures.writelines('\\n*****Author: '+author+'********\\n')
    for errorInstance, errorMsg in  errors[author]:
        failures.writelines('Test: '+errorInstance)
        failures.writelines(errorMsg)
        failures.writelines('\\n\\n')
failures.close()



test = Test(tests,'failures3.log')
test.run()
test.summaryText()
        """
        testsFile.writelines(tail)
        testsFile.close()
        # we generated the test file, now generate the report of failed
        # imports.
        print('Writing level 1 failures to file: failures1.log ')
        failures = open('failures1.log','w')
        failures.writelines('Failed import summary (level 1):\n\n')
        for winner in losersCum.keys():
            msg = 'Author: '+winner
            msg += ' Failures: '+str(losersCum[winner])
            failures.writelines(msg+'\n')
        failures.writelines('\nFailed import details:\n\n')
        for winner in losers.keys():
            failures.writelines('************Author: '+winner+'***********\n\n')
            for failed in losers[winner]:
                failures.writelines('File: '+failed[0]+'\n\n')
                failures.writelines('Failed import: '+failed[1]+'\n\n')
                failures.writelines('Error message: \n'+failed[2]+'\n\n')              
 
 
    def summaryText(self):
        """
        Summary for the tests result.
        """
        print('Writing level 3 failures to file: '+self.logFile)
        failures = open(self.logFile,'w')
        failures.writelines('Following tests where run\n\n')
        for test in self.tests:
            failures.writelines(test[0].__class__.__name__+'-->'+test[1]+'\n')
        failures.writelines('\n\n') 
        failures.writelines('Failed tests (level 3):\n\n')    
        for author in self.failures.keys():
            failures.writelines(author+ \
                ':'+str(len(self.failures[author]))+' failures\n')
        for author in self.errors.keys():
            failures.writelines(author+ \
                ':'+str(len(self.errors[author]))+' errors\n')
        failures.writelines('Failures (level 3):\n\n')    
        for author in self.failures.keys():
            failures.writelines('Author: '+author+'\n\n')
            for failure in self.failures[author]:
                failures.writelines('Test: '+failure[0]+'\n\n')
                failures.writelines('Failure: '+failure[1]+'\n\n') 

        for author in self.errors.keys():
            failures.writelines('Author: '+author+'\n\n')
            for failure in self.errors[author]:
                failures.writelines('Test: '+failure[0]+'\n\n')
                failures.writelines('Error: '+failure[1]+'\n\n') 
        failures.close()

    def summarizeTest(self, customReport = {}):
        """
        Aggregates a summary of the test. If the test framework failed
        (e.g. the developer did not inherit from the test object, we need
        to send a custom report.
        """
        if customReport != {}:
            self.totalFailures += 1
            if not self.failures.has_key(customReport['developer']):
                self.failures[customReport['developer']] = []
            self.failures[customReport['developer']].append(\
                [customReport['class'], customReport['msg']])
        else: 
            for i in self.testResult.failures:
                obj, msg = i
                self.totalFailures += 1
                if not self.failures.has_key(obj.developer):
                    self.failures[obj.developer] = []
                self.failures[obj.developer].append([obj.__class__.__name__, \
                    msg])
    
            for i in self.testResult.errors:
                obj, msg = i
                self.totalErrors += 1
                if not self.errors.has_key(obj.developer):
                    self.errors[obj.developer] = []
                self.errors[obj.developer].append([obj.__class__.__name__, \
                    msg])
 

