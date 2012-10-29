#!/usr/bin/env python
"""
_Code_


Component that can parse a cvs log
and generate a file for checking the
code style.
"""





import commands
import os
import sys


class Code:
    """
    _Code_


    Component that can parse a cvs log
    and generate a file for checking the
    code style.
    """

    def __init__(self, script, report, baseDir, threshold, packages = {}):
        # location of the script to calculate the quality
        self.script = script
        # where the intermediate report should be written
        self.report = report
        # mapping from packages to persons responsible (dict.)
        self.packages = packages
        # basedir  (e.g.: /home/fvlingen/WMCORE), so we can fine
        # files in ../WMCORE/test/pyhon/... and ../WMCORE/src/python
        self.baseDir = baseDir
        # threshold under which a report should be filed.
        self.threshold = threshold
        # author to file/rate mappings for which a report will be filed.
        self.lowQuality = {}


    def run(self):
        """
        Runs the test script over the specified packages and records
        anomalies.
        """

        print 'Quality script: '+ self.script
        print 'Report file:    '+ self.report
        print 'Base dir:       '+ self.baseDir

        cont = raw_input('Are these values correct? ' + \
            'Press "A" to abbort or any other key to proceed ')
        if cont == 'A':
            sys.exit(0)

        for packageDir in self.packages.keys():
            localPath = os.path.join(self.baseDir, packageDir)
            # execute the quality script which produces a codeQuality.txt file
            command = self.script+' '+localPath
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))
            # parse the code quality file for the rating:
            reportFile = open(self.report, 'r')
            repNl = reportFile.readline()
            while repNl:
                if repNl.find('Your code has been rated at') == 0:
                    relRating = repNl.split(' ')[6]
                    absRating = float(relRating.split('/')[0])
                    if absRating < self.threshold:
                        fileRating = (str(absRating), packageDir)
                        authors = self.packages[packageDir]
                        if not self.lowQuality.has_key(authors):
                            self.lowQuality[self.packages[packageDir]] = []
                            # add the low rating
                        self.lowQuality[authors].append(fileRating)
                        break
                repNl = reportFile.readline()
            reportFile.close()

    def parseCVS(self, cvsLog, pathCut, moduleCut, maxVotes):
        """
        Parses a cvs log to information to generate a style quality
        test.
        """
        # pathCuts the path from e.g.
        # cvmsserver/repositories/CMSSW/WMCore/src/python/WMCore)
        # to src/python/WMCore

        # moduleCut ensures that non relevant modules are not incorporated. e.g.
        # src/python/WMCore becomes WMCore

        # maxVotes: maximum number of authors for voting.

        # modules used for testing
        self.module = {}

        logFile = open(cvsLog, 'r')
        nl = logFile.readline()
        state = 'file'
        # reset the vote
        vote = 0
        # a list on who we should vote
        vote2 = []
        while nl:
            # locate the cvs files
            if nl.find('RCS file:') == 0:
                # reset our vote structure
                vote = 0
                vote2 = []
                # filter the file path:
                path = nl.split(' ')[2].split(',')[0]
                # check if it is .py file
                if path.endswith('.py'):
                    # split it and start building modules
                    parts = path.split('/')
                    moduleName = ''
                    # do not include the actual file for style testing
                    # (only modules)
                    for index in xrange(0, len(parts)-1):
                        # we cut of part of the path
                        if index > pathCut:
                            moduleName = os.path.join(moduleName, parts[index])
                            if not self.module.has_key(moduleName) and \
                                index > pathCut + moduleCut:
                                self.module[moduleName] = {}
                                vote2.append(moduleName)
                    # now we need to find authors and let them vote.
                    state = 'authors'
                    # reset the vote
                    vote = 0
            if nl.find('date:') == 0 and state == 'authors':
                author = nl.split(' ')[6].split(';')[0]
                # start voting:
                for moduleName in vote2:
                    if not self.module[moduleName].has_key(author):
                        self.module[moduleName][author] = 0
                    self.module[moduleName][author] += 1
                # we voted
                vote += 1
                # if we reach maxVotes where done
                if vote > maxVotes:
                    state = 'file'
                    vote = 0
                    vote2 = []
            nl = logFile.readline()
        # we are done voting

    def preProcess(self):
        """
        Does some preprocessinc on information.
        If submodules of a module are the responsbility of one developer
        we aggregrate them in our style check.
        """

        for moduleName in self.module.keys():
            # find the one with the most votes per module:
            votes = 0
            winner = ''
            for voter in self.module[moduleName].keys():
                if self.module[moduleName][voter] > votes:
                    votes = self.module[moduleName][voter]
                    winner = voter
            self.module[moduleName] = winner

        # quick and dirty algorithm O(n^2). Can be done in O(n*lg(n))
        moduleLength = {}
        # find module lengths first
        for moduleName in self.module.keys():
            parts = moduleName.split('/')
            if not moduleLength.has_key(len(parts)):
                moduleLength[len(parts)] = []
            moduleLength[len(parts)].append(moduleName)
        lengths = moduleLength.keys()
        lengths.sort(reverse = True)

        for length in lengths:
            # FIXME: needs to be configurable.
            if length > 2:
                parents = {}
                for moduleName in self.module.keys():
                    parts = moduleName.split('/')
                    # group all parts of same length.
                    if len(parts) == length:
                        parent = moduleName.rsplit('/',1)[0]
                        if not parents.has_key(parent):
                            parents[parent] = []
                        parents[parent].append([moduleName, self.module[moduleName]])
                # check if all the children have the same developer as parent. If so remove the children.
                for parent in parents.keys():
                    same = True
                    parentDeveloper = self.module[parent]
                    for moduleName, developer in parents[parent]:
                        if developer != parentDeveloper:
                            same = False
                    if same:
                        for moduleName, developer in parents[parent]:
                            del self.module[moduleName]


    def generate(self, fileName):
        """
        Generates a python file that uses the result of parsing
        and this class to generate a script that checks to code quality.
        """
        self.preProcess()
        styleFile = open(fileName, 'w')
        # write head part
        head = """#!/usr/bin/env python

import os

from WMQuality.Code import Code

# output of the log files
# prefix of the files in cvs
# quality script for using pylint:
qualityScript = '%s'
# output file:
qualityReport = '%s'
# rating threshold (min: 0, max 10)
threshold = %s

packages = {\\
        """ % (self.script, self.report, self.threshold)
        styleFile.writelines(head)
        styleFile.writelines('\n')

        for moduleName in self.module.keys():
            # find the one with the most votes per module:
            # register this.
            styleFile.writelines("        '"+moduleName+"':'"+self.module[moduleName]+"',\\\n")
        styleFile.writelines('}\n')
        tail = """
code = Code(qualityScript, qualityReport, WMCore.WMInit.getWMBASE(), threshold, packages)
code.run()
code.summaryText()
        """
        styleFile.writelines(tail)
        styleFile.close()


    def summaryText(self):
        """
        Prints a summary of the run
        """

        print('\nReport Summary:\n')
        for author in self.lowQuality.keys():
            if len(self.lowQuality[author]) > 0:
                print('Author: '+author)
                print('---------------------')
                # do some sorting for readability
                files = []
                file2rating = {}
                for fileRating in self.lowQuality[author]:
                    files.append(fileRating[1])
                    file2rating[fileRating[1]] = fileRating[0]
                files.sort()
                for fileRating in files:
                    print(file2rating[fileRating]+' :: '+fileRating)
                print('\n\n')
