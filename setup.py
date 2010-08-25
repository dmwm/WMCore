#!/usr/bin/env python
from distutils.core import setup, Command
from unittest import TextTestRunner, TestLoader, TestSuite
from glob import glob
from os.path import splitext, basename, join as pjoin, walk
import os

class TestCommand(Command):
    user_options = [ ]

    def initialize_options(self):
        self._dir = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        '''
        Finds all the tests modules in tests/, and runs them.
        '''
        testfiles = [ ]
        # Walk the directory tree
        for dirpath, dirnames, filenames in os.walk('./test/python/WMCore_t'):
            # skipping CVS directories and their contents
            pathelements = dirpath.split('/')
            if not 'CVS' in pathelements:
                # to build up a list of file names which contain tests
                for file in filenames:
                    if file not in ['__init__.py']:
                        if file.endswith('_t.py'):
                            testmodpath = pathelements[3:]
                            testmodpath.append(file.replace('.py',''))
                            testfiles.append('.'.join(testmodpath))
                            
        testsuite = TestSuite()
        for test in testfiles:
            try:
                testsuite.addTest(TestLoader().loadTestsFromName(test))
            except Exception, e:
                print "Could not load %s test - fix it!\n %s" % (test, e)
        print "Running %s tests" % testsuite.countTestCases()
        
        t = TextTestRunner(verbosity = 1)
        t.run(testsuite)

class CleanCommand(Command):
    user_options = [ ]

    def initialize_options(self):
        self._clean_me = [ ]
        for root, dirs, files in os.walk('.'):
            for f in files:
                if f.endswith('.pyc'):
                    self._clean_me.append(pjoin(root, f))

    def finalize_options(self):
        pass

    def run(self):
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass
            
setup (name='wmcore',
       version='1.0',
       cmdclass = { 'test': TestCommand, 'clean': CleanCommand },
       package_dir={'WMCore': 'src/python/WMCore','WMComponent' : 'src/python/WMComponent','WMQuality' : 'src/python/WMQuality'},
       packages=['WMComponent.DBSBuffer.Handler',
                 'WMComponent.DBSBuffer.Database.SQLite.DBSBufferFiles',
                 'WMComponent.DBSBuffer.Database.SQLite',
                 'WMComponent.DBSBuffer.Database.Oracle.DBSBufferFiles',
                 'WMComponent.DBSBuffer.Database.Oracle',
                 'WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles',
                 'WMComponent.DBSBuffer.Database.MySQL',
                 'WMComponent.DBSBuffer.Database.Interface',
                 'WMComponent.DBSBuffer.Database',
                 'WMComponent.DBSBuffer',
                 'WMComponent.Proxy.Handler',
                 'WMComponent.Proxy',
                 'WMComponent.ErrorHandler.Database.MySQL',
                 'WMComponent.ErrorHandler.Database',
                 'WMComponent.ErrorHandler',
                 'WMComponent',
                 'WMQuality',
                 'WMCore.MsgService.Oracle',
                 'WMCore.MsgService.MySQL',
                 'WMCore.MsgService',
                 'WMCore.Trigger.Oracle',
                 'WMCore.Trigger.MySQL',
                 'WMCore.Trigger',
                 'WMCore.Alerts.MySQL',
                 'WMCore.Alerts',
                 'WMCore.Agent',
                 'WMCore.Algorithms',
                 'WMCore.JobSplitting',
                 'WMCore.JobStateMachine',                 
                 'WMCore.WMBS.SQLite.Jobs',
                 'WMCore.WMBS.SQLite.Masks',
                 'WMCore.WMBS.SQLite.Workflow',
                 'WMCore.WMBS.SQLite.JobGroup',
                 'WMCore.WMBS.SQLite.Fileset',
                 'WMCore.WMBS.SQLite.Locations',
                 'WMCore.WMBS.SQLite.Files',
                 'WMCore.WMBS.SQLite.Subscriptions',
                 'WMCore.WMBS.SQLite',
                 'WMCore.WMBS.Oracle.Jobs',
                 'WMCore.WMBS.Oracle.Masks',
                 'WMCore.WMBS.Oracle.Workflow',
                 'WMCore.WMBS.Oracle.JobGroup',
                 'WMCore.WMBS.Oracle.Fileset',
                 'WMCore.WMBS.Oracle.Locations',
                 'WMCore.WMBS.Oracle.Files',
                 'WMCore.WMBS.Oracle.Subscriptions',
                 'WMCore.WMBS.Oracle',                 
                 'WMCore.WMBS.Actions.Fileset',
                 'WMCore.WMBS.Actions.Files',
                 'WMCore.WMBS.Actions.Subscriptions',
                 'WMCore.WMBS.Actions',
                 'WMCore.WMBS.WMBSAccountant',
                 'WMCore.WMBS.Oracle',
                 'WMCore.WMBS.WMBSAllocater.Allocaters',
                 'WMCore.WMBS.WMBSAllocater',
                 'WMCore.WMBS.WMBSFeeder.Feeders',
                 'WMCore.WMBS.WMBSFeeder',
                 'WMCore.WMBS.T0AST',
                 'WMCore.WMBS.MySQL.Jobs',
                 'WMCore.WMBS.MySQL.Masks',
                 'WMCore.WMBS.MySQL.Workflow',
                 'WMCore.WMBS.MySQL.JobGroup',
                 'WMCore.WMBS.MySQL.Fileset',
                 'WMCore.WMBS.MySQL.Locations',
                 'WMCore.WMBS.MySQL.Files',
                 'WMCore.WMBS.MySQL.Subscriptions',
                 'WMCore.WMBS.MySQL',
                 'WMCore.WMBS',
                 'WMCore.DataStructs',
                 'WMCore.WMBSFeeder.DBS',
                 'WMCore.WMBSFeeder.PhEDExNotifier',
                 'WMCore.WMBSFeeder.Fake',
                 'WMCore.WMBSFeeder',
                 'WMCore.ThreadPool.MySQL',
                 'WMCore.ThreadPool',
                 'WMCore.Services.SAM',
                 'WMCore.Services.Dashboard',
                 'WMCore.Services.JSONParser',
                 'WMCore.Services.SiteDB',
                 'WMCore.Services.Twitter',
                 'WMCore.Services',
                 'WMCore.SiteScreening',
                 'WMCore.Database',
                 'WMCore.WebTools',
                 'WMCore.HTTPFrontEnd.WMBS',
                 'WMCore.HTTPFrontEnd',
                 'WMCore'],)

