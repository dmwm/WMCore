#!/usr/bin/env python
from distutils.core import setup, Command
from unittest import TextTestRunner, TestLoader, TestSuite
from glob import glob
from os.path import splitext, basename, join as pjoin, walk
import os
from pylint import lint

"""
Build, clean and test the WMCore package.
"""

class TestCommand(Command):
    """
    Handle setup.py test with this class - walk through the directory structure 
    and build up a list of tests, then build a test suite and execute it.
    
    TODO: Pull database URL's from environment, and skip tests where database 
    URL is not present (e.g. for a slave without Oracle connection)
    """
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
    """
    Clean up (delete) compiled files
    """
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
            
class LintCommand(Command):
    user_options = [ ]
    
    def initialize_options(self):
        self._dir = os.getcwd()

    def finalize_options(self):
        pass
    
    def run(self):
        '''
        Find the code and run lint on it
        '''
        files = [ ]
        # Walk the directory tree
        for dirpath, dirnames, filenames in os.walk('./src/python/'):
            # skipping CVS directories and their contents
            pathelements = dirpath.split('/')
            if not 'CVS' in pathelements:
                # to build up a list of file names which contain tests
                for file in filenames:
                    filepath = '/'.join([dirpath, file]) 
                    files.append(filepath)
                    # run individual tests as follows
                    lint.Run(['--rcfile=standards/.pylintrc', filepath])
        # Could run a global test as:
        #input = ['--rcfile=standards/.pylintrc']
        #input.extend(files)
        #lint.Run(input)
                    
                    
def getPackages(package_dirs = []):
    packages = []
    for dir in package_dirs:
        for dirpath, dirnames, filenames in os.walk('./%s' % dir):
            # Exclude things here
            if dirpath not in ['./src/python/', './src/python/IMProv']: 
                pathelements = dirpath.split('/')
                if not 'CVS' in pathelements:
                    path = pathelements[3:]
                    packages.append('.'.join(path))
    return packages

package_dir = {'WMCore': 'src/python/WMCore',
               'WMComponent' : 'src/python/WMComponent',
               'WMQuality' : 'src/python/WMQuality'}

setup (name = 'wmcore',
       version = '1.0',
       cmdclass = { 'test': TestCommand, 
                   'clean': CleanCommand, 
                   'lint': LintCommand },
       package_dir = package_dir,
       packages = getPackages(package_dir.values()),)

