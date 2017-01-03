from __future__ import print_function

import atexit
import hashlib
import os
import os.path
import pickle
import signal
import sys
import threading
import time
from distutils.core import Command

from setup_build import get_path_to_wmcore_root

# pylint and coverage aren't standard, but aren't strictly necessary
# you should get them though
can_coverage = False
can_nose = False

NEEDS_OWN_SLICE = ['Api_t', 'Simple_t']

sys.setrecursionlimit(10000)  # Eliminates recursion exceptions with nose

try:
    import coverage

    can_coverage = True
except ImportError:
    pass
can_coverage = False

try:
    import nose
    import nose.case
    import nose.failure
    from nose.plugins import Plugin

    can_nose = True
except ImportError:
    pass


def generate_filelist(basepath=None, recurse=True, ignore=False):
    """
    Recursively get a list of files to test/lint
    """
    if basepath:
        walkpath = os.path.join(get_path_to_wmcore_root(), 'src/python', basepath)
    else:
        walkpath = os.path.join(get_path_to_wmcore_root(), 'src/python')

    files = []

    if walkpath.endswith('.py'):
        if ignore and walkpath.endswith(ignore):
            files.append(walkpath)
    else:
        for dirpath, dummyDirnames, filenames in os.walk(walkpath):
            # skipping CVS directories and their contents
            pathelements = dirpath.split('/')
            if 'CVS' not in pathelements:
                # to build up a list of file names which contain tests
                for filename in filenames:
                    if filename.endswith('.py'):
                        filepath = '/'.join([dirpath, filename])
                        files.append(filepath)

    if len(files) == 0 and recurse:
        files = generate_filelist(basepath + '.py', not recurse)

    return files


if can_nose:
    def get_subpackages(directory, prefix=""):
        if prefix.startswith('.'):
            prefix = prefix[1:]
        result = []
        for subdir in os.listdir(directory):
            if os.path.isdir(os.path.join(directory, subdir)):
                result.extend(get_subpackages(os.path.join(directory, subdir), prefix + "." + subdir))
            else:
                # should be a file
                if subdir.endswith('.py'):
                    if subdir.startswith('__init__.py'):
                        continue
                    result.append(prefix + "." + subdir[:-3])
        return result


    def trapExit(code):
        """
        Cherrypy likes to call os._exit() which causes the interpreter to
        bomb without a chance of catching it. This sucks. This function
        will replace os._exit() and throw an exception instead
        """
        if hasattr(threading.local(), "isMain") and threading.local().isMain:
            # The main thread should raise an exception
            sys.stderr.write("*******EXIT WAS TRAPPED**********\n")
            raise RuntimeError("os._exit() was called in the main thread")
        elif "DONT_TRAP_EXIT" in os.environ:
            # We trust this component to run real exits
            os.DMWM_REAL_EXIT(code)
        else:
            # os._exit on child threads should just blow away the thread
            raise SystemExit("os._exit() was called in a child thread. " + \
                             "Protecting the interpreter and trapping it")


    class DetailedOutputter(Plugin):
        name = "detailed"

        def __init__(self):
            super(DetailedOutputter, self).__init__()
            self.stream = None

        def setOutputStream(self, stream):
            """Get handle on output stream so the plugin can print id #s
            """
            self.stream = stream

        def beforeTest(self, test):
            if test.id().startswith('nose.failure'):
                pass
                # Plugin.beforeTest(self, test)
            else:

                self.stream.write('%s -- ' % (test.id(),))

        def configure(self, options, conf):
            """
                Configure plugin. Skip plugin is enabled by default.
            """
            self.enabled = True


    class TestCommand(Command):
        """Runs our test suite"""
        # WARNING WARNING WARNING
        # if you set this to true, I will really delete your database
        #  after every test
        # WARNING WARNING WARNING
        user_options = [('reallyDeleteMyDatabaseAfterEveryTest=',
                         None,
                         'If you set this I WILL DELETE YOUR DATABASE AFTER EVERY TEST. DO NOT RUN ON A PRODUCTION SYSTEM'),
                        ('buildBotMode=',
                         None,
                         'Are we running inside buildbot?'),
                        ('workerNodeTestsOnly=',
                         None,
                         "Are we testing WN functionality? (Only runs WN-specific tests)"),
                        ('testCertainPath=',
                         None,
                         "Only runs tests below a certain path (i.e. test/python searches the whole test tree"),
                        ('quickTestMode=',
                         None,
                         "Fails on the first error, doesn't compute coverage"),
                        ('testTotalSlices=',
                         None,
                         "Number of ways to split up the test suite"),
                        ('testCurrentSlice=',
                         None,
                         "Which slice to run (zero-based index)"),
                        ('testingRoot=',
                         None,
                         "Primarily used by buildbot. Gives the path to the root of the test tree (i.e. the directory with WMCore_t)"),
                        ('testMinimumIndex=',
                         None,
                         "The minimum ID to be executed (advanced use)"),
                        ('testMaximumIndex=',
                         None,
                         "The maximum ID to be executed (advanced use)")]

        def initialize_options(self):
            self.reallyDeleteMyDatabaseAfterEveryTest = False
            self.buildBotMode = False
            self.workerNodeTestsOnly = False
            self.testCertainPath = False
            self.quickTestMode = False
            self.testingRoot = "test/python"
            self.testTotalSlices = 1
            self.testCurrentSlice = 0
            self.testMinimumIndex = 0
            self.testMaximumIndex = 9999999

        def finalize_options(self):
            pass

        def callNose(self, args, paths):
            # let people specify more than one path
            pathList = paths.split(':')

            # sometimes this doesn't get removed
            if os.path.exists('.noseids'):
                os.unlink('.noseids')

            # run once to get splits
            collectOnlyArgs = args[:]
            collectOnlyArgs.extend(['-q', '--collect-only', '--with-id'])
            retval = nose.run(argv=collectOnlyArgs, addplugins=[DetailedOutputter()])
            if not retval:
                print("Failed to collect TestCase IDs")
                return retval

            idhandle = open(".noseids", "r")
            testIds = pickle.load(idhandle)['ids']
            idhandle.close()

            if os.path.exists("nosetests.xml"):
                os.unlink("nosetests.xml")

            print("path lists is %s" % pathList)
            # divide it up
            totalCases = len(testIds)
            myIds = []
            for testID in testIds.keys():
                if int(testID) >= int(self.testMinimumIndex) and int(testID) <= int(self.testMaximumIndex):
                    # generate a stable ID for sorting
                    if int(self.testCurrentSlice) < int(self.testTotalSlices):  # testCurrentSlice is 0-indexed
                        if len(testIds[testID]) == 3:
                            if testIds[testID][1] in NEEDS_OWN_SLICE:
                                print('%s needs own slice' % testIds[testID][1])
                                continue
                            testName = "%s%s" % (testIds[testID][1], testIds[testID][2])
                            testHash = hashlib.md5(testName).hexdigest()
                            hashSnip = testHash[:7]
                            hashInt = int(hashSnip, 16)
                        else:
                            print('No hash found, using ID number %s' % testID)
                            hashInt = testID

                        if (hashInt % int(self.testTotalSlices)) == int(self.testCurrentSlice):
                            for path in pathList:
                                if path in testIds[testID][0]:
                                    myIds.append(str(testID))
                                    break
                    else:
                        if testIds[testID][1] == NEEDS_OWN_SLICE[int(self.testCurrentSlice) - int(self.testTotalSlices)]:
                            print('Filling extra slice with %s %s' % (testIds[testID][1], testIds[testID][1]))
                            myIds.append(str(testID))  # TODO: allow for multiple dedicated slices

            myIds = sorted(myIds)
            print("Out of %s cases, we will run %s" % (totalCases, len(myIds)))
            if not myIds:
                return True

            args.extend(['-v', '--with-id'])
            args.extend(myIds)
            return nose.run(argv=args)

        def run(self):

            # trap os._exit
            os.DMWM_REAL_EXIT = os._exit
            os._exit = trapExit
            threading.local().isMain = True

            testPath = 'test/python'
            if self.testCertainPath:
                print("Using the tests below: %s" % self.testCertainPath)
                testPath = self.testCertainPath
            else:
                print("Nose is scanning all tests")

            if self.quickTestMode:
                quickTestArg = ['--stop']
            else:
                quickTestArg = []

            if self.reallyDeleteMyDatabaseAfterEveryTest:
                print("#### WE ARE DELETING YOUR DATABASE. 3 SECONDS TO CANCEL ####")
                print("#### buildbotmode is %s" % self.buildBotMode)
                sys.stdout.flush()
                import WMQuality.TestInit
                WMQuality.TestInit.deleteDatabaseAfterEveryTest("I'm Serious")
                time.sleep(4)
            if self.workerNodeTestsOnly:
                args = [__file__, '--with-xunit', '-m', '(_t.py$)|(_t$)|(^test)', '-a', 'workerNodeTest',
                        self.testingRoot]
                args.extend(quickTestArg)
                retval = self.callNose(args, paths=testPath)
            elif not self.buildBotMode:
                args = [__file__, '--with-xunit', '-m', '(_t.py$)|(_t$)|(^test)', '-a', '!workerNodeTest',
                        self.testingRoot]
                args.extend(quickTestArg)
                retval = self.callNose(args, paths=testPath)
            else:
                print("### We are in buildbot mode ###")
                srcRoot = os.path.join(os.path.normpath(os.path.dirname(__file__)), 'src', 'python')
                modulesToCover = []
                modulesToCover.extend(get_subpackages(os.path.join(srcRoot, 'WMCore'), 'WMCore'))
                modulesToCover.extend(get_subpackages(os.path.join(srcRoot, 'WMComponent'), 'WMComponent'))
                modulesToCover.extend(get_subpackages(os.path.join(srcRoot, 'WMQuality'), 'WMQuality'))
                sys.stdout.flush()
                if not quickTestArg:
                    retval = self.callNose([__file__, '--with-xunit', '-m', '(_t.py$)|(_t$)|(^test)', '-a',
                                            '!workerNodeTest,!integration,!performance,!lifecycle,!__integration__,!__performance__,!__lifecycle__',
                                            #  '--with-coverage','--cover-html','--cover-html-dir=coverageHtml','--cover-erase',
                                            #  '--cover-package=' + moduleList, '--cover-inclusive',
                                            testPath],
                                           paths=testPath)
                else:
                    retval = self.callNose([__file__, '--with-xunit', '-m', '(_t.py$)|(_t$)|(^test)', '-a',
                                            '!workerNodeTest,!integration,!performance,!lifecycle,!__integration__,!__performance__,!__lifecycle__',
                                            '--stop', testPath],
                                           paths=testPath)

            threadCount = len(threading.enumerate())

            # Set the signal handler and a 20-second alarm
            def signal_handler(dummy1, dummy2):
                sys.stderr.write("Timeout reached trying to shut down. Force killing...\n")
                sys.stderr.flush()
                if retval:
                    os.DMWM_REAL_EXIT(0)
                else:
                    os.DMWM_REAL_EXIT(1)

            signal.signal(signal.SIGALRM, signal_handler)
            signal.alarm(20)
            marker = open("nose-marker.txt", "w")
            marker.write("Ready to be slayed\n")
            marker.flush()
            marker.close()

            if threadCount > 1:
                import cherrypy
                sys.stderr.write(
                    "There are %s threads running. Cherrypy may be acting up.\n" % len(threading.enumerate()))
                sys.stderr.write("The threads are: \n%s\n" % threading.enumerate())
                atexit.register(cherrypy.engine.stop)
                cherrypy.engine.exit()
                sys.stderr.write("Asked cherrypy politely to commit suicide\n")
                sys.stderr.write("Now there are %s threads running\n" % len(threading.enumerate()))
                sys.stderr.write("The threads are: \n%s\n" % threading.enumerate())

            threadCount = len(threading.enumerate())
            print("Testing complete, there are now %s threads" % len(threading.enumerate()))

            # try to exit
            if retval:
                sys.exit(0)
            else:
                sys.exit(1)

            # if we got here, then sys.exit got cancelled by the alarm...
            sys.stderr.write("Failed to exit after 30 secs...something hung\n")
            sys.stderr.write("Forcing process to die")
            os.DMWM_REAL_EXIT()


else:
    class TestCommand(Command):
        user_options = []

        def run(self):
            print(
                "Nose isn't installed. You must install the nose package to run tests (easy_install nose might do it)")
            sys.exit(1)

        def initialize_options(self):
            pass

        def finalize_options(self):
            pass



class CoverageCommand(Command):
    description = "Run code coverage tests"
    """
    To do this, we need to run all the unittests within the coverage
    framework to record all the lines(and branches) executed
    unfortunately, we have multiple code paths per database schema, so
    we need to find a way to merge them.

    TODO: modify the test command to have a flag to record code coverage
          the file thats used can then be used here, saving us from running
          our tests twice
    """

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        """
        Determine the code's test coverage and return that as a float

        http://nedbatchelder.com/code/coverage/
        """
        if can_coverage:
            files = generate_filelist()
            cov = None

            # attempt to load previously cached coverage information if it exists
            try:
                cov = coverage.coverage(branch=True, data_file='wmcore-coverage.dat')
                cov.load()
            except Exception:
                cov = coverage.coverage(branch=True, )
                cov.start()
                #  runUnitTests() Undefined, no idea where this was supposed to come from - EWV
                cov.stop()
                cov.save()

            # we have our coverage information, now let's do something with it
            # get a list of modules
            cov.report(morfs=files, file=sys.stdout)
            return 0
        else:
            print('You need the coverage module installed before running the' + \
                  ' coverage command')
