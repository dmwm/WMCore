#!/usr/bin/env python
from distutils.core import setup, Command
from unittest import TextTestRunner, TestLoader, TestSuite
from glob import glob
from os.path import splitext, basename, join as pjoin, walk
from ConfigParser import ConfigParser, NoOptionError
import os, sys, os.path
import logging
import unittest
#PyLinter and coverage aren't standard, but aren't strictly necessary
can_lint = False
can_coverage = False
can_nose = False
try:
    from pylint.lint import Run 
    from pylint.lint import preprocess_options, cb_init_hook
    from pylint import checkers
    can_lint = True
except:
    pass
try:
    import coverage
    can_coverage = True
except:
    pass

try:
    import nose
    can_nose = True
except:
    pass

if can_nose:
    class TestCommand(Command):
        user_options = [ ]

        def initialize_options(self):
            pass
        
        def finalize_options(self):
            pass
    
        def run(self):
            retval =  nose.run(argv=[__file__,'--all-modules','-v','test/python'])
            if retval:
                sys.exit(0)
            else:
                sys.exit(1)
else:
    class NoseCommand(Command):
        user_options = [ ]
        def run(self):
            print "Nose isn't installed, fail"
            pass
        
        def initialize_options(self):
            pass
        
        def finalize_options(self):
            pass
        pass

if can_lint:
    class LinterRun(Run):
        def __init__(self, args, reporter=None):
            self._rcfile = None
            self._plugins = []
            preprocess_options(args, {
                # option: (callback, takearg)
                'rcfile':       (self.cb_set_rcfile, True),
                'load-plugins': (self.cb_add_plugins, True),
                })
            self.linter = linter = self.LinterClass((
                ('rcfile',
                 {'action' : 'callback', 'callback' : lambda *args: 1,
                  'type': 'string', 'metavar': '<file>',
                  'help' : 'Specify a configuration file.'}),
    
                ('init-hook',
                 {'action' : 'callback', 'type' : 'string', 'metavar': '<code>',
                  'callback' : cb_init_hook,
                  'help' : 'Python code to execute, usually for sys.path \
    manipulation such as pygtk.require().'}),
    
                ('help-msg',
                 {'action' : 'callback', 'type' : 'string', 'metavar': '<msg-id>',
                  'callback' : self.cb_help_message,
                  'group': 'Commands',
                  'help' : '''Display a help message for the given message id and \
    exit. The value may be a comma separated list of message ids.'''}),
    
                ('list-msgs',
                 {'action' : 'callback', 'metavar': '<msg-id>',
                  'callback' : self.cb_list_messages,
                  'group': 'Commands',
                  'help' : "Generate pylint's full documentation."}),
    
                ('generate-rcfile',
                 {'action' : 'callback', 'callback' : self.cb_generate_config,
                  'group': 'Commands',
                  'help' : '''Generate a sample configuration file according to \
    the current configuration. You can put other options before this one to get \
    them in the generated configuration.'''}),
    
                ('generate-man',
                 {'action' : 'callback', 'callback' : self.cb_generate_manpage,
                  'group': 'Commands',
                  'help' : "Generate pylint's man page.",'hide': 'True'}),
    
                ('errors-only',
                 {'action' : 'callback', 'callback' : self.cb_error_mode,
                  'short': 'e',
                  'help' : '''In error mode, checkers without error messages are \
    disabled and for others, only the ERROR messages are displayed, and no reports \
    are done by default'''}),
    
                ('profile',
                 {'type' : 'yn', 'metavar' : '<y_or_n>',
                  'default': False,
                  'help' : 'Profiled execution.'}),
    
                ), option_groups=self.option_groups,
                   reporter=reporter, pylintrc=self._rcfile)
            # register standard checkers
            checkers.initialize(linter)
            # load command line plugins
            linter.load_plugin_modules(self._plugins)
            # read configuration
            linter.disable_message('W0704')
            linter.read_config_file()
            # is there some additional plugins in the file configuration, in
            config_parser = linter._config_parser
            if config_parser.has_option('MASTER', 'load-plugins'):
                plugins = splitstrip(config_parser.get('MASTER', 'load-plugins'))
                linter.load_plugin_modules(plugins)
            # now we can load file config and command line, plugins (which can
            # provide options) have been registered
            linter.load_config_file()
            if reporter:
                # if a custom reporter is provided as argument, it may be overriden
                # by file parameters, so re-set it here, but before command line
                # parsing so it's still overrideable by command line option
                linter.set_reporter(reporter)
            args = linter.load_command_line_configuration(args)
            # insert current working directory to the python path to have a correct
            # behaviour
            sys.path.insert(0, os.getcwd())
            if self.linter.config.profile:
                print >> sys.stderr, '** profiled run'
                from hotshot import Profile, stats
                prof = Profile('stones.prof')
                prof.runcall(linter.check, args)
                prof.close()
                data = stats.load('stones.prof')
                data.strip_dirs()
                data.sort_stats('time', 'calls')
                data.print_stats(30)
            sys.path.pop(0)
        
        def cb_set_rcfile(self, name, value):
            """callback for option preprocessing (ie before optik parsing)"""
            self._rcfile = value
    
        def cb_add_plugins(self, name, value):
            """callback for option preprocessing (ie before optik parsing)"""
            self._plugins.extend(splitstrip(value))
    
        def cb_error_mode(self, *args, **kwargs):
            """error mode:
            * checkers without error messages are disabled
            * for others, only the ERROR messages are displayed
            * disable reports
            * do not save execution information
            """
            self.linter.disable_noerror_checkers()
            self.linter.set_option('disable-msg-cat', 'WCRFI')
            self.linter.set_option('reports', False)
            self.linter.set_option('persistent', False)
    
        def cb_generate_config(self, *args, **kwargs):
            """optik callback for sample config file generation"""
            self.linter.generate_config(skipsections=('COMMANDS',))
            
        def cb_generate_manpage(self, *args, **kwargs):
            """optik callback for sample config file generation"""
            from pylint import __pkginfo__
            self.linter.generate_manpage(__pkginfo__)
            
        def cb_help_message(self, option, opt_name, value, parser):
            """optik callback for printing some help about a particular message"""
            self.linter.help_message(splitstrip(value))
    
        def cb_list_messages(self, option, opt_name, value, parser):
            """optik callback for printing available messages"""
            self.linter.list_messages()
else:
    class LinterRun:
        def __init__(self):
            pass

"""
Build, clean and test the WMCore package.
"""

def generate_filelist(basepath=None, recurse=True, ignore=False):
    if basepath:
        walkpath = os.path.join(get_relative_path(), 'src/python', basepath)
    else:
        walkpath = os.path.join(get_relative_path(), 'src/python')
    
    files = []
    
    if walkpath.endswith('.py'):
        if ignore and walkpath.endswith(ignore):
            files.append(walkpath)
    else:
        for dirpath, dirnames, filenames in os.walk(walkpath):
            # skipping CVS directories and their contents
            pathelements = dirpath.split('/')
            result = []
            if not 'CVS' in pathelements:
                # to build up a list of file names which contain tests
                for file in filenames:
                    if file.endswith('.py'):
                        filepath = '/'.join([dirpath, file]) 
                        files.append(filepath)

    if len(files) == 0 and recurse:
        files = generate_filelist(basepath + '.py', not recurse)

    return files

def lint_score(stats, evaluation):
    return eval(evaluation, {}, stats)

def lint_files(files, reports=False):
    """
    lint a (list of) file(s) and return the results as a dictionary containing
    filename : result_dict
    """
    
    rcfile=os.path.join(get_relative_path(),'standards/.pylintrc')
    
    arguements = ['--rcfile=%s' % rcfile, '--ignore=DefaultConfig.py']
    
    if not reports:
        arguements.append('-rn')
    
    arguements.extend(files)
    
    lntr = LinterRun(arguements)
    
    results = {}
    for file in files:
        lntr.linter.check(file)
        results[file] = {'stats': lntr.linter.stats,
                         'score': lint_score(lntr.linter.stats, 
                                             lntr.linter.config.evaluation)
                         }
        if reports:
            print '----------------------------------'
            print 'Your code has been rated at %.2f/10' % \
                    lint_score(lntr.linter.stats, lntr.linter.config.evaluation)

    return results, lntr.linter.config.evaluation

class CleanCommand(Command):
    description = "Clean up (delete) compiled files"
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
    description = "Lint all files in the src tree"
    """
    TODO: better format the test results, get some global result, make output 
    more buildbot friendly.    
    """
    
    user_options = [ ('package=', 'p', 'package to lint, default to None'),
                ('report', 'r', 'return a detailed lint report, default False')]
    
    def initialize_options(self):
        self._dir = get_relative_path()
        self.package = None
        self.report = False
        
    def finalize_options(self):
        if self.report:
            self.report = True

    def run(self):
        '''
        Find the code and run lint on it
        '''
        if can_lint:
            srcpypath = os.path.join(self._dir, 'src/python/')
            
            sys.path.append(srcpypath) 
            
            files_to_lint = []
            
            if self.package:
                if self.package.endswith('.py'):
                    cnt = self.package.count('.') - 1
                    files_to_lint = generate_filelist(self.package.replace('.', '/', cnt), 'DeafultConfig.py')
                else:
                    files_to_lint = generate_filelist(self.package.replace('.', '/'), 'DeafultConfig.py')
            else:
                files_to_lint = generate_filelist(ignore='DeafultConfig.py')
            
            results, evaluation = lint_files(files_to_lint, self.report)
            ln = len(results)
            scr = 0
            print
            for k, v in results.items():
                print "%s: %.2f/10" % (k.replace('src/python/', ''), v['score'])
                scr += v['score']
            if ln > 1:
                print '--------------------------------------------------------'
                print 'Average pylint score for %s is: %.2f/10' % (self.package,
                                                                  scr/ln)
                
        else:
            print 'You need to install pylint before using the lint command'
            
class ReportCommand(Command):
    description = "Generate a simple html report for ease of viewing in buildbot"
    """
    To contain:
        average lint score
        % code coverage
        list of classes missing tests
        etc.
    """
    
    user_options = [ ]
    
    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        """
        run all the tests needed to generate the report and make an
        html table
        """
        files = generate_filelist()
        
        error = 0 
        warning = 0
        refactor = 0
        convention = 0 
        statement = 0
        
        srcpypath = '/'.join([get_relative_path(), 'src/python/'])
        sys.path.append(srcpypath)

        cfg = ConfigParser()
        cfg.read('standards/.pylintrc')
        
        # Supress stdout/stderr
        sys.stderr = open('/dev/null', 'w')
        sys.stdout = open('/dev/null', 'w')
        # wrap it in an exception handler, otherwise we can't see why it fails
        try:
            # lint the code
            for stats in lint_files(files):
                error += stats['error']
                warning += stats['warning']
                refactor += stats['refactor']
                convention += stats['convention'] 
                statement += stats['statement']
        except Exception,e:
            # and restore the stdout/stderr
            sys.stderr = sys.__stderr__
            sys.stdout = sys.__stderr__
            raise e
        
        # and restore the stdout/stderr
        sys.stderr = sys.__stderr__
        sys.stdout = sys.__stderr__ 
        
        stats = {'error': error,
            'warning': warning,
            'refactor': refactor,
            'convention': convention, 
            'statement': statement}
        
        lint_score = eval(cfg.get('MASTER', 'evaluation'), {}, stats)
        coverage = 0 # TODO: calculate this
        testless_classes = [] # TODO: generate this
        
        print "<table>"
        print "<tr>"
        print "<td colspan=2><h1>WMCore test report</h1></td>"
        print "</tr>"
        print "<tr>"
        print "<td>Average lint score</td>"
        print "<td>%.2f</td>" % lint_score
        print "</tr>"
        print "<tr>"
        print "<td>% code coverage</td>"
        print "<td>%s</td>" % coverage
        print "</tr>"
        print "<tr>"
        print "<td>Classes missing tests</td>"
        print "<td>"
        if len(testless_classes) == 0:
            print "None"
        else:
            print "<ul>"
            for c in testless_classes:
                print "<li>%c</li>" % c
            print "</ul>"
        print "</td>"
        print "</tr>"
        print "</table>"
            
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
    
    user_options = [ ]
    
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
            dataFile = None
            cov = None
            
            # attempt to load previously cached coverage information if it exists
            try:
                dataFile = open("wmcore-coverage.dat","r")
                cov = coverage.coverage(branch = True, data_file='wmcore-coverage.dat')
                cov.load()
            except:  
                cov = coverage.coverage(branch = True, )
                cov.start()
                runUnitTests()
                cov.stop()
                cov.save()
                
            # we have our coverage information, now let's do something with it
            # get a list of modules
            cov.report(morfs = files, file=sys.stdout)
            return 0
        else:
            print 'You need the coverage module installed before running the' +\
                            ' coverage command'
    
class DumbCoverageCommand(Command):
    description = "Run a simple coverage test - find classes that don't have a unit test"
    
    user_options = [ ]
    
    def initialize_options(self):
        pass

    def finalize_options(self):
        pass
    
    def run(self):
        """
        Determine the code's test coverage in a dumb way and return that as a 
        float.
        """
        print "This determines test coverage in a very crude manner. If your"
        print "test file is incorrectly named it will not be counted, and" 
        print "result in a lower coverage score." 
        print '----------------------------------------------------------------'
        filelist = generate_filelist()
        tests = 0
        files = 0
        pkgcnt = 0
        dir = get_relative_path()
        pkg = {'name': '', 'files': 0, 'tests': 0}
        for f in filelist:
            testpath = '/'.join([dir, f])
            pth = testpath.split('./src/python/')
            pth.append(pth[1].replace('/', '_t/').replace('.', '_t.'))
            if pkg['name'] == pth[2].rsplit('/', 1)[0].replace('_t/', '/'):
                # pkg hasn't changed, increment counts
                pkg['files'] += 1
            else:
                # new package, print stats for old package
                pkgcnt += 1
                if pkg['name'] != '' and pkg['files'] > 0:
                    print 'Package %s has coverage %.1f percent' % (pkg['name'], 
                                (float(pkg['tests'])/float(pkg['files']) * 100))
                # and start over for the new package
                pkg['name'] = pth[2].rsplit('/', 1)[0].replace('_t/', '/')
                # do global book keeping
                files += pkg['files']
                tests += pkg['tests'] 
                pkg['files'] = 0 
                pkg['tests'] = 0
            pth[1] = 'test/python'
            testpath = '/'.join(pth)
            try: 
                os.stat(testpath)
                pkg['tests'] += 1
            except:
                pass
            
        coverage = (float(tests) / float(files)) * 100
        print '----------------------------------------------------------------'
        print 'Code coverage (%s packages) is %.2f percent' % (pkgcnt, coverage)
        return coverage

class EnvCommand(Command):
    description = "Configure the PYTHONPATH, DATABASE and PATH variables to" +\
    "some sensible defaults, if not already set. Call with -q when eval-ing," +\
    """ e.g.:
        eval `python setup.py -q env`
    """
    
    user_options = [ ]
    
    def initialize_options(self):
        pass

    def finalize_options(self):
        pass
    
    def run(self):
        if not os.getenv('DATABASE', False):
            # Use an in memory sqlite one if none is configured. 
             print 'export DATABASE=sqlite://'
            
        here = get_relative_path()
        
        tests = here + '/test/python'
        source = here + '/src/python'
        webpth = source + '/WMCore/WebTools'
        
        pypath=os.getenv('PYTHONPATH', '').strip(':').split(':')
         
        for pth in [tests, source]:
            if pth not in pypath:
                pypath.append(pth)
        
        # We might want to add other executables to PATH
        expath=os.getenv('PATH', '').split(':')
        for pth in [webpth]:
            if pth not in expath:
                expath.append(pth)
              
        print 'export PYTHONPATH=%s' % ':'.join(pypath)
        print 'export PATH=%s' % ':'.join(expath)
        
        #We want the WMCORE root set, too
        print 'export WMCOREBASE=%s' % get_relative_path()


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
       maintainer_email = 'hn-cms-wmDevelopment@cern.ch',
       cmdclass = {'clean': CleanCommand, 
                   'lint': LintCommand,
                   'report': ReportCommand,
                   'coverage': CoverageCommand ,
                   'missing': DumbCoverageCommand,
                   'env': EnvCommand,
                   'test' : TestCommand },
       package_dir = package_dir,
       packages = getPackages(package_dir.values()),)

