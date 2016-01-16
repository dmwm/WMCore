#!/usr/bin/env python
"""
Build, clean and test the WMCore package.
"""

from distutils.core import setup, Command
from distutils.util import get_platform

from glob import glob
from os.path import splitext, basename, join as pjoin, walk
from ConfigParser import ConfigParser, NoOptionError
import os, sys, os.path
import unittest
import time

from setup_build import BuildCommand, InstallCommand
from setup_build import get_path_to_wmcore_root, list_packages, list_static_files
from setup_test import LintCommand, ReportCommand, CoverageCommand, TestCommand
from setup_dependencies import dependencies

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
       if not os.getenv('COUCHURL', False):
           # Use the default localhost URL if none is configured.
            print 'export COUCHURL=http://localhost:5984'
       here = get_path_to_wmcore_root()

       tests = here + '/test/python'
       source = here + '/src/python'
       # Stuff we want on the path
       exepth = [source + '/WMCore/WebTools',
                 here + '/bin']

       pypath=os.getenv('PYTHONPATH', '').strip(':').split(':')

       for pth in [tests, source]:
           if pth not in pypath:
               pypath.append(pth)

       # We might want to add other executables to PATH
       expath=os.getenv('PATH', '').split(':')
       for pth in exepth:
           if pth not in expath:
               expath.append(pth)

       print 'export PYTHONPATH=%s' % ':'.join(pypath)
       print 'export PATH=%s' % ':'.join(expath)

       #We want the WMCORE root set, too
       print 'export WMCORE_ROOT=%s' % get_path_to_wmcore_root()
       print 'export WMCOREBASE=$WMCORE_ROOT'
       print 'export WTBASE=$WMCORE_ROOT/src'

# The actual setup command, and the classes associated to the various options

# Need all the packages we want to build by default, this will be overridden in sub-system builds.
# Since it's a lot of code determine it by magic.
default_packages = list_packages(['src/python/Utils',
                                  'src/python/WMCore',
                                  'src/python/WMComponent',
                                  'src/python/WMQuality',
                                  'src/python/PSetTweaks'])

setup (name = 'wmcore',
       version = '1.0',
       maintainer_email = 'hn-cms-wmDevelopment@cern.ch',
       cmdclass = {'deep_clean': CleanCommand,
                   'lint': LintCommand,
                   'report': ReportCommand,
                   'coverage': CoverageCommand ,
                   'test' : TestCommand,
                   'env': EnvCommand,
                   'build_system': BuildCommand,
                   'install_system': InstallCommand },
       # base directory for all our packages
       package_dir = {'': 'src/python/'},# % get_path_to_wmcore_root()},
       packages = default_packages,
       data_files = list_static_files())
