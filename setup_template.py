#!/usr/bin/env python

# This template for the setup script is used to build several pypi packages
# from the WMCore codebase. The variable package_name controls which package
# is built. PACKAGE_TO_BUILD is manipulated via tools/build_pypi_packages.sh
# at build time.
#
# The version number comes from WMCore/__init__.py and needs to
# follow PEP 440 conventions

from __future__ import print_function, division
import os
import sys
from setuptools import setup, Command
from setup_build import list_static_files, things_to_build
from setup_dependencies import dependencies

# get the WMCore version (thanks rucio devs)
sys.path.insert(0, os.path.abspath('src/python'))
from WMCore import __version__
wmcore_version = __version__

# the contents of package_name are modified via tools/build_pypi_packages.sh
package_name = "PACKAGE_TO_BUILD"
packages, py_modules = things_to_build(package_name, pypi=True)
data_files = list_static_files(dependencies[package_name])

# we need to override 'clean' command to remove specific files
class CleanCommand(Command):
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        os.system ('rm -rfv ./dist ./src/python/*.egg-info')

def parse_requirements(requirements_file):
    """
      Create a list for the 'install_requires' component of the setup function
      by parsing a requirements file
    """

    if os.path.exists(requirements_file):
        # return a list that contains each line of the requirements file
        return open(requirements_file, 'r').read().splitlines()
    else:
        print("ERROR: requirements file " + requirements_file + " not found.")
        sys.exit(1)

setup(name=package_name,
      version=wmcore_version,
      package_dir={'': 'src/python/'},
      packages=packages,
      py_modules=py_modules,
      data_files=data_files,
      install_requires=parse_requirements("requirements.txt"),
      maintainer='CMS DMWM Group',
      maintainer_email='hn-cms-wmdevelopment@cern.ch',
      cmdclass={
          'clean': CleanCommand,
      },
      url="https://github.com/dmwm/WMCore",
      license="Apache License, Version 2.0",
      )
