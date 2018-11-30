#!/usr/bin/env python

# This setup script is used to build the WMAgent pypi package.
# The version number comes from WMCore/__init__.py and needs to
# follow PEP 440 conventions

from __future__ import print_function, division
import os
import sys
import imp
from setuptools import setup
from setup_build import list_packages, list_static_files, get_path_to_wmcore_root

# Obnoxiously, there's a dependency cycle when building packages. We'd like
# to simply get the current WMCore version by using
# from WMCore import __version__
# But PYTHONPATH isn't set until after the package is built, so we can't
# depend on the python module resolution behavior to load the version.
# Instead, we use the imp module to load the source file directly by
# filename.
wmcore_root = get_path_to_wmcore_root()
wmcore_package = imp.load_source('temp_module', os.path.join(wmcore_root,
                                                             'src',
                                                             'python',
                                                             'WMCore',
                                                             '__init__.py'))
wmcore_version = wmcore_package.__version__

# Requirements file for pip dependencies
requirements = "requirements.txt"


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


setup(name='wmagent',
      version=wmcore_version,
      maintainer='CMS DMWM Group',
      maintainer_email='hn-cms-dmDevelopment@cern.ch',
      package_dir={'': 'src/python/'},
      packages=list_packages(['src/python/Utils',
                              'src/python/WMCore',
                              'src/python/WMComponent',
                              'src/python/WMQuality',
                              'src/python/PSetTweaks']),
      data_files=list_static_files(),
      install_requires=parse_requirements(requirements),
      url="https://github.com/dmwm/WMCore",
      license="Apache License, Version 2.0",
      )
