#!/usr/bin/env python
"""
Build, clean and test the WMCore package.
"""
from __future__ import print_function

import os
import os.path
import importlib.util
from setuptools import setup, find_packages, Command
from setup_build import BuildCommand, InstallCommand, get_path_to_wmcore_root, list_static_files
from setup_test import CoverageCommand, TestCommand


class CleanCommand(Command):
    description = "Clean up (delete) compiled files"
    user_options = []

    def initialize_options(self):
        self.clean_files = [
            os.path.join(root, f) for root, _, files in os.walk('.')
            for f in files if f.endswith('.pyc')
        ]

    def finalize_options(self):
        pass

    def run(self):
        for clean_file in self.clean_files:
            try:
                os.unlink(clean_file)
            except Exception:
                pass


class EnvCommand(Command):
    description = "Configure the PYTHONPATH, DATABASE and PATH variables"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        if not os.getenv('COUCHURL'):
            print('export COUCHURL=http://localhost:5984')

        here = get_path_to_wmcore_root()
        tests, source = here + '/test/python', here + '/src/python'
        exepaths = [source + '/WMCore/WebTools', here + '/bin']

        pypath = os.getenv('PYTHONPATH', '').strip(':').split(':')
        for pth in [tests, source]:
            if pth not in pypath:
                pypath.append(pth)

        expath = os.getenv('PATH', '').split(':')
        for pth in exepaths:
            if pth not in expath:
                expath.append(pth)

        print(f'export PYTHONPATH={":".join(pypath)}')
        print(f'export PATH={":".join(expath)}')
        print(f'export WMCORE_ROOT={here}')
        print('export WMCOREBASE=$WMCORE_ROOT')
        print('export WTBASE=$WMCORE_ROOT/src')

def get_version():
    """Retrieve version from WMCore package"""
    wmcore_root = os.path.dirname(__file__)
    wmcore_init_path = os.path.join(wmcore_root, 'src', 'python', 'WMCore', '__init__.py')

    spec = importlib.util.spec_from_file_location("wmcore", wmcore_init_path)
    wmcore = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wmcore)
    return wmcore.__version__

# Find packages dynamically
DEFAULT_PACKAGES = find_packages(where="src/python")

# Read long description from README
long_description = ""
if os.path.exists("README.md"):
    with open("README.md", encoding="utf-8") as f:
        long_description = f.read()

package_data = []
for _, values in list_static_files():
    for val in values:
        package_data.append(val)

package_version = get_version()

setup(
    name="wmcore",
    version=package_version,
    maintainer="CMS DMWM Group",
    maintainer_email="hn-cms-wmDevelopment@cern.ch",
    cmdclass={
        "deep_clean": CleanCommand,
        "coverage": CoverageCommand,
        "test": TestCommand,
        "env": EnvCommand,
        "build_system": BuildCommand,
        "install_system": InstallCommand,
    },
    package_dir={"": "src/python"},
    packages=DEFAULT_PACKAGES,
    package_data={"": package_data},  # Use package_data instead of data_files
    include_package_data=True,  # Ensure non-Python files are included
    url="https://github.com/dmwm/WMCore",
    license="Apache License 2.0",
    download_url=f"https://github.com/dmwm/WMCore/tarball/{package_version}",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
