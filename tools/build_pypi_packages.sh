#!/bin/bash
#
# Script used to build each application from the WMCore repo and upload to pypi.
#
# Usage
# Build a single package:
# sh tools/build_pypi_packages.sh <package name>
# Build all WMCore packages:
# sh tools/build_pypi_packages.sh all
#

set -x

# package passed as parameter, can be one of PACKAGES or "all"
TOBUILD=$1
# list of packages that can be built and uploaded to pypi
PACKAGES="wmagent wmcore reqmon reqmgr2 reqmgr2ms global-workqueue acdcserver"
PACKAGE_REGEX="^($(echo $PACKAGES | sed 's/\ /|/g')|all)$"

if [[ -z $TOBUILD ]]; then
  echo "Usage: sh tools/build_pypi_packages.sh <package name>"
  echo "Usage: sh tools/build_pypi_packages.sh all"
  exit 1
fi


# check to make sure a valid package name was passed
if [[ ! $TOBUILD =~ $PACKAGE_REGEX ]]; then
  echo "$TOBUILD is not a valid package name"
  echo "Supported packages are $PACKAGES"
  exit 1
fi

# update package list when building all packages
if [[ $TOBUILD == "all" ]]; then
  TOBUILD=$PACKAGES
fi

# loop through packages to build
for package in $TOBUILD; do
  # make a copy of requirements.txt to reference for each build
  cp requirements.txt requirements.wmcore.txt

  # update the setup script template with package name
  sed "s/PACKAGE_TO_BUILD/$package/" setup_template.py > setup.py

  # build requirements.txt file
  awk "/($package$)|($package,)/ {print \$1}" requirements.wmcore.txt > requirements.txt

  # build the package
  python setup.py clean sdist
  if [[ $? -ne 0 ]]; then
    echo "Error building package $package"
    exit 1
  fi
  
  # upload the package to pypi
  echo "Uploading package $package to PyPI"
  twine upload dist/$package-*
  
  # replace requirements.txt contents
  cp requirements.wmcore.txt requirements.txt
done


