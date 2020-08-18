#!/bin/bash
#
# Script used to build each application from the WMCore repo and upload to pypi.
#
# Usage:
# sh tools/build_pypi_packages.sh <wmagent|wmcore|all>
#


wmcore=false
wmagent=false
reqmgr2=false
TOBUILD=$1

case $TOBUILD in
    wmagent)
      echo "Building wmagent package"
      wmagent=true
      ;;
    wmcore)
      echo "Building wmcore package"
      wmcore=true
      ;;
    reqmgr2)
      echo "Building wmcore package"
      reqmgr2=true
      ;;
    all)
      echo "Building wmagent package"
      echo "Building wmcore package"
      wmcore=true
      wmagent=true
      reqmgr2=true
      ;;
    *)
      echo "Please enter one of the following arguments."
      echo "  all       Build all WMCore packages"
      echo "  wmagent   Build wmagent package"
      echo "  wmcore    Build wmcore package"
      echo "  reqmgr2   Build reqmgr2 package"
      exit 1
esac

# make a copy of requirements.txt to reference for each build
cp requirements.txt requirements.wmcore.txt

if $wmagent
  then
    sed 's/PACKAGE_TO_BUILD/wmagent/' setup_template.py > setup.py
    awk '/wmagent/ {print $1}' requirements.wmcore.txt > requirements.txt
    python setup.py clean sdist
    twine upload dist/wmagent-*
fi

if $wmcore
  then
    sed 's/PACKAGE_TO_BUILD/wmcore/' setup_template.py > setup.py
    awk '/wmcore/ {print $1}' requirements.wmcore.txt > requirements.txt
    python setup.py clean sdist
    twine upload dist/wmcore-*
fi

if $reqmgr2
  then
    sed 's/PACKAGE_TO_BUILD/reqmgr2/' setup_template.py > setup.py
    awk '/reqmgr2/ {print $1}' requirements.wmcore.txt > requirements.txt
    python setup.py clean sdist
    twine upload dist/reqmgr2-*
fi
