#!/bin/bash
#
# Script used to build each application from the WMCore repo and upload to pypi.
#
# Usage:
# sh tools/build_pypi_packages.sh <wmagent|wmcore|all>
#


wmcore=false
wmagent=false

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
    all)
      echo "Building wmagent package"
      echo "Building wmcore package"
      wmcore=true
      wmagent=true
      ;;
    *)
      echo "Please enter one of the following arguments."
      echo "  all       Build all WMCore packages"
      echo "  wmagent   Build wmagent package"
      echo "  wmcore    Build wmcore package"
      exit 1
esac

# clean up any previous builds
rm -fv dist/*

if $wmagent
  then
  /bin/cp requirements.wmagent.txt requirements.txt
  /bin/cp setup_wmagent.py setup.py
  python setup.py sdist
  twine upload dist/wmagent-*
fi

if $wmcore
  then
  /bin/cp requirements.wmcore.txt requirements.txt
  /bin/cp setup_wmcore.py setup.py
  python setup.py sdist
  twine upload dist/wmcore-*
fi


