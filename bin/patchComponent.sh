#!/bin/bash

usage()
{
    echo -ne "\nA simple script to facilitate component patching\n"
    echo -ne "and to decrease the development && testing turnaround time.\n"
    echo -ne "Usage: \n"
    echo -ne "\t git diff --no-color | sudo ./patchComponent.sh (wmagent | reqmon | reqmgr2 | reqmgr2ms...)\n or:\n"
    echo -ne "\t curl https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/11270.patch | sudo ./patchComponent.sh (wmagent | reqmon | reqmgr2 | reqmgr2ms...)\n"
    exit 1
}

# TODO: To check against a list of components
component=$1
[[ -z $component ]] && usage

echo "Patching component: $component"

if [[ $component == "wmagent" ]]
then
    rootDir=/data/srv/$component/current/apps.sw/$component/lib/python*/site-packages
else
    rootDir=/data/srv/current/apps.sw/$component/lib/python*/site-packages/
fi

stripLevel=3

patch --verbose -b --version-control=numbered -d $rootDir -p$stripLevel
