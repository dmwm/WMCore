#!/bin/bash

usage()
{
    echo -ne "\nA simple script to facilitate component patching\n"
    echo -ne "and to decrease the development && testing turnaround time.\n"
    echo -ne "Usage: \n"
    echo -ne "\t git diff --no-color | ./patchComponent.sh (reqmon | reqmgr2 | reqmgr2ms...)\n or:\n"
    echo -ne "\t curl https://patch-diff.githubusercontent.com/raw/dmwm/deployment/pull/740.patch  | ./patchComponent.sh (reqmon | reqmgr2 | reqmgr2ms...)\n"
    exit 1
}

# TODO: To check against a list of components
component=$1
[[ -z $component ]] && usage

echo "Patching component: $component"

# TODO: To fix path differences for wmagents and central services
rootDir=/data/srv/current/apps/$component/lib/python2.7/site-packages/
stripLevel=3

patch --verbose -b --version-control=numbered -d $rootDir -p$stripLevel
