#!/bin/bash

###
# usage
# deploy current version of wmagent external library and use the dmwm/master WMCore code to set up unittest
# sh ./deploy_unittest.sh
#
# Also optional values can be specified. -v deploy agent version, -r git repository for code to test,
# -b branch name of the repository.
# following
# i.e) sh ./deploy_unittest.sh -v 1.0.6 -r ticonaan -v 1.0.5_wmagent
#
# for running the test check the tutorial, https://github.com/dmwm/WMCore/wiki/Setup-wmcore-unittest
###
DMWM_ARCH=slc7_amd64_gcc630
VERSION=$(curl -s "http://cmsrep.cern.ch/cgi-bin/repos/comp/$DMWM_ARCH?C=M;O=D" | grep -oP "(?<=>cms\+wmagent-dev\+).*(?=-1-1)" | head -1)

REPOSITORY=dmwm
BRANCH=
UPDATE=false

deploy_agent() {

    git clone https://github.com/dmwm/deployment.git
    curl -s https://raw.githubusercontent.com/dmwm/WMCore/master/test/deploy/init.sh > init.sh
    curl -s https://raw.githubusercontent.com/dmwm/WMCore/master/test/deploy/env_unittest.sh > env_unittest.sh
    curl -s https://raw.githubusercontent.com/dmwm/WMCore/master/test/deploy/WMAgent_unittest.secrets > WMAgent_unittest.secrets
    source ./init.sh
    $PWD/deployment/Deploy -R wmagent-dev@$1 -r comp=comp -t $1 -A $DMWM_ARCH -s 'prep sw post' $INSTALL_DIR admin/devtools wmagent
}

setup_test_src() {
    (
     mkdir $TEST_DIR;
     cd $TEST_DIR;
     git clone https://github.com/$1/WMCore.git;
     cd WMCore
     # if branch is set check out the branch
     if [ -n $2 ]
     then
        git checkout $2
     fi;
    )
}

update_src() {
    (
    rm -rf $TEST_DIR;
    setup_test_src $1 $2
    )
}

while [ $# -gt 0 ]
do
    case "$1" in
        -v)  VERSION=$2; shift;;
        -r)  REPOSITORY=$2; shift;;
        -b)  BRANCH=$2; shift;;
        -u)  UPDATE=true;;
        *)  break;;	# terminate while loop
    esac
    shift
done

if [ $UPDATE = "true" ]
then
    source ./env_unittest.sh
    update_src $REPOSITORY $BRANCH
else
    echo "--- deploying agent $VERSION with local user $USER"
    # deploy agent
    deploy_agent $VERSION
    echo "--- updating agent source repository $REPOSITORY, branch $BRANCH"
    # checkout test source
    setup_test_src $REPOSITORY $BRANCH

    # swap the source code from deployed one test source
    source ./env_unittest.sh
    echo "--- starting services"
    $manage start-services

    echo "--- creating MariaDB database"
    mysql -u $USER --socket=$INSTALL_DIR/current/install/mysql/logs/mysql.sock --execute "create database wmcore_unittest"
fi
