#!/bin/bash
git clone git@github.com:dmwm/deployment.git
(cd deployment; curl https://github.com/dmwm/deployment/commit/3309be9ce3b628efe39e1db30b6a1f865e1fa3e4.patch | git am)
curl -s https://raw.githubusercontent.com/dmwm/WMCore/master/test/deploy/init.sh > init.sh
curl -s https://raw.githubusercontent.com/dmwm/WMCore/master/test/deploy/env_unittest.sh > env_unittest.sh
curl -s https://raw.githubusercontent.com/dmwm/WMCore/master/test/deploy/WMAgent_unittest.secrets > WMAgent_unittest.secrets
source ./init.sh
$PWD/deployment/Deploy -R wmagent-dev@1.0.6.pre7 -r comp=comp.pre -t 1.0.6.pre7 -A slc6_amd64_gcc481 -s 'prep sw post' $INSTALL_DIR admin/devtools wmagent
(mkdir $TEST_DIR; cd $TEST_DIR; git clone git@github.com:dmwm/WMCore.git)
source ./env_unittest.sh
$manage start-services
echo "when prompted type 'passwd' (without ')"
mysql -u unittestagent -p --sock $INSTALL_DIR/current/install/mysql/logs/mysql.sock --exec "create database wmcore_unittest"