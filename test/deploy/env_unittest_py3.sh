#!/bin/bash
export USER=`whoami`
export BASE_DIR=$PWD
export TEST_DIR=$BASE_DIR/wmcore_unittest
export TEST_SRC=$TEST_DIR/WMCore/src
export TEST_SRC_PYTHON=$TEST_SRC/python
export INSTALL_DIR=$BASE_DIR/unittestdeploy/wmagent
export ADMIN_DIR=$BASE_DIR
export CERT_DIR=$BASE_DIR/certs

export ORG_SRC_PYTHON=$INSTALL_DIR/current/apps/wmagentpy3/lib/python3.8/site-packages/
export ORG_SRC_OTHER=$INSTALL_DIR/current/apps/wmagentpy3/data
export DBSOCK=$INSTALL_DIR/current/install/mysql/logs/mysql.sock

export DATABASE=mysql://unittestagent@localhost/wmcore_unittest
export COUCHURL=http://unittestagent:passwd@localhost:6994
export DIALECT=MySQL

rm -rf $ORG_SRC_PYTHON/*

ln -s $TEST_SRC_PYTHON/WMCore/ $ORG_SRC_PYTHON
ln -s $TEST_SRC_PYTHON/WMComponent/ $ORG_SRC_PYTHON
ln -s $TEST_SRC_PYTHON/PSetTweaks/ $ORG_SRC_PYTHON
ln -s $TEST_SRC_PYTHON/WMQuality/ $ORG_SRC_PYTHON
ln -s $TEST_SRC_PYTHON/Utils/ $ORG_SRC_PYTHON

rm -rf $ORG_SRC_OTHER/*

ln -s $TEST_SRC/couchapps/ $ORG_SRC_OTHER
ln -s $TEST_SRC/css/ $ORG_SRC_OTHER
ln -s $TEST_SRC/html/ $ORG_SRC_OTHER
ln -s $TEST_SRC/javascript/ $ORG_SRC_OTHER
ln -s $TEST_SRC/template/ $ORG_SRC_OTHER

export WMAGENT_SECRETS_LOCATION=$ADMIN_DIR/WMAgent_unittest.secrets
export X509_HOST_CERT=$CERT_DIR/servicecert.pem
export X509_HOST_KEY=$CERT_DIR/servicekey.pem
export X509_USER_CERT=$CERT_DIR/servicecert.pem
export X509_USER_KEY=$CERT_DIR/servicekey.pem

export install=$INSTALL_DIR/current/install/wmagentpy3
export config=$INSTALL_DIR/current/config/wmagentpy3
export manage=$config/manage

source $INSTALL_DIR/current/apps/wmagentpy3/etc/profile.d/init.sh
source $INSTALL_DIR/current/apps/wmcorepy3-devtools/etc/profile.d/init.sh

export PYTHONPATH=$TEST_SRC/../test/python:$PYTHONPATH

### some Rucio setup needed for jenkins and docker unit tests
# fetch the values defined in the secrets file and update rucio.cfg file
export RUCIO_HOME=$config/../rucio/
MATCH_RUCIO_HOST=`cat $WMAGENT_SECRETS_LOCATION | grep RUCIO_HOST | sed s/RUCIO_HOST=//`
MATCH_RUCIO_AUTH=`cat $WMAGENT_SECRETS_LOCATION | grep RUCIO_AUTH | sed s/RUCIO_AUTH=//`
sed -i "s+^rucio_host.*+rucio_host = $MATCH_RUCIO_HOST+" $RUCIO_HOME/etc/rucio.cfg
sed -i "s+^auth_host.*+auth_host = $MATCH_RUCIO_AUTH+" $RUCIO_HOME/etc/rucio.cfg
echo "Updated RUCIO_HOME file under: $RUCIO_HOME"
