#!/bin/bash
export BASE_DIR=$PWD
export TEST_DIR=$BASE_DIR/wmcore_unittest
export TEST_SRC=$TEST_DIR/WMCore/src
export TEST_SRC_PYTHON=$TEST_SRC/python
export INSTALL_DIR=$BASE_DIR/unittestdeploy/wmagent
export ADMIN_DIR=$BASE_DIR
export CERT_DIR=$BASE_DIR/certs

export ORG_SRC_PYTHON=$INSTALL_DIR/current/apps/wmagent/lib/python2.7/site-packages/
export ORG_SRC_OTHER=$INSTALL_DIR/current/apps/wmagent/data
export DBSOCK=$INSTALL_DIR/current/install/mysql/logs/mysql.sock

export DATABASE=mysql://unittestagent:passwd@localhost/wmcore_unittest
export COUCHURL=http://unittestagent:passwd@localhost:6994
export DIALECT=MySQL

rm -rf $ORG_SRC_PYTHON/*

ln -s $TEST_SRC_PYTHON/WMCore/ $ORG_SRC_PYTHON
ln -s $TEST_SRC_PYTHON/WMComponent/ $ORG_SRC_PYTHON
ln -s $TEST_SRC_PYTHON/PSetTweaks/ $ORG_SRC_PYTHON
ln -s $TEST_SRC_PYTHON/WMQuality/ $ORG_SRC_PYTHON

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

export install=$INSTALL_DIR/current/install/wmagent
export config=$INSTALL_DIR/current/config/wmagent
export manage=$config/manage

source $INSTALL_DIR/current/apps/wmagent/etc/profile.d/init.sh
source $INSTALL_DIR/current/apps/wmcore-devtools/etc/profile.d/init.sh

export PYTHONPATH=$TEST_SRC/../test/python:$PYTHONPATH
