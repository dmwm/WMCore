#!/bin/bash

ME=reqmgr2ms
TOP=/data/srv
ROOT=/data/srv/current
CFGDIR=$ROOT/config/$ME
LOGDIR=/data/srv/logs/$ME
STATEDIR=/data/srv/state/$ME
CFGFILEOUT=$ROOT/config/$ME/config-output.py
LOG_OUT=ms-output
AUTHDIR=/data/srv/current/auth/$ME

. /data/srv/current/apps/$ME/etc/profile.d/init.sh

export PYTHONPATH=$ROOT/auth/$ME:$PYTHONPATH
export WMCORE_ROOT=$REQMGR2MS_ROOT/lib/python*/site-packages
export REQMGR_CACHE_DIR=$STATEDIR
export WMCORE_CACHE_DIR=$STATEDIR
export RUCIO_HOME=$CFGDIR
if [ -e $AUTHDIR/dmwm-service-cert.pem ] && [ -e $AUTHDIR/dmwm-service-key.pem ]; then
  export X509_USER_CERT=$AUTHDIR/dmwm-service-cert.pem
  export X509_USER_KEY=$AUTHDIR/dmwm-service-key.pem
fi

# python3 -i mongoInit.py $@

exc=$1 && shift
python3 -i $exc -c $CFGFILEOUT $@
