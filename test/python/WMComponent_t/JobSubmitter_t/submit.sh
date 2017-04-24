#!/bin/bash


SANDBOX=$1
INDEX=$2

ls
. /uscmst1/prod/sw/cms/slc6_amd64_gcc493/external/python/2.7.13/etc/profile.d/init.sh 
. /uscmst1/prod/sw/cms/setup/shrc prod
# BADPYTHON
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/uscmst1/prod/sw/cms/slc6_amd64_gcc493/external/openssl/1.0.1p/lib:/uscmst1/prod/sw/cms/slc6_amd64_gcc493/external/bz2lib/1.0.6/lib
python2 Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export PYTHONPATH=$PYTHONPATH:$PWD
python2 WMCore/WMRuntime/Startup.py

cp WMTaskSpace/Report.pkl ../

sleep 90

ls -l WMTaskSpace
ls -l WMTaskSpace/*

exit 0

