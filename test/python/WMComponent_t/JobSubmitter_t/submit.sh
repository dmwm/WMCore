#!/bin/bash


SANDBOX=$1
INDEX=$2

ls
. /uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/python/2.6.4-cms2/etc/profile.d/init.sh 
. /uscmst1/prod/sw/cms/setup/shrc prod
# BADPYTHON
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib
python2.6 Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export PYTHONPATH=$PYTHONPATH:$PWD
python2.6 WMCore/WMRuntime/Startup.py
if [ -f WMTaskSpace/cmsRun1/Report.pkl ]
then
  cp WMTaskSpace/cmsRun1/Report.pkl ../
fi
if [ -f WMTaskSpace/mergeReco/Report.pkl ]
then
  cp WMTaskSpace/mergeReco/Report.pkl ../
fi
if [ -f WMTaskSpace/mergeAlcaReco/Report.pkl ]
then
  cp WMTaskSpace/mergeAlcaReco/Report.pkl ../
fi

sleep 90

ls -l WMTaskSpace
ls -l WMTaskSpace/*

exit 0

