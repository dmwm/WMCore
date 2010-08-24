#!/bin/sh
if [ -z $REST_HOME ] ; then
   export REST_HOME=$PWD
   echo "Set REST_HOME='$REST_HOME'"
fi
export CVSROOT=:kserver:cmscvs.cern.ch:/cvs_server/repositories/CMSSW
export PYTHONPATH=$REST_HOME/:$PYTHONPATH
export PYTHONPATH=/Users/vk/CMS/WEBTOOLS:$PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/opt/local/lib/python2.5/site-packages
export PATH=$PATH:/Users/vk/CMS/WEBTOOLS/
