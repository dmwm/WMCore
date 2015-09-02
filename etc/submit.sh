#!/bin/bash

touch Report.pkl

# should be a bit nicer than before
echo "WMAgent bootstrap : `date -u` : starting..."

# validate arguments
if [ -z "$1" ]
then
    echo "WMAgent bootstrap : `date -u` : Error: A sandbox must be specified" >&2
    exit 1
fi
if [ -z "$2" ]
then
    echo "WMAgent bootstrap : `date -u` : Error: An index must be specified" >&2
    exit 1
fi

# assign arguments
SANDBOX=$1
INDEX=$2
echo "WMAgent bootstrap : `date -u` : arguments validated..."

### source the CMSSW stuff using either OSG or LCG style entry env. variables
if [ -f "$VO_CMS_SW_DIR"/cmsset_default.sh ]
then  #   LCG style --
    . $VO_CMS_SW_DIR/cmsset_default.sh
elif [ -f "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]
then  #   OSG style --
    . $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
elif [ -f "$CVMFS"/cms.cern.ch/cmsset_default.sh ]
then
    . $CVMFS/cms.cern.ch/cmsset_default.sh
elif [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]
then  # ok, lets call it CVMFS then
    export CVMFS=/cvmfs/cms.cern.ch
    . $CVMFS/cmsset_default.sh
else
    echo "WMAgent bootstrap : `date -u` : Error: VO_CMS_SW_DIR, OSG_APP, CVMFS environment variables were not set and /cvmfs is not present" >&2
    echo "WMAgent bootstrap : `date -u` : Error: Because of this, we can't load CMSSW. Not good." >&2
    exit 2
fi

echo "WMAgent bootstrap : `date -u` : WMAgent thinks it found the correct CMSSW setup script"

if [ -e "$VO_CMS_SW_DIR"/COMP/slc6_amd64_gcc481/external/python/2.6.8-comp9/etc/profile.d/init.sh ]
then
    . "$VO_CMS_SW_DIR"/COMP/slc6_amd64_gcc481/external/python/2.6.8-comp9/etc/profile.d/init.sh
elif [ -e "$OSG_APP"/cmssoft/cms/COMP/slc6_amd64_gcc481/external/python/2.6.8-comp9/etc/profile.d/init.sh ]
then
    . "$OSG_APP"/cmssoft/cms/COMP/slc6_amd64_gcc481/external/python/2.6.8-comp9/etc/profile.d/init.sh
elif [ -e "$CVMFS"/COMP/slc6_amd64_gcc481/external/python/2.6.8-comp9/etc/profile.d/init.sh ]
then
    . "$CVMFS"/COMP/slc6_amd64_gcc481/external/python/2.6.8-comp9/etc/profile.d/init.sh
else
    echo "WMAgent bootstrap : `date -u` : Error: OSG_APP, VO_CMS_SW_DIR, CVMFS, /cvmfs/cms.cern.ch environment does not contain init.sh" >&2
    echo "WMAgent bootstrap : `date -u` : Error: Because of this, we can't load CMSSW. Not good." >&2
    exit 4
fi
command -v python2.6 > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
    echo "WMAgent bootstrap : `date -u` : Error: Python2.6 isn't available on this worker node." >&2
    echo "WMAgent bootstrap : `date -u` : Error: WMCore/WMAgent REQUIRES python2.6" >&2
    exit 3	
else
    echo "WMAgent bootstrap : `date -u` : found python2.6 at.."
    echo `which python2.6`
fi

# Should be ready to unpack and run this
echo "WMAgent bootstrap : `date -u` : is unpacking the job..."
python2.6 Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export WMAGENTJOBDIR=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD/WMCore.zip:$PWD
echo "WMAgent bootstrap : `date -u` :    Hostname: `hostname -f`"
echo "WMAgent bootstrap : `date -u` :    Username: `id`"
echo "WMAgent bootstrap : `date -u` : Environemnt:"
env
echo "WMAgent bootstrap : `date -u` : WMAgent is now running the job..."
python2.6 Startup.py
jobrc=$?
echo "WMAgent bootstrap : `date -u` : WMAgent finished the job, is copying the pickled report"
cp WMTaskSpace/Report*.pkl ../
ls -l WMTaskSpace
ls -l WMTaskSpace/*
echo "WMAgent bootstrap : `date -u` : WMAgent is finished. The job had an exit code of $jobrc "
exit 0


