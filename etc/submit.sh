#!/bin/bash

# On some sites we know there was some problems with environment cleaning
# with using 'env -i'. To overcome this issue, whenever we start a job, we have
# to save full current environment into file, and whenever it is needed we can load
# it. Be aware, that there are some read-only variables, like: BASHOPTS, BASH_VERSINFO,
# EUID, PPID, SHELLOPTS, UID, etc.
set > startup_environment.sh
sed -e 's/^/export /' startup_environment.sh > tmp_env.sh
mv tmp_env.sh startup_environment.sh
export JOBSTARTDIR=$PWD


# Saving START_TIME and when job finishes, check if runtime is not lower than 20m
# If it is lower, sleep the difference. We don`t want to overload agents with too fast jobs...
START_TIME=$(date +%s)
function finish {
  END_TIME=$(date +%s)
  DIFF_TIME=$((END_TIME-START_TIME))
  echo "Job Running time in seconds: " $DIFF_TIME
  if [ $DIFF_TIME -lt 1200];
  then
    SLEEP_TIME=$((1200 - DIFF_TIME))
    echo "Job runtime is less than 20minutes. Sleeping " $SLEEP_TIME
    sleep $SLEEP_TIME
  fi
}
# Trap all exits and execute finish function
trap finish EXIT


# should be a bit nicer than before
echo "WMAgent bootstrap : `date -u` : starting..."

# We need to create the expected output file in advance just in case
# some problem happens during the job bootstrap
outputFile="Report.0.pkl Report.1.pkl Report.2.pkl Report.3.pkl"
if [ -n "$_CONDOR_JOB_AD" ]; then
    outputFile=`grep ^TransferOutput $_CONDOR_JOB_AD | awk -F'=' '{print $2}' | sed 's/\"//g'`
fi
if [ -z $outputFile ]; then
    outputFile="Report.0.pkl Report.1.pkl Report.2.pkl Report.3.pkl"
fi
touch $outputFile

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

if [ -e "$VO_CMS_SW_DIR"/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh ]
then
    . "$VO_CMS_SW_DIR"/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh
elif [ -e "$OSG_APP"/cmssoft/cms/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh ]
then
    . "$OSG_APP"/cmssoft/cms/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh
elif [ -e "$CVMFS"/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh ]
then
    . "$CVMFS"/COMP/slc6_amd64_gcc493/external/python/2.7.6/etc/profile.d/init.sh
else
    echo "WMAgent bootstrap : `date -u` : Error: OSG_APP, VO_CMS_SW_DIR, CVMFS, /cvmfs/cms.cern.ch environment does not contain init.sh" >&2
    echo "WMAgent bootstrap : `date -u` : Error: Because of this, we can't load CMSSW. Not good." >&2
    exit 4
fi
command -v python2 > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
    echo "WMAgent bootstrap : `date -u` : Error: Python2.6 isn't available on this worker node." >&2
    echo "WMAgent bootstrap : `date -u` : Error: WMCore/WMAgent REQUIRES python2" >&2
    exit 3	
else
    echo "WMAgent bootstrap : `date -u` : found python2 at.."
    echo `which python2`
fi

# Should be ready to unpack and run this
echo "WMAgent bootstrap : `date -u` : is unpacking the job..."
python2 Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export WMAGENTJOBDIR=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD/WMCore.zip:$PWD
echo "WMAgent bootstrap : `date -u` :    Hostname: `hostname -f`"
echo "WMAgent bootstrap : `date -u` :    Username: `id`"
echo "WMAgent bootstrap : `date -u` : Environemnt:"
env
echo "WMAgent bootstrap : `date -u` : WMAgent is now running the job..."
python2 Startup.py
jobrc=$?
echo "WMAgent bootstrap : `date -u` : WMAgent finished the job, is copying the pickled report"
cp WMTaskSpace/Report*.pkl ../
ls -l WMTaskSpace
ls -l WMTaskSpace/*
echo "WMAgent bootstrap : `date -u` : WMAgent is finished. The job had an exit code of $jobrc "
exit 0


