#!/bin/bash
# On some sites we know there were some problems with environment cleaning
# with using 'env -i'. To overcome this issue, whenever we start a job, we have
# to save full current environment into file, and whenever it is needed we can load
# it. Be aware, that there are some read-only variables, like: BASHOPTS, BASH_VERSINFO,
# EUID, PPID, SHELLOPTS, UID, etc.
set | sed 's/^/export /g' > startup_environment.sh

# Function to check the exit code of this bootstrap script and the job/python
# wrapper exit code.
# 1) If the bootstrap exit code is not 0, then something is wrong with the worker
#    node and this script will sleep for WMA_MIN_JOB_RUNTIMESECS before it exits.
# 2) If the job exit code is not 0, then again sleep for WMA_MIN_JOB_RUNTIMESECS
# 3) If all exit codes are 0, then just quit
finish() {
  exitCode=$?
  echo "======== WMAgent final job runtime checks STARTING at $(TZ=GMT date) ========"
  END_TIME=$(date +%s)
  DIFF_TIME=$((END_TIME-START_TIME))
  echo "$(TZ=GMT date): Job Runtime in seconds: " $DIFF_TIME
  echo "$(TZ=GMT date): Job bootstrap script exited: " $exitCode
  echo "$(TZ=GMT date): Job execution exited: " $jobrc

  if [ $exitCode -ne 0 ];
  then
    WMA_MIN_JOB_RUNTIMESECS=300
  elif [ $jobrc -eq 0 ];
  then
    WMA_MIN_JOB_RUNTIMESECS=0
  fi
  if [ $DIFF_TIME -lt $WMA_MIN_JOB_RUNTIMESECS ];
  then
    SLEEP_TIME=$((WMA_MIN_JOB_RUNTIMESECS - DIFF_TIME))
    echo "$(TZ=GMT date): Job runtime is less than $WMA_MIN_JOB_RUNTIMESECS seconds. Sleeping " $SLEEP_TIME
    sleep $SLEEP_TIME
  fi
  echo -e "======== WMAgent final job runtime checks FINISHED at $(TZ=GMT date) ========\n"
}

# Trap all exits and execute finish function
trap finish EXIT

# should be a bit nicer than before
echo "======== WMAgent bootstrap STARTING at $(TZ=GMT date) ========"
echo "User id:    $(id)"
echo "Local time: $(date)"
echo "Hostname:   $(hostname -f)"
echo "System:     $(uname -a)"
echo "Arguments:  $@"

WMA_SCRAM_ARCH=slc6_amd64_gcc493
# Saving START_TIME and when job finishes END_TIME.
WMA_MIN_JOB_RUNTIMESECS=300
START_TIME=$(date +%s)

export JOBSTARTDIR=$PWD

if [ "X$_CONDOR_JOB_AD" != "X" ];
then
   outputFile=`grep ^TransferOutput $_CONDOR_JOB_AD | awk -F'=' '{print $2}' | sed 's/\"//g' | sed 's/,/ /g'`
   WMA_SiteName=`grep '^MachineAttrGLIDEIN_CMSSite0 =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
   echo "Site name:  $WMA_SiteName"
   echo "======== HTCondor jobAds start at $(TZ=GMT date) ========"
   while read i; do
       echo "  $i"
   done < $_CONDOR_JOB_AD | sort
   echo -e "======== HTCondor jobAds finished at $(TZ=GMT date) ========\n"
fi

# We need to create the expected output files in advance just in case
# some problem happens during the job bootstrap
if [ -z "$outputFile" ]; then
    outputFile="Report.0.pkl Report.1.pkl Report.2.pkl Report.3.pkl wmagentJob.log"
fi
touch $outputFile


echo "======== WMAgent validate arguments starting at $(TZ=GMT date) ========"
if [ -z "$1" ]
then
    echo "Error during job bootstrap: A sandbox must be specified" >&2
    exit 11001
fi
if [ -z "$2" ]
then
    echo "Error during job bootstrap: A job index must be specified" >&2
    exit 11002
fi
# assign arguments
SANDBOX=$1
INDEX=$2
echo -e "======== WMAgent validate arguments finished at $(TZ=GMT date) ========\n"


echo "======== WMAgent CMS environment load starting at $(TZ=GMT date) ========"
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
    echo "Error during job bootstrap: VO_CMS_SW_DIR, OSG_APP, CVMFS  or /cvmfs were not found." >&2
    echo "  Because of this, we can't load CMSSW. Not good." >&2
    exit 11003
fi
echo "WMAgent bootstrap: WMAgent thinks it found the correct CMSSW setup script"
echo -e "======== WMAgent CMS environment load finished at $(TZ=GMT date) ========\n"


echo "======== WMAgent Python boostrap starting at $(TZ=GMT date) ========"
suffix=etc/profile.d/init.sh
if [ -d "$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python ]
then
    prefix="$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python
elif [ -d "$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python ]
then
    prefix="$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python
elif [ -d "$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python ]
then
    prefix="$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python
else
    echo "Error during job bootstrap: job environment does not contain the init.sh script." >&2
    echo "  Because of this, we can't load CMSSW. Not good." >&2
    exit 11004
fi

latestPythonVersion=`ls -t "$prefix"/*/"$suffix" | head -n1 | sed 's|.*/external/python/||' | cut -d '/' -f1`
pythonMajorVersion=`echo $latestPythonVersion | cut -d '.' -f1`
pythonCommand="python"${pythonMajorVersion}
echo "WMAgent bootstrap: latest python release is: $latestPythonVersion"
source "$prefix"/"$latestPythonVersion"/"$suffix"

command -v $pythonCommand > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
    echo "Error during job bootstrap: python isn't available on the worker node." >&2
    echo "  WMCore/WMAgent REQUIRES at least python2" >&2
    exit 11005
else
    echo "WMAgent bootstrap: found $pythonCommand at.."
    echo `which $pythonCommand`
fi
echo -e "======== WMAgent Python bootstrap finished at $(TZ=GMT date) ========\n"


echo "======== WMAgent Unpack the job starting at $(TZ=GMT date) ========"
# Should be ready to unpack and run this
$pythonCommand Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export WMAGENTJOBDIR=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD/WMCore.zip:$PWD
echo -e "======== WMAgent Unpack the job finished at $(TZ=GMT date) ========\n"

echo "======== Current environment dump starting ========"
for i in `env`; do
  echo "  $i"
done
echo -e "======== Current environment dump finished ========\n"

echo "======== WMAgent Run the job starting at $(TZ=GMT date) ========"
$pythonCommand Startup.py
jobrc=$?
echo -e "======== WMAgent Run the job FINISH at $(TZ=GMT date) ========\n"


echo "WMAgent bootstrap: WMAgent finished the job, it's copying the pickled report"
set -x
cp WMTaskSpace/Report*.pkl ../
ls -l WMTaskSpace
ls -l WMTaskSpace/*
set +x
echo -e "======== WMAgent bootstrap FINISH at $(TZ=GMT date) ========\n"
exit $jobrc
