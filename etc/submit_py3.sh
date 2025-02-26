#!/bin/bash
#
# acquire initial timestamps for reporting in FJR
timeStartSec=`date +%s`

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

# Python library required for Python2/Python3 compatibility through "future"
PY3_FUTURE_VERSION=0.18.2

# Saving START_TIME and when job finishes END_TIME.
WMA_MIN_JOB_RUNTIMESECS=300
START_TIME=$(date +%s)
WMA_DEFAULT_OS=rhel8
WMA_CURRENT_OS=rhel$(rpm --eval '%{rhel}')
[[ $WMA_CURRENT_OS =~ ^rhel[6789]?$ ]] || WMA_CURRENT_OS=$WMA_DEFAULT_OS
# assign arguments
SANDBOX=$1
INDEX=$2
RETRY_NUM=$3

export JOBSTARTDIR=$PWD

if [ "X$_CONDOR_JOB_AD" != "X" ];
then
   WMA_SiteName=`grep '^MachineAttrGLIDEIN_CMSSite0 =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
   echo "Site name:  $WMA_SiteName"
   echo "======== HTCondor jobAds start at $(TZ=GMT date) ========"
   while read i; do
       echo "  $i"
   done < $_CONDOR_JOB_AD | sort
   echo -e "======== HTCondor jobAds finished at $(TZ=GMT date) ========\n"
fi

# We need to create the expected output file in advance, just in case
# some problem happens during the job bootstrap
outputFile="Report.$RETRY_NUM.pkl"
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

echo -e "======== WMAgent validate arguments finished at $(TZ=GMT date) ========\n"


echo "======== WMAgent CMS environment load starting at $(TZ=GMT date) ========"
if [ -f "$VO_CMS_SW_DIR"/cmsset_default.sh ]
then  #   LCG style --
    echo "WN with a LCG style environment, thus using VO_CMS_SW_DIR=$VO_CMS_SW_DIR"
    . $VO_CMS_SW_DIR/cmsset_default.sh
elif [ -f "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]
then  #   OSG style --
    echo "WN with an OSG style environment, thus using OSG_APP=$OSG_APP"
    . $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
elif [ -f "$CVMFS"/cms.cern.ch/cmsset_default.sh ]
then
    echo "WN with CVMFS environment, thus using CVMFS=$CVMFS"
    . $CVMFS/cms.cern.ch/cmsset_default.sh
elif [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]
then  # ok, lets call it CVMFS then
    export CVMFS=/cvmfs/cms.cern.ch
    echo "WN missing VO_CMS_SW_DIR/OSG_APP/CVMFS environment variable, forcing it to CVMFS=$CVMFS"
    . $CVMFS/cmsset_default.sh
else
    echo "Error during job bootstrap: VO_CMS_SW_DIR, OSG_APP, CVMFS or /cvmfs were not found." >&2
    echo "  Because of this, we can't load CMSSW. Not good." >&2
    exit 11003
fi
echo "WMAgent bootstrap: WMAgent thinks it found the correct CMSSW setup script"
echo -e "======== WMAgent CMS environment load finished at $(TZ=GMT date) ========\n"


echo "======== WMAgent COMP Python bootstrap starting at $(TZ=GMT date) ========"
### This is a bit messy! We do not have all the necessary COMP packages under all
# the CMSSW ScramArch. So, for many ScramArchs not supported by COMP, what we did
# was to make a symlink in CVMFS to packages provided under a given CMSSW branch.
#
### A summary of our current setup is:
# slc7_amd64_gcc630 (standard COMP): py2-future/0.18.2 python/2.7.13 py3-future/0.18.2 python3/3.8.2
# slc6_amd64_gcc700 (from CMSSW): py2-future/0.16.0 py3-future/0.18.2 python3/3.6.4
# slc7_ppc64le_gcc630 (from CMSSW): py2-future/0.18.2 python/2.7.15 py3-future/0.18.2 python3/3.8.2
# slc6_ppc64le_gcc493 (from CMSSW): py2-future/0.18.2 python/2.7.15 py3-future/0.18.2 python3/3.8.2
#
# NOTE: all the ppc64le ScramArchs are actually pointing to: slc7_ppc64le_gcc820
### UPDATE on 11 April, 2022: See a new map of CVMFS packages in:
# https://github.com/dmwm/WMCore/pull/11077#issuecomment-1094814966

# First, decide which COMP ScramArch to use based on the required OS and Architecture
THIS_ARCH=`uname -m`  # if it's PowerPC, it returns `ppc64le`
# if this job can run at any OS, then try to run it on the OS discovered at runtime
if [ "$REQUIRED_OS" = "any" ]
then
    WMA_SCRAM_ARCH=${WMA_CURRENT_OS}_${THIS_ARCH}
else
    WMA_SCRAM_ARCH=${REQUIRED_OS}_${THIS_ARCH}
fi
echo "Job requires OS: $REQUIRED_OS, thus setting ScramArch to: $WMA_SCRAM_ARCH"

suffix=etc/profile.d/init.sh
if [ -d "$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python3 ]
then
    prefix="$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python3
elif [ -d "$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python3 ]
then
    prefix="$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python3
elif [ -d "$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python3 ]
then
    prefix="$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python3
else
    echo "Failed to find a COMP python3 installation in the worker node setup." >&2
    echo "  Without a known python3, there is nothing else we can do with this job. Quiting!" >&2
    exit 11004
fi

compPythonPath=`echo $prefix | sed 's|/python3||'`
echo "WMAgent bootstrap: COMP Python path is: $compPythonPath"
latestPythonVersion=`ls -t "$prefix"/*/"$suffix" | head -n1 | sed 's|.*/external/python3/||' | cut -d '/' -f1`
pythonMajorVersion=`echo $latestPythonVersion | cut -d '.' -f1`
pythonCommand="python"${pythonMajorVersion}
echo "WMAgent bootstrap: latest python3 release is: $latestPythonVersion"
source "$prefix/$latestPythonVersion/$suffix"
echo "Sourcing python future library from: ${compPythonPath}/py3-future/${PY3_FUTURE_VERSION}/${suffix}"
source "$compPythonPath/py3-future/${PY3_FUTURE_VERSION}/${suffix}"

command -v $pythonCommand > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
    echo "Error during job bootstrap: python3 isn't available on the worker node." >&2
    echo "  WMCore/WMAgent REQUIRES python3" >&2
    exit 11005
else
    echo "WMAgent bootstrap: found $pythonCommand at.."
    echo `which $pythonCommand`
fi
echo -e "======== WMAgent Python bootstrap finished at $(TZ=GMT date) ========\n"

echo -e "======= WMAgent token verification at $(TZ=GMT date) ========\n"
if [ -n "${_CONDOR_CREDS}" ]; then
    echo "Content under _CONDOR_CREDS: ${_CONDOR_CREDS}"
    ls -l ${_CONDOR_CREDS}
    # Now, check specifically for cms token
    if [ -f "${_CONDOR_CREDS}/cms.use" ]
    then
        echo "CMS token found, setting BEARER_TOKEN_FILE=${_CONDOR_CREDS}/cms.use"
        export BEARER_TOKEN_FILE=${_CONDOR_CREDS}/cms.use
    
        # Show token information
        # This tool requires htgettoken package in the cmssw runtime apptainer image
        if command -v httokendecode ls 2>&1 > /dev/null
        then
            httokendecode -H ${BEARER_TOKEN_FILE}
        else
            echo "Warning: [WMAgent Token verification] httokendecode tool could not be found."
            echo "Warning: Token exists and can be used, but details will not be displayed."
        fi
    else
        echo "[WMAgent token verification]: The bearer token file could not be found."
        # Do not fail, we still support x509 proxies
        # if we fail here in the future, we need to define an exit code number
        # exit 1106
    fi
else
    echo "Variable _CONDOR_CREDS is not defined, condor auth/token credentials directory not found."
fi

echo "======== WMAgent Unpack the job starting at $(TZ=GMT date) ========"
# Should be ready to unpack and run this
$pythonCommand Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export WMAGENTJOBDIR=$PWD
export PYTHONPATH=$PYTHONPATH:$WMAGENTJOBDIR/WMCore.zip:$WMAGENTJOBDIR
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

# add WMTiming metrics to FJR pkl file
echo "======== WMAgent add WMTiming metrics to FJR $outputFile ========"
timeEndSec=`date +%s`
tpy=$HOME/job/Utils/Timestamps.py
reportIn=WMTaskSpace/$outputFile
echo -e "$pythonCommand $tpy --reportFile=$reportIn --wmJobEnd=$timeEndSec --wmJobStart=$timeStartSec"
$pythonCommand $tpy --reportFile=$reportIn --wmJobEnd=$timeEndSec --wmJobStart=$timeStartSec
status=$?
if [ "$status" != "0" ]; then
    echo "WARNING: failed to update FJR with timestamps metrics, status=$status"
fi
echo -e "======== WMAgent finished adding WMTiming metrics to FJR ========\n"

echo "WMAgent bootstrap: WMAgent finished the job, it's copying the pickled report"
set -x
cp WMTaskSpace/$outputFile ../
ls -l WMTaskSpace
ls -l WMTaskSpace/*

set +x
echo -e "======== WMAgent bootstrap FINISH at $(TZ=GMT date) ========\n"
exit $jobrc
