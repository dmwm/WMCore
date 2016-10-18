#!/bin/bash
# should be a bit nicer than before
echo "======== WMAgent bootstrap STARTING at $(TZ=GMT date) ========"
echo "User id:    $(id)"
echo "Local time: $(date)"
echo "Hostname:   $(hostname -f)"
echo "System:     $(uname -a)"
echo "Arguments:  $@"

# Saving START_TIME and when job finishes END_TIME.
WMA_MIN_JOB_RUNTIMESECS=300
START_TIME=$(date +%s)

# On some sites we know there was some problems with environment cleaning
# with using 'env -i'. To overcome this issue, whenever we start a job, we have
# to save full current environment into file, and whenever it is needed we can load
# it. Be aware, that there are some read-only variables, like: BASHOPTS, BASH_VERSINFO,
# EUID, PPID, SHELLOPTS, UID, etc.
set > startup_environment.sh
sed -e 's/^/export /' startup_environment.sh > tmp_env.sh
mv tmp_env.sh startup_environment.sh
export JOBSTARTDIR=$PWD


# Multiple checks are done:
# 1) Check submit.sh bootstrap exit code.
#    If it is not 0 sleep WMA_MIN_JOB_RUNTIMESECS and in case it is 0, force to sleep 5 mins total runtime
#    If this bootstrap script is exiting non 0 exit code, something is wrong on this WN...
# 2) If Job exited 0, do not sleep any timeout
# 3) Otherwise, sleep up to WMA_MIN_JOB_RUNTIMESECS seconds of runtime.
function finish {
  exitCode=$?
  echo "======== WMAgent final job runtime checks STARTING at $(TZ=GMT date) ========"
  if [ $exitCode -ne 0 ];
  then
      if [ $WMA_MIN_JOB_RUNTIMESECS -eq 0 ];
      then
          WMA_MIN_JOB_RUNTIMESECS=300
      fi
  elif [ $jobrc -eq 0 ];
  then
      WMA_MIN_JOB_RUNTIMESECS=0
  fi
  END_TIME=$(date +%s)
  DIFF_TIME=$((END_TIME-START_TIME))
  echo "$(TZ=GMT date): Job Runtime in seconds: " $DIFF_TIME
  echo "$(TZ=GMT date): Job bootstrap script exited: " $exitCode
  echo "$(TZ=GMT date): Job execution exited: " $jobrc
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


if [ "X$_CONDOR_JOB_AD" != "X" ];
then
   outputFile=`grep ^TransferOutput $_CONDOR_JOB_AD | awk -F'=' '{print $2}' | sed 's/\"//g'`
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
    outputFile="Report.0.pkl Report.1.pkl Report.2.pkl Report.3.pkl"
fi
touch $outputFile


echo "======== WMAgent validate arguments starting at $(TZ=GMT date) ========"
if [ -z "$1" ]
then
    echo "WMAgent bootstrap: Error: A sandbox must be specified" >&2
    exit 1
fi
if [ -z "$2" ]
then
    echo "WMAgent bootstrap: Error: An index must be specified" >&2
    exit 1
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
    echo "WMAgent bootstrap: Error: VO_CMS_SW_DIR, OSG_APP, CVMFS environment variables were not set and /cvmfs is not present" >&2
    echo "WMAgent bootstrap: Error: Because of this, we can't load CMSSW. Not good." >&2
    exit 2
fi
echo "WMAgent bootstrap: WMAgent thinks it found the correct CMSSW setup script"
echo -e "======== WMAgent CMS environment load finished at $(TZ=GMT date) ========\n"


echo "======== WMAgent Python boostrap starting at $(TZ=GMT date) ========"
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
    echo "WMAgent bootstrap: Error: OSG_APP, VO_CMS_SW_DIR, CVMFS, /cvmfs/cms.cern.ch environment does not contain init.sh" >&2
    echo "WMAgent bootstrap: Error: Because of this, we can't load CMSSW. Not good." >&2
    exit 4
fi
command -v python2 > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
    echo "WMAgent bootstrap: Error: python2 isn't available on this worker node." >&2
    echo "WMAgent bootstrap: Error: WMCore/WMAgent REQUIRES python2" >&2
    exit 3	
else
    echo "WMAgent bootstrap: found python2 at.."
    echo `which python2`
fi
echo -e "======== WMAgent Python boostrap finished at $(TZ=GMT date) ========\n"


echo "======== WMAgent Unpack the job starting at $(TZ=GMT date) ========"
# Should be ready to unpack and run this
python2 Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

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
python2 Startup.py
jobrc=$?
echo -e "======== WMAgent Run the job FINISH at $(TZ=GMT date) ========\n"


echo "WMAgent bootstrap: WMAgent finished the job, it's copying the pickled report"
cp WMTaskSpace/Report*.pkl ../
ls -l WMTaskSpace
ls -l WMTaskSpace/*
echo -e "======== WMAgent bootstrap FINISH at $(TZ=GMT date) ========\n"
exit 0
