#!/bin/bash

touch Report.pkl

# should be a bit nicer than before
echo "WMAgent bootstrap starting..."
# validate arguments

if [ "x" = "x$1" ]
then
	echo "WMAgent Error: A sandbox must be specified" >&2
	exit 1
fi

if [ "x" = "x$2" ]
then
	echo "WMAgent Error: An index must be specified" >&2
	exit 1
fi

# assign arguments

SANDBOX=$1
INDEX=$2
echo "WMAgent arguments validated..."
# debugging, I guess
ls


# 2 steps for python2.6 bootstrapping -- first, see if it's there already
command -v python2.6 > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
	if [ -e $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh ]
	then
		. $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh 
		export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib
	else
		echo "WMAgent Error: Python2.6 isn't available on this worker node." >&2
		echo "WMAgent Error: WMCore/WMAgent REQUIRES python2.6" >&2
		exit 3
	fi
else
	echo "WMAgent found python2.6 without having to try"
fi


### source the CMSSW stuff using either OSG or LCG style entry env. variables
###    (incantations per oli's instructions)
#   LCG style --
if [ "x" != "x$VO_CMS_SW_DIR" ]
then
	. $VO_CMS_SW_DIR/cmsset_default.sh

#   OSG style --
elif [ "x" != "x$OSG_APP" ]
then
	. $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
else
	echo "WMAgent Error: neither OSG_APP nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "WMAgent Error: Because of this, we can't load CMSSW. Not good." >&2
	exit 2
fi
echo "WMAgent thinks it found the correct CMSSW setup script"

# Should be ready to unpack and run this
echo "WMAgent is unpacking the job..."
python2.6 Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export PYTHONPATH=$PYTHONPATH:$PWD
echo "WMAgent is now running the job..."
python2.6 WMCore/WMRuntime/Startup.py
jobrc=$?
echo "WMAgent finished the job, is copying the pickled report"
cp WMTaskSpace/Report.pkl ../
ls -l WMTaskSpace
ls -l WMTaskSpace/*
echo "WMAgent is finished. The job had an exit code of $jobrc "
exit 0

