#!/bin/sh

#
# Interactive script to setup and run a WMAgent job
#
JOB_SANDBOX=
JOB_PACKAGE=
JOB_INDEX=
JOB_JOBNAME=
WORKING_DIR=`pwd`

#
# Setup environment
#
if [ "x$VO_CMS_SW_DIR" == "x" ]; then
    echo "Environment Error: VO_CMS_SW_DIR is not set"
    exit 1
fi
if [ "x$SCRAM_ARCH" == "x" ]; then
    echo "SCRAM_ARCH not set, defaulting to slc6_amd64_gcc493"
    export SCRAM_ARCH=slc6_amd64_gcc493
fi
if [ "$SCRAM_ARCH" != "slc6_amd64_gcc493" ]; then
    echo "SCRAM_ARCH not set to slc6_amd64_gcc493 which could royally balls things up..."
    echo "lets see what happens..."
fi
if [ "x$WMCORE_ROOT" == "x" ]; then
    echo "Environment Error: WMCORE_ROOT is not set"
    exit 1
fi


function help(){
    echo "interacivejob.sh"
    echo " Mandatory arguments"
    echo "   -s <sandbox> Path to the Job Sandbox file"
    echo "   -p <package> Path to the Job Sandbox file"
    echo "   -j <jobname> The name of the job to be executed"
    echo "   -i <index> Job Index of the job to be executed in the Job Package file"
    echo " Optional arguments"
    echo "   -d <working dir> Unpack sandbox and execute job in specified dir, else work in pwd"
}


while [ $# -ge 1 ]; do
  case $1 in
    -i ) JOB_INDEX=$2; shift; shift ;;
    -j ) JOB_JOBNAME=$2; shift; shift ;;
    -p ) JOB_PACKAGE=$2; shift; shift ;;
    -s ) JOB_SANDBOX=$2; shift; shift ;;
    -d ) WORKING_DIR=$2; shift; shift ;;
    -h ) help; exit 0;;
    -* ) echo "$0: unrecognised option $1, use -h for help" 1>&2; exit 1 ;;
    *  ) break ;;
  esac
done

echo "JOB_INDEX=${JOB_INDEX}"
echo "JOB_PACKAGE=${JOB_PACKAGE}"
echo "JOB_SANDBOX=${JOB_SANDBOX}"
echo "JOB_JOBNAME=${JOB_JOBNAME}"

if [ "x$JOB_INDEX" == "x" ]; then
    echo "Argument Error: -i option, the job index not provided"
    exit 1
fi
if [ "x$JOB_PACKAGE" == "x" ]; then
    echo "Argument Error: -p option, the job package not provided"
    exit 1
fi

if [ "x$JOB_SANDBOX" == "x" ]; then
    echo "Argument Error: -s option, the job sandbox not provided"
    exit 1
fi

if [ "x$JOB_JOBNAME" == "x" ]; then
    echo "Argument Error: -j option, the job name not provided"
    exit 1
fi

if [ ! -e $JOB_PACKAGE ]; then 
    echo "Argument Error: Job package: $JOB_PACKAGE does not exist"
    exit 1
fi
if [ ! -e $JOB_SANDBOX ]; then 
    echo "Argument Error: Job sandbox: $JOB_SANDBOX does not exist"
    exit 1
fi

 . $VO_CMS_SW_DIR/COMP/slc6_amd64_gcc493/external/python/2.7.13/etc/profile.d/init.sh
#
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/$SCRAM_ARCH/external/openssl/1.0.1p/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/$SCRAM_ARCH/external/bz2lib/1.0.6/lib

UNPACKER_SCRIPT=${WMCORE_ROOT}/WMCore/WMRuntime/Unpacker.py

echo "Executing Job:"
echo "   WMCORE_ROOT=$WMCORE_ROOT"
echo "   SCRAM_ARCH=$SCRAM_ARCH"
echo "   WORKING_DIR=$WORKING_DIR"
echo "   Unpacker=$UNPACKER_SCRIPT"
echo "   Python Version:"
echo     `python2 -V`
echo "   package = $JOB_PACKAGE"
echo "   sandbox = $JOB_SANDBOX"
echo "   index   = $JOB_INDEX"
echo "   name    = $JOB_JOBNAME"

cd $WORKING_DIR
echo "Running Unpacker..."
python2 $UNPACKER_SCRIPT --sandbox=$JOB_SANDBOX \
                        --package=$JOB_PACKAGE \
                        --index=$JOB_INDEX \
                        --jobname=$JOB_JOBNAME

export PYTHONPATH=$PYTHONPATH:$WORKING_DIR/job
cd $WORKING_DIR/job
echo "Running job..."
python2 WMCore/WMRuntime/Startup.py







