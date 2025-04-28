#!/bin/bash

usage()
{
    echo -e "\nA simple script to facilitate component patching"
    echo -e "and to decrease the development && testing turnaround time."
    echo -e "  * A list of upstream PRs (the order of applying them matters) or "
    echo -e "  * A patch file provided or"
    echo -e "  * A patch created from StdIn"
    echo -e ""
    echo -e "Usage: ./patchComponent [-n] [-z] [-f <patchFile>] <patchNum> <patchNum> ..."
    echo -e "       -z - Only zero the code base to the currently deployed tag for the files changed in the patch - no actual patches will be applied"
    echo -e "       -f - Apply the specified patch file. No multiple files supported. If opt is repeated only the last one will be considered."
    echo -e "       -n - Do not zero the code base neither from TAG nor from Master branch, just apply the patch"
    echo -e "       -o - One go - Apply the patch only through a single attempt starting from the tag deployed at the destination and avoid the second attempt upon syncing to master."
    echo -e ""
    echo -e "NOTE: We do not support patching from file and patching from command line simultaneously"
    echo -e "       If both provided at the command line patching from command line takes precedence"
    echo -e ""
    echo -e "Examples: \n"
    echo -e "       sudo ./patchComponent.sh 11270 12120"
    echo -e "       sudo ./patchComponent.sh -f /tmp/11270.patch"
    echo -e "       git diff --no-color | sudo ./patchComponent.sh"
    echo -e "       curl https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/11270.patch | sudo ./patchComponent.sh \n"
}



# Add default value for zeroOnly option
zeroOnly=false
zeroCodeBase=true
oneGo=false
extPatchFile=""
while getopts ":f:znoh" opt; do
    case ${opt} in
        f)
            extPatchFile=$OPTARG
            ;;
        z)
            zeroOnly=true
            ;;
        n)
            zeroCodeBase=false
            ;;
        o)
            oneGo=true
            ;;
        h)
            usage
            exit 0 ;;
        \? )
            echo -e "\nERROR: Invalid Option: -$OPTARG\n"
            usage
            exit 1 ;;
        : )
            echo -e "\nERROR: Invalid Option: -$OPTARG requires an argument\n"
            usage
            exit 1 ;;
    esac
done
# shift to the last  parsed option, so we can consume the patchNum with a regular shift
shift $(expr $OPTIND - 1 )


# if fd 0 (stdin) is open and refers to a terminal - then we are running the script directly, without a pipe
# if fd 0 (stdin) is open but does not refer to the terminal - then we are running the script through a pipe
if [ -t 0 ] ; then pipe=false; else pipe=true ; fi

patchList=$*
# [[ -z $patchList ]] && patchList="temp"

currTag=$(python -c "from WMCore import __version__ as WMCoreVersion; print(WMCoreVersion)")

echo
echo
echo
echo ========================================================
echo "INFO: Current WMCoreTag: $currTag"

# Find all possible locations for the component source
# NOTE: We always consider PYTHONPATH first
pythonLibPaths=$(echo $PYTHONPATH |sed -e "s/\:/ /g")
pythonLibPaths="$pythonLibPaths $(python -c "import sysconfig; print(sysconfig.get_path('purelib'))")"

for path in $pythonLibPaths
do
    [[ -d $path/WMCore ]] && { pythonLibPath=$path; echo "INFO: Source code found at: $path"; break ;}
done

[[ -z $pythonLibPath  ]] && { echo "ERROR: Could not find WMCore source to patch"; exit  1 ;}
echo "INFO: Current PythonLibPath: $pythonLibPath"
echo --------------------------------------------------------
echo

# Set patch command parameters
stripLevel=3
patchCmd="patch -t --verbose -b --version-control=numbered -d $pythonLibPath -p$stripLevel"

# Define Auxiliary functions
_createTestFilesDst() {
    # A simple function to create test files destination for not breaking the patches
    # because of a missing destination:
    # :param $1:   The source branch to be used for checking the files: could be TAG or Master
    # :param $2-*: The list of files to be checked out
    local srcBranch=$1
    shift
    local testFileList=$*
    for file in $testFileList
    do
        # file=${file#a\/test\/python\/}
        fileName=`basename $file`
        fileDir=`dirname $file`
        # Create the file path if missing
        mkdir -p $pythonLibPath/$fileDir
        echo INFO: orig: https://raw.githubusercontent.com/dmwm/WMCore/$srcBranch/test/python/$file
        echo INFO: dest: $pythonLibPath/$file
        curl -f https://raw.githubusercontent.com/dmwm/WMCore/$srcBranch/test/python/$file  -o $pythonLibPath/$file || {
            echo INFO: file: $file missing at the origin.
            echo INFO: Seems to be a new file for the curren patch.
            echo INFO: Removing it from the destination as well!
            rm -f $pythonLibPath/$file
        }
    done
}

_zeroCodeBase() {
    # A simple function to zero the code base for a set of files starting from
    # a given tag or branch at the origin
    # :param $1:   The source branch to be used for checking the files: could be TAG or Master
    # :param $2-*: The list of files to be checked out
    local srcBranch=$1
    shift
    local srcFileList=$*
    for file in $srcFileList
    do
        # file=${file#a\/src\/python\/}
        fileName=`basename $file`
        fileDir=`dirname $file`
        # Create the file path if missing
        mkdir -p $pythonLibPath/$fileDir
        echo INFO: orig: https://raw.githubusercontent.com/dmwm/WMCore/$srcBranch/src/python/$file
        echo INFO: dest: $pythonLibPath/$file
        curl -f https://raw.githubusercontent.com/dmwm/WMCore/$srcBranch/src/python/$file  -o $pythonLibPath/$file || {
            echo INFO: file: $file missing at the origin.
            echo INFO: Seems to be a new file for the curren patch.
            echo INFO: Removing it from the destination as well!
            rm -f $pythonLibPath/$file
        }
    done
}


# DONE: ....HERE TO START ITERATING THROUGH THE PATCH LIST

# Create the full list of patch files to be applied - keeping the order
# from the original patch list as provided at the command line
patchFileList=""
_createPatchFiles(){
    local patchFile

    # Check if we are running from a pipe
    $pipe && {
        if $zeroOnly ;then
            echo "INFO: Zeroing WMCore code base from StdIn"
        else
            echo "INFO: Patching WMCore code from StdIn"
        fi
        patchFile="/tmp/pipeTmp_$(id -u).patch"
        patchFileList=$patchFile
        echo "INFO: Creating a temporary patchFile from stdin at: $patchFile"
        cat <&0 > $patchFile
        return
    }

    # Check if we were sent a file to patch from
    [[ -n $extPatchFile ]] && {
        if $zeroOnly ;then
            echo "INFO: Zeroing WMCore code base with file: $extPatchFile"
        else
            echo "INFO: Patching WMCore code with file: $extPatchFile"
        fi
        patchFile=$extPatchFile
        patchFileList=$patchFile
        echo "INFO: Using command line provided patch file: $patchFile"
        return
    }

    # Finally, if none of the above, build the list of patch files to be applied from the patchNums provided at the command line
    if $zeroOnly ; then
        echo "INFO: Zeroing WMCore code base with PRs: $patchList"
    else
        echo "INFO: Patching WMCore code with PRs: $patchList"
    fi
    for patchNum in $patchList
    do
        patchFile=/tmp/$patchNum.patch
        patchFileList="$patchFileList $patchFile"
        echo "INFO: Downloading a temporary patchFile at: $patchFile"
        curl https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/$patchNum.patch -o $patchFile
    done
}

_warnFilelist(){
    echo WARNING: Please consider checking the follwoing list of files for eventual code conflicts:
    for file in $srcFileList $testFileList
    do
        echo WARNING: $pythonLibPath/$file
    done
}

_createPatchFiles

echo "DEBUG: patchFileList: $patchFileList"

# Build full lists of files altered by the given set of patch files to be applied
srcFileList=""
testFileList=""
for patchFile in $patchFileList
do
    # Parse a list of files changed only by the current patch
    srcFileListTemp=`grep diff $patchFile |grep "a/src/python" |awk '{print $3}' |sort |uniq`
    testFileListTemp=`grep diff $patchFile |grep "a/test/python" |awk '{print $3}' |sort |uniq`

    # Reduce paths for both src and test file lists to the path depth known to
    # the WMCore modules/packages and add them to the global scope file lists
    for file in $srcFileListTemp
    do
        file=${file#a\/src\/python\/} && srcFileList="$srcFileList $file"
    done

    for file in $testFileListTemp
    do
        file=${file#a\/test\/python\/} && testFileList="$testFileList $file"
    done
done


$zeroCodeBase && {
    echo
    echo --------------------------------------------------------
    echo "INFO: Refreshing all files which are to be patched from the origin and TAG: $currTag"
    echo

    # First create destination for test files from currTag if missing
    _createTestFilesDst $currTag $testFileList


    # Then zero code base for source files from currTag
    _zeroCodeBase $currTag $srcFileList
}

# exit if the user has requested to only zero the code base
$zeroOnly && {  _warnFilelist; exit ;}

err=0
echo
echo
echo --------------------------------------------------------
echo "INFO: Patching all files starting from the $($zeroCodeBase && echo original version of TAG: $currTag || echo current version of files)"
for patchFile  in $patchFileList
do
    echo
    echo
    echo --------------------------------------------------------
    echo "INFO: ----------------- Currently applying patch: $patchNum -----------------"
    echo "INFO: cat $patchFile | $patchCmd"
    cat $patchFile | $patchCmd
    let err+=$?
done

echo
echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
echo
if [[ $err -eq 0 ]]; then
    echo INFO: First patch attempt exit status: $err
    echo INFO: ALL GOOD
    echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    exit
else
    echo WARNING: First patch attempt exit status: $err
    echo
    echo
    echo WARNING: There were errors while patching from TAG: $currTag
    echo WARNING: Most probably some of the files from the current patch were having changes
    echo WARNING: between the current PR and the tag deployed at the host/container.
    echo
    echo

    # exit at this stage if the user has requested to patch without zeroing the code base
    $zeroCodeBase || { _warnFilelist ; exit 1 ;}

    # exit at this stage if the user has requested to do the patch only in one go
    # without a second attempt from master. Restore files to their version at TAG
    $oneGo && {
        echo WARNING: Restoring all files to their original version at TAG: $currTag
        echo
        echo
        _createTestFilesDst $currTag $testFileList
        _zeroCodeBase $currTag $srcFileList
        echo
        echo
        echo WARNING: All files have been rolled back to their original version at TAG: $currTag
        _warnFilelist
        exit 1
    }
    echo WARNING: TRYING TO START FROM ORIGIN/MASTER BRANCH INSTEAD:
    echo
    echo
fi


# If we are here it means something went wrong while patching some of the files.
# Most probably some of the files are having changes between the current PR and the tag deployed.
# What we can do in such cases is to try to fetch and zero the code base for those files
# to be patched from master and hope there are no conflicts in the PR.

echo
echo --------------------------------------------------------
echo "WARNING: Refreshing all files which are to be patched from origin/master branch:"
echo

# First create destination for test files from master if missing
_createTestFilesDst "master" $testFileList

# Then zero code base for source files from master
_zeroCodeBase "master" $srcFileList

err=0
echo
echo
echo --------------------------------------------------------
echo "WARNING: Patching all files starting from origin/master branch"
for patchFile in $patchFileList
do
    echo
    echo
    echo "WARNING: --------------- Currently applying patch: $patchNum ---------------"
    echo "WARNING: cat $patchFile | $patchCmd"
    cat $patchFile | $patchCmd
    let err+=$?
done


echo
echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
echo WARNING: Second patch attempt exit status: $err
echo
echo

[[ $err -eq 0 ]] || {

    _createTestFilesDst $currTag $testFileList
    _zeroCodeBase $currTag $srcFileList

    echo
    echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    echo WARNING: There were errors while patching from master branch as well
    echo WARNING: All files have been rolled back to their original version at TAG: $currTag
    echo
    echo
}
echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

_warnFilelist

exit $err
