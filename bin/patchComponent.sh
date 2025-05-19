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

# Add default WMCore repository links
WMPatchUrl="https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull"
WMRawUrl="https://raw.githubusercontent.com/dmwm/WMCore"

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

# Figure out if we are running from source
runFromSource=false
[[ $pythonLibPath =~ .*srv/WMCore/src/python ]] && runFromSource=true

# Find the toplevel for the current deployment:
$runFromSource && toplevel=`realpath $pythonLibPath/../../../../` || toplevel=`realpath $pythonLibPath/../../../`


# Define all auxiliary functions:

_patchSingle() {
    # Auxiliary function to apply a patch file containing a single source file change
    # The patch command parameters like stripLevel and destination are supposed
    # to be correctly estimated based on the source file in the patch and the WMCore
    # runtime environment (e.g. WMCore@K8 or WMAgent@Docker or WMAgent@Venv)
    # :param $1: The path to the patch file to be applied
    local patchFile=$1
    local stripLevel=""
    local dest=""
    local patchcmd=""

    # Find the source file to which the current patchFile relates
    local srcFile=`grep -m 1 ^diff $patchFile |awk '{print $3}'`
    srcFile=${srcFile#a\/} && srcFile=${srcFile#b\/}
    echo INFO: $FUNCNAME: patchFile: $patchFile srcFile: $srcFile

    # Set patch command parameters

    # Search if the srcFile is contained in the $toplevFileList
    [[ $toplevFileList =~ $srcFile ]] && {
        echo INFO: $FUNCNAME: Found $BASH_REMATCH in the toplevFileList
        stripLevel=1
        dest=$toplevel
    }

    # Search if the srcFile is contained in the $staticFileList
    [[ $staticFileList =~ $srcFile ]] && {
        echo INFO: $FUNCNAME: Found $BASH_REMATCH in the staticFileList
        stripLevel=2
        dest=$toplevel/data
    }

    # Search if the srcFile is contained in the $testFileList
    [[ $testFileList =~ $srcFile ]] && {
        echo INFO: $FUNCNAME: Found $BASH_REMATCH in the testFileList
        stripLevel=1
        dest=$toplevel
    }

    # Search if the srcFile is contained in the $srcFileList
    [[ $srcFileList =~ $srcFile ]] && {
        echo INFO: $FUNCNAME: Found $BASH_REMATCH in the srcFileList
        stripLevel=3
        dest=$pythonLibPath
    }

    # Forge patchCmd:
    # patchCmd="patch -t --verbose -b --version-control=numbered -d $pythonLibPath -p$stripLevel"
    patchCmd="patch -t --verbose -b --version-control=numbered -d $dest -p$stripLevel"
    echo "INFO: $FUNCNAME: cat $patchFile | $patchCmd"

    # Apply the patch:
    # NOTE: This must be the last line to execute in order to properly return
    #       the error code from the attempted patch
    cat $patchFile | $patchCmd
}

_splitPatchByFiles() {
    # Auxiliary function to split a patch file by different files modified in the patch
    # creating one patch file per file change in the code
    # :param $1:  The path to the patch file to be split
    # NOTE:       The output are to be files named as <patchFileName_[0-9][0-9].patch>
    #             and will be placed at /tmp/patchFileName.d/
    # NOTE:       It echos only the splitDir so that the destination of the split files
    #             can be caught by the caller with the constrict:
    #             `local splitDir=$(_splitPatchByfiles $patchFile)`
    #             The return code, though, would still be the return code of the call to `csplit`
    local patchFile=$1
    local patchFileName=`basename $patchFile` && patchFileName=${patchFileName%.patch}
    local splitDir=/tmp/${patchFileName}.d && echo $splitDir
    mkdir -p $splitDir > /dev/null 2>&1
    cd $splitDir && rm -rf *.patch > /dev/null 2>&1

    # NOTE: This must be the last line to execute in order to properly return
    #       the error code from the attempted to split the patchFile
    csplit --quiet -b %02d.patch -z -f ${patchFileName}_ $patchFile /^diff\ --git.*/ '{*}'
}

_cleanSplitDir() {
    # Auxiliary function to clean the splitDir from any garbage patch files created during the
    # process of splitting the original patch file in chinks. It is possible few of the so
    # created chunk patch files to contain only commit information  and no source code changes.
    # These will later generate false positives during the process of incremental changes application.
    # :param $1: The directory containing the chunk patchFiles
    local splitDir=$1
    for file in ${splitDir}/*
    do
        grep -qE ^diff $file || rm $file
    done
}

_createFilesDst() {
    # A simple function to create test, static and toplevel files destination for not breaking the patches
    # because of a missing destination:
    # :param $1:   The source branch to be used for checking the files: could be TAG or Master
    # :param $2-*: The list of files to be checked out
    # NOTE:        The destinations are all starting from $toplevel but static files should
    #              live under $toplevel/data instead of $toplevel/src
    local srcBranch=$1
    shift
    local fileList=$*

    for file in $fileList
    do
        fileName=`basename $file`
        fileDir=`dirname $file`
        fileDest=$toplevel/$file
        # for static files substitute `/src/` with `/data/`
        fileDest=${fileDest/\/src\//\/data\/}

        # echo DEBUG: $FUNCNAME: fileName=$fileName
        # echo DEBUG: $FUNCNAME: fileDir=$fileDir

        # Create the file path if missing
        # echo DEBUG: $FUNCNAME: mkdir -p $toplevel/$fileDir
        mkdir -p $toplevel/$fileDir
        echo INFO: $FUNCNAME: orig: $WMRawUrl/$srcBranch/$file
        echo INFO: $FUNCNAME: dest: $fileDest
        curl -f $WMRawUrl/$srcBranch/$file  -o $fileDest || {
            echo INFO: $FUNCNAME: file: $file missing at the origin.
            echo INFO: $FUNCNAME: Seems to be a new file for the current patch.
            echo INFO: $FUNCNAME: Removing it from the destination as well!
            rm -f $fileDest
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
        echo INFO: $FUNCNAME: orig: $WMRawUrl/$srcBranch/$file
        echo INFO: $FUNCNAME: dest: $pythonLibPath/$file
        curl -f $WMRawUrl/$srcBranch/$file  -o $pythonLibPath/$file || {
            echo INFO: $FUNCNAME: file: $file missing at the origin.
            echo INFO: $FUNCNAME: Seems to be a new file for the current patch.
            echo INFO: $FUNCNAME: Removing it from the destination as well!
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
    local splitDir
    # Check if we are running from a pipe
    $pipe && {
        if $zeroOnly ;then
            echo "INFO: $FUNCNAME: Zeroing WMCore code base from StdIn"
        else
            echo "INFO: $FUNCNAME: Patching WMCore code from StdIn"
        fi
        patchFile="/tmp/pipeTmp_$(id -u).patch"
        echo "INFO: $FUNCNAME: Creating a temporary patchFile from stdin at: $patchFile"
        cat <&0 > $patchFile || { err=$?; echo "ERROR: $FUNCNAME: While creating $patchFile"; exit $err ;}
        echo "INFO: $FUNCNAME: Splitting the temporary patchFile by files to update"
        splitDir=$(_splitPatchByFiles $patchFile) || { err=$?; echo "ERROR: $FUNCNAME: While splitting $patchFile"; exit $err ;}

        # Clean patchFiles containing only commit info and no source code changes:
        _cleanSplitDir $splitDir

        # Create the final list of patches
        [[ -z `ls -A $splitDir` ]] && { echo;echo "WARNING: $FUNCNAME: Splitting $patchFile produced no output! Skipping it!!" ;}
        patchFileList=`ls -1Xd $splitDir/*.patch`
        return
    }

    # Check if we were sent a file to patch from
    [[ -n $extPatchFile ]] && {
        if $zeroOnly ;then
            echo "INFO: $FUNCNAME: Zeroing WMCore code base with file: $extPatchFile"
        else
            echo "INFO: $FUNCNAME: Patching WMCore code with file: $extPatchFile"
        fi
        patchFile=$extPatchFile
        patchFileList=$patchFile
        echo "INFO: $FUNCNAME: Using command line provided patch file: $patchFile"
        echo "INFO: $FUNCNAME: Splitting the command line provided patchFile by files to update"
        splitDir=$(_splitPatchByFiles $patchFile) || { err=$?; echo "ERROR: $FUNCNAME: While splitting $patchFile"; exit $err ;}

        # Clean patchFiles containing only commit info and no source code changes:
        _cleanSplitDir $splitDir

        # Create the final list of patches
        [[ -z `ls -A $splitDir` ]] && { echo;echo "WARNING: $FUNCNAME: Splitting $patchFile produced no output! Skipping it!!" ;}
        patchFileList=`ls -1Xd $splitDir/*.patch`
        return
    }

    # Finally, if none of the above, build the list of patch files to be applied from the patchNums provided at the command line
    if $zeroOnly ; then
        echo "INFO: $FUNCNAME: Zeroing WMCore code base with PRs: $patchList"
    else
        echo "INFO: $FUNCNAME: Patching WMCore code with PRs: $patchList"
    fi
    for patchNum in $patchList
    do
        patchFile=/tmp/$patchNum.patch
        echo "INFO: $FUNCNAME: Downloading a temporary patchFile at: $patchFile"
        curl $WMPatchUrl/$patchNum.patch -o $patchFile || {
            err=$?; echo "ERROR: $FUNCNAME: While downloading $patchFile"; exit $err
        }
        echo "INFO: $FUNCNAME: Splitting the temporary patchFile by files to update"
        splitDir=$(_splitPatchByFiles $patchFile) || { err=$?; echo "ERROR: $FUNCNAME: While splitting $patchFile"; exit $err ;}

        # Clean patchFiles containing only commit info and no source code changes:
        _cleanSplitDir $splitDir

        # Create the final list of patches
        [[ -z `ls -A $splitDir` ]] && { echo;echo "WARNING: $FUNCNAME: Splitting $patchFile produced no output! Skipping it!!"; continue ;}
        patchFileList="$patchFileList `ls -1Xd $splitDir/*.patch`"
    done
}

_warnFilelist(){
    echo WARNING: Please consider checking the following list of files for eventual code conflicts:
    for file in $srcFileList $testFileList
    do
        echo WARNING: $pythonLibPath/$file
    done
}

_sortListUniq() {
    local list=($*)
    local uniqList=($(printf "%s\n" "${list[@]}" | sort -u))
    echo ${uniqList[@]}
}


# Start execution:

_createPatchFiles

echo
echo -e "DEBUG: patchFileList: \n`for i in $patchFileList; do echo $i; done`"
echo

# Build the full lists of source files altered by the given set of patch files to be applied
srcFileList=""
testFileList=""
toplevFileList=""
staticFileList=""
for patchFile in $patchFileList
do
    # Parse a list of files changed only by the current patch
    srcFileListTemp=`grep ^diff $patchFile |grep "a/src/python" |awk '{print $3}' |sort -u`
    testFileListTemp=`grep ^diff $patchFile |grep "a/test" |awk '{print $3}' |sort -u`
    toplevFileListTemp=`grep ^diff $patchFile |grep -E "a/(bin|deploy|doc|etc|standards|tools)" |awk '{print $3}' |sort -u`
    staticFileListTemp=`grep ^diff $patchFile |grep "a/src" |grep -v "a/src/python" |awk '{print $3}' |sort -u`

    # Reduce paths for both src and test file lists to the path depth known to
    # the WMCore modules/packages and add them to the global scope file lists
    for file in $srcFileListTemp
    do
        file=${file#a\/} && srcFileList="$srcFileList $file"
    done

    for file in $testFileListTemp
    do
        file=${file#a\/} && testFileList="$testFileList $file"
    done

    for file in $toplevFileListTemp
    do
        file=${file#a\/} && toplevFileList="$toplevFileList $file"
    done

    for file in $staticFileListTemp
    do
        file=${file#a\/} && staticFileList="$staticFileList $file"
    done

done

srcFileList=$(_sortListUniq $srcFileList)
testFileList=$(_sortListUniq $testFileList)
toplevFileList=$(_sortListUniq $toplevFileList)
staticFileList=$(_sortListUniq $staticFileList)

echo
echo INFO: srcFileList: $srcFileList
echo

echo
echo INFO: testFileList: $testFileList
echo

echo
echo INFO: toplevFileList: $toplevFileList
echo

echo
echo INFO: staticFileList: $staticFileList
echo


$zeroCodeBase && {
    echo
    echo --------------------------------------------------------
    echo "INFO: Refreshing all files which are to be patched from the origin and TAG: $currTag"
    echo

    # First create destination for test files from currTag if missing
    _createFilesDst $currTag $testFileList

    # Second create any needed toplevel destination from currTag if missing
    _createFilesDst $currTag $toplevFileList

    # Third create any needed static files destination from currTag if missing
    _createFilesDst $currTag $staticFileList

    # Then zero code base for source files from currTag
    _zeroCodeBase $currTag $srcFileList
}

# exit if the user has requested to only zero the code base
$zeroOnly && {  _warnFilelist; exit ;}

err=0
failedPatchList=""
echo
echo
echo --------------------------------------------------------
echo "INFO: Patching all files starting from the $($zeroCodeBase && echo original version of TAG: $currTag || echo current version of files)"
for patchFile  in $patchFileList
do
    echo
    echo
    echo --------------------------------------------------------
    echo "INFO: ----------------- Currently applying patch: $patchFile -----------------"
    _patchSingle $patchFile
    currErr=$?
    let err+=$currErr
    [[ $currErr -eq 0 ]] || failedPatchList="$failedPatchList $patchFile"
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
    echo WARNING: First patch attempt number of Errors: $err
    echo
    echo
    echo WARNING: There were errors while patching from TAG: $currTag
    echo WARNING: Most probably some of the files from the current patch were having changes
    echo WARNING: between the current PR and the tag deployed at the host/container.
    echo
    echo WARNING: failedPatchList: $failedPatchList
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
        _createFilesDst $currTag $testFileList
        _createFilesDst $currTag $toplevFileList
        _createFilesDst $currTag $staticFileList
        _zeroCodeBase $currTag $srcFileList
        echo
        echo
        echo WARNING: All files have been rolled back to their original version at TAG: $currTag
        _warnFilelist
        echo
        echo WARNING: failedPatchList: $failedPatchList
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
echo "INFO: Refreshing all files which are to be patched from origin/master branch:"
echo

# First create destination for test,static and toplevel files from master if missing
_createFilesDst "master" $testFileList
_createFilesDst "master" $toplevFileList
_createFilesDst "master" $staticFileList

# Then zero code base for source files from master
_zeroCodeBase "master" $srcFileList

err=0
failedPatchList=""
echo
echo
echo --------------------------------------------------------
echo "INFO: Patching all files starting from origin/master branch"
for patchFile in $patchFileList
do
    echo
    echo
    echo "INFO: --------------- Currently applying patch: $patchFile ---------------"
    _patchSingle $patchFile
    currErr=$?
    let err+=$currErr
    [[ $currErr -eq 0 ]] || failedPatchList="$failedPatchList $patchFile"
done


echo
echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
echo INFO: Second patch attempt number of Errors: $err
echo
echo

[[ $err -eq 0 ]] || {

    # Restore test, static, toplevel and source files to their original version
    _createFilesDst $currTag $testFileList
    _createFilesDst $currTag $toplevFileList
    _createFilesDst $currTag $staticFileList
    _zeroCodeBase $currTag $srcFileList

    echo
    echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    echo WARNING: There were errors while patching from master branch as well
    echo WARNING: All files have been rolled back to their original version at TAG: $currTag
    echo
    echo WARNING: failedPatchList: $failedPatchList
    echo
    echo
}
echo +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

_warnFilelist

exit $err
