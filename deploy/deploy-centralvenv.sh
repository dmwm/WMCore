#!/bin/bash

help(){
    echo -e $1
    cat <<EOF
    Usage: deploy-centralvenv.sh -c <central_services_url> [-r]
                                [-s <wmcore_source_repository>] [-b <wmcore_source_branch>] [-t <wmcore_tag>]
                                [-g <wmcore_config_repository>] [-d <wmcore_config_branch>]
                                [-v wmcore_path] [-p <patches>]
                                [-l <component_list>] [-i <pypi_index>]
                                [-h <help>]

      -c  <central_services>         Url to central services (e.g. cmsweb-test1.cern.ch)
      -r  <run_from_source>          Bool flag to setup run from source [Default: false]
      -s  <wmcore_source_repository> WMCore source repository [Default: git://github.com/dmwm/wmcore.git"]
      -b  <wmcore_source_branch>     WMCore source branch [Default: master]
      -t  <wmcore_tag>               WMCore tag to be used for this deployment [Default: None]
      -g  <wmcore_config_repository> WMCore configuration repository [Default: https://gitlab.cern.ch/cmsweb-k8s/services_config"]
      -d  <wmcore_config_branch>     WMCore configuration branch [Default: test]
      -v  <wmcore_path>              WMCore virtual environment target path to be used for this deployment [Default: ./WMCore.venv3]
      -p  <patches>                  List of PR numbers [Default: None]
                                     (in double quotes and space separated e.g. "5906 5934 5922")
      -l  <component_list>           List of components to be deployed [Default: "wmcore" - WMCore metapackage]
                                     (in double quotes and space separated e.g. "rqmgr2 reqmgr2ms")
                                     (pip based package version syntax is also acceptable e.g. "wmcore>=2.0.0")
      -i <pypi-index>                The pypi index to use (i.e. prod or test) [Default: prod - pointing to https://pypi.org/simple/]
      -h <help>                      Provides help to the current script

    Example: ./deploy-centralvenv.sh -c cmsweb-test1.cern.ch -s -t 2.0.0.pre3
    Example: ./deploy-centralvenv.sh -c cmsweb-test1.cern.ch -p "10003 9998"
    Example: yes | ./deploy-centralvenv.sh -p "10003" -s reqmgr2ms -c cmsweb-test1.cern.ch
EOF
# DONE: Add option for fetching pypi packages from testbed index:
#       pip install --index-url https://test.pypi.org/simple/ wmcore
}

usage(){
    echo -e $1
    help
    exit 1
}

_realPath(){
    [[ -z $1 ]] &&  return
    pathExpChars="\? \* \+ \@ \! \{ \} \[ \]"
    for i in $pathExpChars
    do
        # Path name expansion not supported
        [[ $1 =~ .*${i}.* ]] &&  return 38
    done
    sufix="$(basename $1)"
    prefix=$(dirname $1)
    until cd $prefix
    do
        sufix="$(basename $prefix)/$sufix"
        prefix=$(dirname $prefix)
    done  2>/dev/null
    realPath=$(pwd -P)
    userPath=$sufix
    if [[ $realPath == "/" ]]
    then
        echo ${realPath}${userPath}
    else
        echo ${realPath}/${userPath}
    fi
    cd - 2>&1 >/dev/null
}


# _realPath(){
#     $( cd "$(dirname "$0")" ; pwd -P )
# }


FULL_SCRIPT_PATH="$(_realPath "${0}")"

# Setting default values for all input parameters.
# Command line options overwrite the default values.
# All of the lists from bellow are interval separated.
# e.g. componentList="admin reqmgr2 reqmgr2ms workqueue reqmon acdcserver"

componentList="wmcore"                                                    # default is the WMCore meta package
venvPath="./WMCore.venv3"                                                 # WMCore virtual environment target path
wmSrcRelPath="WMCore"                                                     # WMCore source code path relative to $venvPath
wmSrcRepo="https://github.com/dmwm/WMCore.git"                            # WMCore source Repo
wmSrcBranch="master"                                                      # WMCore source branch
wmCfgRepo="https://:@gitlab.cern.ch:8443/cmsweb-k8s/services_config.git"  # WMCore config Repo
wmCfgBranch="test"                                                        # WMCore config branch
wmTag=""                                                                  # wmcore tag Default: no tag
serPatch=""                                                               # a list of service patches to be applied
runFromSource=false                                                       # a bool flag indicating run from source
vmName=""                                                                 # hostname for central services
vmName=${vmName%%.*}
pipIndex="prod"                                                           # pypi Index to use


# TODO: find python vars from env
pythonCmd=/usr/bin/python3
pythonVersion=3.6

### Searching for the mandatory and optional arguments:
# export OPTIND=1
while getopts ":v:t:c:s:b:g:d:p:l:i:rh" opt; do
    case ${opt} in
        v)
            venvPath=$OPTARG
            venvPath=$(_realPath $venvPath)
            ;;
        t)
            wmTag=$OPTARG
            ;;
        c)
            vmName=$OPTARG
            vmName=${vmName%%.*}
            ;;
        r)
            runFromSource=true
            ;;
        s)
            wmSrcRepo=$OPTARG
            ;;
        b)
            wmSrcBranch=$OPTARG
            ;;
        g)
            wmCfgRepo=$OPTARG
            ;;
        d)
            wmCfgBranch=$OPTARG
            ;;
        p)
            serPatch=$OPTARG
            ;;
        l)
            componentList=$OPTARG
            ;;
        i)
            pipIndex=$OPTARG
            ;;
        h)
            help
            exit 0
            ;;
        \? )
            msg="Invalid Option: -$OPTARG"
            usage "$msg"
            ;;
        : )
            msg="Invalid Option: -$OPTARG requires an argument"
            usage "$msg"
            ;;
    esac
done


# check for mandatory parameters:
[[ -z $vmName ]] && usage "Missing mandatory argument: -c <central_services>"

# setting some more paths
venvPath=$(_realPath $venvPath)
wmSrcPath=${venvPath}/${wmSrcRelPath}  # WMCore source code target path
wmDepPath=${venvPath}/sw               # WMCore deployment target path
wmCfgPath=${venvPath}/config           # WMCore cofig target path
wmAuthPath=${venvPath}/auth            # WMCore auth target path
wmTmpPath=${venvPath}/tmp              # WMCore tmp path

# setting the default pypi options
pipOpt=""
[[ $pipIndex == "test" ]] && {
    pipIndexUrl="https://test.pypi.org/simple/"
    pipOpt="$pipOpt --index-url $pipIndexUrl" ;}

handleReturn(){

# Handling script interruption based on last exit code
# Return codes:
# 0     - Success - CONTINUE
# 100   - Success - skip step, consider it recoverable
# 101   - Success - skip step based on user choice
# 102   - Failure - interrupt execution based on user choice
# 1-255 - Failure - interrupt all posix return codes

# TODO: to test return codes compatibility to avoid system error codes overlaps

case $1 in
    0)
        return 0
        ;;
    100)
        echo "Skipping step due to execution errors. Continue script execution."
        return 0
        ;;
    101)
        echo "Skipping step due to user choice. Continue script execution."
        return 0
        ;;
    102)
        echo "Interrupt execution due to user choice."
        exit 102
        ;;
    *)
        echo "Interrupt execution due to execution failure."
        exit $?
        ;;
esac
}

startSetupVenv(){
    echo "======================================================="
    echo "Deployment parameters:"
    echo "-------------------------------------------------------"
    echo "componentList: $componentList"
    echo "venvPath: $venvPath"
    echo "wmSrcPath: $wmSrcPath"
    echo "wmDepPath: $wmDepPath"
    echo "wmCfgPath: $wmCfgPath"
    echo "wmCfgBranch: $wmCfgBranch"
    echo "wmAuthPath: $wmAuthPath"
    echo "wmTag: $wmTag"
    echo "serPatch: $serPatch"
    echo "pypi Index: $pipIndex"
    echo "runFromSource: $runFromSource"
    echo "vmName: $vmName"
    echo "======================================================="
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 102
    echo "..."
}

createVenv(){
    # Cleanup your setup space
    echo
    echo "======================================================="
    echo "Creating minimal virtual environment:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 102
    echo "..."

    [[ -d $venvPath ]] || mkdir $venvPath
    $pythonCmd -m venv --clear $venvPath
}

cloneWMCore(){
    # clone deployment scripts
    echo
    echo "======================================================="
    echo "Cloning WMCore source code:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 101
    echo "..."

    [[ -d $wmSrcPath ]] ||  mkdir -p $wmSrcPath
    cd $wmSrcPath
    git clone $wmSrcRepo $wmSrcPath && git checkout $wmSrcBranch && [[ -n $wmTag ]] && git reset --hard $wmTag
    cd -
    # echo we are here
}

cloneConfig(){
    # clone deployment scripts
    echo
    echo "======================================================="
    echo "Cloning WMCore configuration files:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 101
    echo "..."
    [[ -d $wmCfgPath ]] ||  mkdir -p $wmCfgPath
    cd $wmCfgPath
    git clone $wmCfgRepo $wmCfgPath && [[ -n $wmCfgBranch ]] && git checkout $wmCfgBranch
    cd -
}

_pipUpgradeVenv(){
    # upgrade pip for the current virtual env
    cd $venvPath
    pip install $pipOpt wheel
    pip install $pipOpt --upgrade pip
}

activateVenv(){
    # activate virtual environment
    echo
    echo "======================================================="
    echo "Activate WMCore virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 102
    echo "..."
    source ${venvPath}/bin/activate
    _pipUpgradeVenv
}

_pkgInstall(){
    # Basic pip install caller
    pkgList=$*
    pkgFail=""
    for pkg in $pkgList
    do
        pip install $pipOpt $pkg || pkgFail="$pkgFail $pkg"
    done

    [[ -z $pkgFail ]] || {
        echo
        echo "======================================================="
        echo "There were some package dependencies that couldn't be satisfied."
        echo "List of packages failed to install: $depFail"
        echo -n "Should we try to reinstall them while releasing version constraint? [y]: "
        read x && [[ $x =~ (n|N) ]] && exit 1
        echo "Retrying to satisfy dependency releasing version constraint:"
        echo "..."
        for pkg in $pkgFail
        do
            pkg=${pkg%%=*}
            pkg=${pkg%%~*}
            pkg=${pkg%%!*}
            pkg=${pkg%%>*}
            pkg=${pkg%%<*}
            pip install $pipOpt $pkg
        done ;}
}

pkgInstall(){
    # install the service package list inside the virtual environment:
    echo
    echo "======================================================="
    echo "Install all requested services inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 102
    echo "..."
    _pkgInstall $componentList
}

depSetupVenv(){
    # install all python dependencies inside the virtual environment:
    echo
    echo "======================================================="
    echo "Install all WMCore python dependencies inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 101
    echo "..."

    reqList=""
    reqFile=${wmSrcPath}/"requirements.txt"
    [[ -d $wmSrcPath ]] || { echo "Could not find WMCore source at: $wmSrcPath"; return 100 ;}
    [[ -f $reqFile ]] || { echo "Could not find requirements.txt file at: $reqFile"; return 100 ;}

    # first try to install the whole requirement list as it is from the global/prod pypi index
    pip install -r $reqFile && { echo "Dependencies successfully installed"; return 0 ;}

    # only then try to parse the list a package at a time and install from the pypi index used for the current run
    for pkg in `grep -v ^# $reqFile|awk '{print $1}'`
    do
        reqList="$reqList $pkg"
    done
    _pkgInstall $reqList || { echo "We did the best we could to deploy all needed packages but there are still unresolved dependencies. Consider fixing them manually! "; return 100 ;}

}

rucioSetupVenv(){
    # minimal setup for the Rucio package inside the virtual environment:
    echo
    echo "======================================================="
    echo "Create minimal Rucio client setup inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 101
    echo "..."

    cat << EOF > $venvPath/etc/rucio.cfg
[common]
[client]
rucio_host = http://cmsrucio-int.cern.ch
auth_host = https://cmsrucio-auth-int.cern.ch
auth_type = x509
ca_cert = /etc/grid-security/certificates/
client_cert = \$X509_USER_CERT
client_key = \$X509_USER_KEY
client_x509_proxy = \$X509_USER_PROXY
request_retries = 3
EOF
}

_setVenvHooks(){
    # Setting all virtual environment hooks that we need && executing them in the current run.
    # param $1: A string representing the hook we want to add.
    # TODO: Properly redefine the `deactivate` function, so we can get rid of
    #       whatever we have defined at activate time when deactivating the venv
    local hook=$1
    shift
    # adding the hook to the activate script:
    echo $hook >> ${VIRTUAL_ENV}/bin/activate

    # executing the hook for the current run:
    $hook

}

setDepPaths(){
    # Setup WMcore deployment paths:
    echo
    echo "======================================================="
    echo "Setup WMCore paths inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 102
    echo "..."
    [[ -d $wmDepPath  ]] || mkdir -p $wmDepPath
    [[ -d $wmCfgPath  ]] || mkdir -p $wmCfgPath
    [[ -d $wmAuthPath ]] || mkdir -p $wmAuthPath

    # Find current pythonlib
    # TODO: first double check if we are actually inside the virtual environment
    pythonLib=$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")
    # echo ${wmSrcPath}/src/python/ > ${pythonLib}/WMCore.pth # this does not work
    $runFromSource && {
        _setVenvHooks "export PYTHONPATH=${wmSrcPath}/src/python/:${pythonLib}"
        _setVenvHooks "export PATH=${wmSrcPath}/bin/:$PATH"
    }
}

setupInitScripts(){
    # Setup WMcore deployment paths:
    echo
    echo "======================================================="
    echo "Setup WMCore init scripts inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && return 101
    echo "..."
    # TODO: To create the init.sh scripts
}

setupIpython(){
    # Setup Ipython:
    echo
    echo "======================================================="
    echo "If the current environment is about to be used for deployment Ipython would be a good recomemndation, but is not mandatory."
    echo -n "Skip Ipythin instalation? [y]: "
    read x && [[ $x =~ (n|N) ]] || { echo; echo "Skipping Ipython installation!"; return 101 ;}
    echo "Installing ipython..."
    pip install $pipOpt ipython
}

main(){
    startSetupVenv || handleReturn $?
    createVenv || handleReturn $?
    $runFromSource && cloneWMCore
    activateVenv || handleReturn $?
    pkgInstall || handleReturn $?
    depSetupVenv || handleReturn $?
    cloneConfig || handleReturn $?
    rucioSetupVenv || handleReturn $?
    setDepPaths || handleReturn $?
    setupInitScripts || handleReturn $?
    setupIpython || handleReturn $?
}

startPath=$(pwd)
main
cd $startPath
