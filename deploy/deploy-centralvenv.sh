#!/bin/bash

### Usage: Usage: deploy-centralvenv.sh -t <wmcore_tag> -c <central_services_url> [-v wmcore_path] [-p  <patches>] [-s  <service_names>] [-l  <component_list>]
### Usage:
### Usage:   -t  <wmcore_tag>       WMCore tag to be used for this deployment [Default: 2.0.0]
### Usage:   -c  <central_services> Url to central services (e.g. tivanov-unit01.cern.ch)
### Usage:   -v  <wmcore_path>      WMCore virtual environment target path to be used for this deployment [Default: ./WMCore.venv3]
### Usage:   -p  <patches>          List of PR numbers
### Usage:                          (in double quotes and space separated e.g. "5906 5934 5922")
### Usage:   -s  <service_names>    List of service names to be patched
### Usage:                          (in double quotes and space separated (e.g. "rqmgr2 reqmgr2ms")
### Usage:   -l  <component_list>   List of components to be deployed [Default: "admin reqmgr2 reqmgr2ms workqueue reqmon acdcserver"]
### Usage:                          (in double quotes and space separated (e.g. "rqmgr2 reqmgr2ms")
### Usage:   -h <help>              Provides help to the current script
### Usage:
### Usage: Example: ./deploy-centralvenv.sh -c tivanov-unit01.cern.ch -t 2.0.0.pre3
### Usage: Example: ./deploy-centralvenv.sh -c tivanov-unit01.cern.ch -p "10003" -s reqmgr2ms
### Usage: Example: yes | ./deploy-centralvenv.sh -p "10003" -s reqmgr2ms -c tivanov-unit01.cern.ch
### Usage:
# TODO: to add configuration repository to use as a parameter and default + branch - prod/preprod/dev

realPath(){
    [[ -z $1 ]] &&  return
    pathExpChars="\? \* \+ \@ \! \{ \} \[ \]"
    for i in $pathExpChars
    do
        [[ $1 =~ .*${i}.* ]] && echo "Path name expansion not supported" &&  return
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

FULL_SCRIPT_PATH="$(realPath "${0}")"

usage()
{
    echo -e $1
    /usr/bin/perl -ne '/^### Usage:/ && do { s/^### Usage:?//; print }' < $FULL_SCRIPT_PATH
    exit 1
}

help()
{
    echo -e $1
    /usr/bin/perl -ne '/^### Usage:/ && do { s/^### Usage:?//; print }' < $FULL_SCRIPT_PATH
    exit 0
}



# Set the default parameters here.
# Command line options overwrite the default values.
# All of the lists from bellow are interval separated.

# old componentList used for the RPM based deployments
# componentList="admin frontend couchdb reqmgr2 reqmgr2ms workqueue reqmon t0_reqmon acdcserver"

componentList="admin reqmgr2 reqmgr2ms workqueue reqmon acdcserver"
venvPath="./WMCore.venv3"           # WMCore virtual environment target path
venvPath=$(realPath $venvPath)
wmSrcRelPath="WMCore"               # WMCore source code path relative to $venvPath
wmTag="2.0.0"                       # wmcore tag
serPatch=""                         # a list of service patches to be applied
serNameToPatch=""                   # a list of service Names to patch
vmName=""                           # hostname for central services
vmName=${vmName%%.*}
pythonCmd=/usr/bin/python3


### Searching for the mandatory and optional arguments:
# export OPTIND=1
while getopts ":v:t:c:s:p:l:h" opt; do
    case ${opt} in
        v)
            venvPath=$OPTARG
            venvPath=$(realPath $venvPath)
            ;;
        t)
            wmTag=$OPTARG
            ;;
        c)
            vmName=$OPTARG
            vmName=${vmName%%.*}
            ;;
        s)
            serNameToPatch=$OPTARG
            ;;
        p)
            serPatch=$OPTARG
            ;;
        l)
            componentList=$OPTARG
            ;;
        h)
            help
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
[[ -z $wmTag ]] && usage "Missing mandatory argument: -t <wmcore_tag"

# setting some more paths
wmSrcPath=${venvPath}/${wmSrcRelPath}  # WMCore source code target path
wmDepPath=${venvPath}/sw               # WMCore deployment target path
wmCfgPath=${venvPath}/config           # WMCore cofig target path
wmAuthPath=${venvPath}/auth            # WMCore auth target path
wmTmpPath=${venvPath}/tmp              # WMCore tmp path


handleReturn() {

# Handling script interruption based on last exit code
# Return codes:
# 0     - Success - CONTINUE
# 101   - Success - skip step based on user choice
# 102   - Failure - interrupt execution based on user choice
# 1-255 - Failure - interrupt all posix return codes

# TODO: to test return codes compatibility to avoid system error codes overlaps

case $1 in
    0)
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

realPath() {
    $( cd "$(dirname "$0")" ; pwd -P )
}

startSetupVenv()
{
    echo "======================================================="
    echo "Deployment parameters:"
    echo "-------------------------------------------------------"
    echo "componentList: $componentList"
    echo "venvPath: $venvPath"
    echo "wmSrcPath: $wmSrcPath"
    echo "wmDepPath: $wmDepPath"
    echo "wmCfgPath: $wmCfgPath"
    echo "wmAuthPath: $wmAuthPath"
    echo "wmTag: $wmTag"
    echo "serPatch: $serPatch"
    echo "serNameToPatch: $serNameToPatch"
    echo "vmName: $vmName"
    echo "======================================================="
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."
}

# initSetupVenv()
# {
#     # Initial setup
#     echo
#     echo "======================================================="
#     echo -n "Assuming this is NOT an initial installation! Is it correct? [y]: "
#     read x && [[ $x =~ (n|N) ]] && {
#         echo "Initial setup ..."
#         echo "Resolving system packages dependencies:"
#         # TODO: Here to probe for the underlying packaging system

#         sudo -l
#         sudo yum -y install python3 git zip unzip gcc python-devel mariadb mariadb-devel openssl openssl-devel
#     }
# }

cleanVenv()
{
    # Cleanup your setup space
    echo
    echo "======================================================="
    echo "Cleaning UP your setup space"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    ([ "$(hostname -f)" = "$vmName.cern.ch" ] || exit;
        echo "Deleting...";
        [[ -d $wmDepPath ]] && cd $wmDepPath &&  sudo rm -fr [^aceu]* .??* current enabled)
}

createVenv()
{
    # Cleanup your setup space
    echo
    echo "======================================================="
    echo "Creating minimal virtual environment:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    [[ -d $venvPath ]] || mkdir $venvPath
    $pythonCmd -m venv $venvPath
}

cloneWMCore()
{
    # clone deployment scripts
    echo
    echo "======================================================="
    echo "Cloning WMCore source code:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    [[ -d $wmSrcPath ]] ||  mkdir -p $wmSrcPath
    cd $wmSrcPath && git clone git://github.com/dmwm/wmcore.git . && git reset --hard $wmTag
}

activateVenv()
{
    # activate virtual environment
    echo
    echo "======================================================="
    echo "Activate WMCore virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."
    source ${venvPath}/bin/activate
}

pipUpgradeVenv()
{
    # upgrade pip for the current virtual env
    cd $venvPath
    pip install wheel
    pip install --upgrade pip
}

depSetupVenv()
{
    # install all python dependencies inside the virtual environment:
    echo
    echo "======================================================="
    echo "Install all WMCore python dependencies inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    cd $wmSrcPath

    depFail=""
    for pkg in `grep -v ^# requirements_py3.txt`
    do
        pip install $pkg || depFail="$depFail $pkg"
    done

    [[ -z $depFail ]] || {
        echo
        echo "======================================================="
        echo "There were some package dependencies that couldn't be satisfied."
        echo "List of packages failed to install: $depFail"
        echo -n "Should we try to reinstall them while releasing version constraint? [y]: "
        read x && [[ $x =~ (n|N) ]] && exit 1
        echo "Retrying to satisfy dependency releasing version constraint:"
        echo "..."
        for pkg in $depFail
        do
            pkg=${pkg%%=*}
            pkg=${pkg%%~*}
            pkg=${pkg%%!*}
            pkg=${pkg%%>*}
            pkg=${pkg%%<*}
            pip install $pkg
        done ;}
}


rucioSetupVenv()
{
    # minimal setup for the Rucio package inside the virtual environment:
    echo
    echo "======================================================="
    echo "Create minimal Rucio client setup inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
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

setDepPaths() {
    # Setup WMcore deployment paths:
    echo
    echo "======================================================="
    echo "Setup WMCore setup paths inside the virtual env:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."
    [[ -d $wmDepPath  ]] || mkdir -p $wmDepPath
    [[ -d $wmCfgPath  ]] || mkdir -p $wmCfgPath
    [[ -d $wmAuthPath ]] || mkdir -p $wmAuthPath
}

setupIpython(){
    # Setup Ipython:
    echo
    echo "======================================================="
    echo "If the current environment is about to be used for deployment Ipython would be a good recomemndation, but is not mandatory."
    echo -n "Skip Ipythin instalation? [y]: "
    read x && [[ $x =~ (n|N) ]] || { echo; echo "Skipping Ipython installation!"; return ;}
    echo "Installing ipython..."
    pip install ipython
}

main()
{
    startSetupVenv || handleReturn $?
    initSetupVenv  || handleReturn $?
    cleanVenv
    createVenv
    cloneWMCore
    activateVenv
    pipUpgradeVenv
    depSetupVenv
    rucioSetupVenv
    setDepPaths
    setupIpython
}

main
cd -
