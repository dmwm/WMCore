#!/usr/bin/env bash

help(){
    echo -e $*
    cat <<EOF
    Usage: deploy-wmagent-venv.sh [-s] [-n] [-v] [-y]
                                  [-r <wmcore_source_repository>] [-b <wmcore_source_branch>] [-t <wmcore_tag>]
                                  [-g <wmcore_config_repository>] [-d wmagent_path] [-i <pypi_index>]
                                  [-h <help>]

      -s                             Bool flag to setup run from source [Default: false]
      -n                             Bool flag to skip virtual environment space cleanup before deployment [Default: false - ALWAYS cleanup before deployment]
      -v                             Bool flag to set verbose mode [Default: false]
      -y                             Bool flag to assume 'Yes' to all deployment questions. [Default: false]
      -r  <wmcore_source_repository> WMCore source repository [Default: git://github.com/dmwm/wmcore.git"]
      -b  <wmcore_source_branch>     WMCore source branch [Default: master]
      -t  <wmcore_tag>               WMCore tag to be used for this deployment [Default: None]
      -d  <wmagent_path>             WMAgent virtual environment target path to be used for this deployment [Default: ./WMAgent.venv3]
      -h <help>                      Provides help to the current script

    # Example: Deploy WMAgent version 2.0.3rc1 from 'test' pypi index:
    #          at destination /data/tmp/WMAgent.venv3/
    # ./deploy-wmagent-venv.sh -i test -t 2.2.1 -d /data/tmp/WMAgent.venv3/

    # Example: Same as above, but do not cleanup deployment area and reuse it from previous installtion - usefull for testing behaviour
    #          of different versions during development or mix running from source and from pypi installed packages:
    # ./deploy-wmagent-venv.sh -n -i test -t 2.2.1 -d /data/tmp/WMAgent.venv3/

    # Example: Deploy WMAgent from source repository, use tag 2.2.1,
    #          at destination /data/tmp/WMAgent.venv3/:
    # ./deploy-wmagent-venv.sh -s -t 2.2.1 -d /data/tmp/WMAgent.venv3/

    # Example: Same as above, but assume 'Yes' to all questions. To be used in order to chose the default flow and rely only
    #          on the pameters set for configuring the deployment steps. This will avoid human intervention during deployment:
    # ./deploy-wmagent-venv.sh -y -s -t 2.2.1 -d /data/tmp/WMAgent.venv3/

    # DEPENDENCIES: All WMCore packages have OS or external libraries/packages dependencies, which are not having a pypi equivalent.
    #               In the past those have been resolved through the set of *.spec files maintained at: https://github.com/cms-sw/cmsdist/tree/comp_gcc630
    #               Currently they need to be resolved manually preveous to the Pypi based package installation.
    #               Here follows the list of all direct (first level) dependencies per service generated from those spec files:

    #               wmagent           : [python3, MariaDB, CouchDB]

EOF
}

usage(){
    help $*
    exit 1
}


# Setting default values for all input parameters.
# Command line options overwrite the default values.
# All of the lists from bellow are interval separated.

service="wmagent"                                                     # default is the WMAgent meta package
venvPath=$(realpath -m ./WMAgent.venv3)                                   # WMCore virtual environment target path
wmSrcRepo="https://github.com/dmwm/WMCore.git"                            # WMCore source Repo
wmSrcBranch="master"                                                      # WMCore source branch
wmTag=""                                                                  # wmcore tag Default: no tag
runFromSource=false                                                       # a bool flag indicating run from source
pipIndex="prod"                                                           # pypi Index to use
verboseMode=false
assumeYes=false
noVenvCleanup=false                                                       # a Bool flag to state if the virtual env is to be cleaned before deployment

# NOTE: We are about to stick to Python3 solely from now on. So if the default
#       python executable for the system we are working on (outside the virtual
#       environment) is linked to Python2, then we should try creating the environment
#       with `python3' instead of `python`. If this link is not present, we simply
#       fail during virtual environment creation. Once we are inside the virtual
#       environment the default link should always point to e Python3 executable
#       so the `pythonCmd' variable shouldn't be needed any more.

pythonCmd=python
[[ $($pythonCmd -V 2>&1) =~ Python[[:blank:]]+3.* ]] || pythonCmd=python3


### Searching for the mandatory and optional arguments:
# export OPTIND=1
while getopts ":t:r:d:b:i:snvyh" opt; do
    case ${opt} in
        d)
            venvPath=$OPTARG
            venvPath=$(realpath -m $venvPath) ;;
        t)
            wmTag=$OPTARG ;;
        r)
            wmSrcRepo=$OPTARG ;;
        b)
            wmSrcBranch=$OPTARG ;;
        i)
            pipIndex=$OPTARG ;;
        s)
            runFromSource=true ;;
        n)
            noVenvCleanup=true ;;
        v)
            verboseMode=true ;;
        y)
            assumeYes=true ;;
        h)
            help
            exit 0 ;;
        \? )
            msg="\nERROR: Invalid Option: -$OPTARG\n"
            usage "$msg" ;;
        : )
            msg="\nERROR: Invalid Option: -$OPTARG requires an argument\n"
            usage "$msg" ;;
    esac
done

[[ -n $wmTag ]] || $runFromSource || { usage "\nERROR: Either <run_from_source> or <wmcore_tag> must be set!\n" ;}

$verboseMode && set -x

# Swap noVenvCleanup flag with venvCleanup to avoid double negation and confusion:
venvCleanup=true && $noVenvCleanup && venvCleanup=false

# setting the default pypi options
pipIndexTestUrl="https://test.pypi.org/simple/"
pipIndexProdUrl="https://pypi.org/simple"

pipOpt="--no-cache-dir"
[[ $pipIndex == "test" ]] && {
    pipOpt="$pipOpt --index-url $pipIndexTestUrl --extra-index $pipIndexProdUrl" ;}

[[ $pipIndex == "prod" ]] && {
    pipOpt="$pipOpt --index-url $pipIndexProdUrl" ;}

# declaring the initial WMCoreVenvVars as an associative array in the global scope
declare -A WMCoreVenvVars

_addWMCoreVenvVar(){
    # Adding a WMCore virtual environment variable to the WMCoreVenvVars array
    # and to the current virtual environment itself
    # :param $1: The variable name
    # :param $2: The actual export value to be used
    local varName=$1
    local exportVal=$2
    WMCoreVenvVars[$varName]=$exportVal
    eval "export $varName=$exportVal"
}

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
            echo "Interrupt execution due to step failure: "
            echo "ERRORNO: $1"
            exit $1
            ;;
    esac
}

startSetupVenv(){
    # A function used for Initial setup parameters visualisation. It waits for 5 sec.
    # before continuing, to give the option for canceling in case of wrong parameter set.
    # :param: None

    echo "======================================================="
    echo "Deployment parameters:"
    echo "-------------------------------------------------------"
    echo "service              : $service"
    echo "venvPath             : $venvPath"
    echo "wmSrcRepo            : $wmSrcRepo"
    echo "wmSrcBranch          : $wmSrcBranch"
    echo "wmTag                : $wmTag"
    echo "serPatch             : $serPatch"
    echo "pypi Index           : $pipIndex"
    echo "runFromSource        : $runFromSource"
    echo "Cleanup Virtual Env  : $venvCleanup"
    echo "verboseMode          : $verboseMode"
    echo "assumeYes            : $assumeYes"
    echo "pythonCmd            : $pythonCmd and `which $pythonCmd`"
    echo "======================================================="
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 102
    echo "..."
    echo "You still have 5 sec. to cancel before we proceed."
    sleep 5
}

createVenv(){
    # Function for creating minimal virtual environment. It uses global
    # $venvCleanup to check if to clean the venv space before deployment.
    # :param: None
    echo
    echo "======================================================="
    echo "Creating minimal virtual environment:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 102
    echo "..."

    [[ -d $venvPath ]] || mkdir -p $venvPath || return $?
    if $venvCleanup ; then
        $pythonCmd -m venv --clear $venvPath || return $?
    else
        $pythonCmd -m venv $venvPath || return $?
    fi
}

cloneWMCore(){
    # Function for cloning WMCore source code and checkout to the proper branch
    # or tag based on the script's runtime prameters.
    # :param: None
    echo
    echo "======================================================="
    echo "Cloning WMCore source code:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
    echo "..."

    wmSrcPath=${venvPath}/srv/WMCore           # WMCore source code target path

    # NOTE: If the Virtual Environment is not to be cleaned during the current
    #       deployment and we already have either a source directory synced from
    #       previous deployments or a link at $wmSrcPath pointing to a source
    #       directory outside the virtual env. we simply skip git actions to protect
    #       developer's previous work.
    if $noVenvCleanup && ( [[ -d $wmSrcPath ]] || [[ -h $wmSrcPath ]] ); then
        echo "WMCore source has already been cloned and the NO Virtual Environment Cleanup is True."
        return 101
    else
        [[ -d $wmSrcPath ]] ||  mkdir -p $wmSrcPath || return $?
        cd $wmSrcPath
        git clone $wmSrcRepo $wmSrcPath && git checkout $wmSrcBranch && [[ -n $wmTag ]] && git reset --hard $wmTag
        [[ -z $wmTag ]] && wmTag=$(git tag --list *.*.*  --sort taggerdate |tail -n 1)
        git reset --hard $wmTag
        cd -
    fi
}

_pipUpgradeVenv(){
    # Helper function used only for pip Upgrade for the current virtual env
    # :param: None
    cd $venvPath
    # pip install $pipOpt wheel
    # pip install $pipOpt --upgrade pip
    pip install wheel
    pip install --upgrade pip
}

activateVenv(){
    # Function for activating the virtual environment
    # :param: None
    echo
    echo "======================================================="
    echo "Activate WMCore virtual env:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 102
    echo "..."
    source ${venvPath}/bin/activate
    _pipUpgradeVenv
}

setupIpython(){
    # Helper function to install Ipython during manual installation.
    # :param: None
    echo
    echo "======================================================="
    echo "If the current environment is about to be used for deployment Ipython would be a good recomemndation, but is not mandatory."
    echo -n "Install Ipython? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
    echo "Installing ipython..."
    pip install ipython
}

setupVenvHooks(){
    # Function used for setting up the WMCore virtual environment hooks
    # It uses the WMCoreVenvVars from the global scope. We also redefine the
    # deactivate function for the virtual environment such that we can restore
    # all WMCore related env. variables at deactivation time.
    # :param: None
    echo
    echo "======================================================="
    echo "Setup the WMCore hooks at the virtual environment activate script"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
    echo "..."

    echo "############# WMCore env vars ################" >> ${VIRTUAL_ENV}/bin/activate
    echo "declare -A WMCoreVenvVars" >> ${VIRTUAL_ENV}/bin/activate
    for var in ${!WMCoreVenvVars[@]}
    do
        echo "WMCoreVenvVars[$var]=${WMCoreVenvVars[$var]}" >> ${VIRTUAL_ENV}/bin/activate
        # echo -e "WMCoreVenvVars[$var]\t:\t${WMCoreVenvVars[$var]}"
    done

    # NOTE: If we have the WMCore hooks setup at the current virtual environment
    #       from previous deployments, we only need to be sure we execute _WMCoreVenvSet
    #       the last, so we fetch the newly added environment values. This is
    #       an extra precaution, because `${VIRTUAL_ENV}/bin/activate' should be
    #       recreated from scratch for a fresh virtual environment anyway, but we
    #       need to take measures in case this behaviour changes in the future.

    if grep "#* WMCore hooks #*" ${VIRTUAL_ENV}/bin/activate
    then
        sed -i 's/_WMCoreVenvSet.*WMCoreVenvVars\[\@\].*//g' ${VIRTUAL_ENV}/bin/activate
        cat <<EOF>>${VIRTUAL_ENV}/bin/activate
_WMCoreVenvSet \${!WMCoreVenvVars[@]}

EOF
    else
        cat <<EOF>>${VIRTUAL_ENV}/bin/activate

############# WMCore hooks ################

_old_deactivate=\$(declare -f deactivate)
_old_deactivate=\${_old_deactivate#*()}
eval "_old_deactivate() \$_old_deactivate"

_WMCoreVenvRrestore(){
    echo "Restoring all WMCore related environment variables:"
    local WMCorePrefix=_OLD_WMCOREVIRTUAL
    for var in \$@
    do
        local oldVar=\${WMCorePrefix}_\${var}
        unset \$var
        [[ -n \${!oldVar} ]] && export \$var=\${!oldVar}
        unset \$oldVar
    done
}

_WMCoreVenvSet(){
    echo "Setting up WMCore related environment variables:"
    local WMCorePrefix=_OLD_WMCOREVIRTUAL
    for var in \$@
    do
        local oldVar=\${WMCorePrefix}_\${var}
        [[ -n \${!var} ]] && export \$oldVar=\${!var}
        export \$var=\${WMCoreVenvVars[\$var]}
    done
}

deactivate (){
    _WMCoreVenvRrestore \${!WMCoreVenvVars[@]}
    _old_deactivate
}

_WMCoreVenvSet \${!WMCoreVenvVars[@]}

EOF
    fi
}

checkNeeded(){
    # Function used to check the current script dependencies.
    # It uses hard coded list of tools required by the current script in order
    # to be able to complete the run.
    # :param: None

    # NOTE: First of all, check for minimal bash version required.
    #       Associative arrays are not supported for bash versions earlier than 4.*
    #       This causes issues on OS X systems with the following error:
    #       ./wmagent-venv-deploy.sh: line 280: declare: -A: invalid option
    #       declare: usage: declare [-afFirtx] [-p] [name[=value] ...]
    verString=$(bash --version)
    [[ $verString =~ ^GNU[[:blank:]]+bash,[[:blank:]]+version[[:blank:]]+[4-9]+\..* ]] || {
        error=$?;
        echo "The current setup script requires bash version: 4.* or later. Please install it and rerun.";
        return $error ;}

    local neededTools="git awk grep md5sum tree"
    for tool in $neededTools
    do
        command -v $tool 2>&1 > /dev/null || {
            error=$?;
            echo "The current setup script requires: $tool in order to continue. Please install it and rerun." ;
            return $error ;}
    done
}

_sort(){
    # Simple auxiliary sort function.
    # :param $*: All parameters need to be string values to be sorted
    # :return:   Prints the alphabetically sorted list of all input parameters
    local -a result
    i=0
    result[$i]=$1
    shift
    for key in $*
    do
        let i++
        x=$i
        y=$i
        result[$i]=$key
        while [[ $x -gt 0 ]]
        do
            let x--
            if [[ ${result[$x]} > ${result[$y]} ]]; then
                tmpKey=${result[$x]}
                result[$x]=${result[$y]}
                result[$y]=$tmpKey
            else
                break
            fi
            let y--
        done
    done
    echo ${result[*]}
}

printVenvSetup(){
    # Function to print the current virtual environment setup. And a basic
    # deployment area tree, no deeper than 3 levels relative to the deployment
    # root path.
    # :param:  None
    # :return: Dumps a formatted string with the information for the current setup

    echo "======================================================="
    echo "Printing the final WMCore virtual environment parameters and tree:"
    echo "-------------------------------------------------------"
    echo
    tree -d -L 3 $venvPath
    echo
    echo "-------------------------------------------------------"

    local prefix="WMCoreVenvVars[]: "
    local prefixLen=${#prefix}

    # NOTE: Here choosing the common alignment position for all variables printed
    #       the position count starts from the beginning of line + prefixLen.
    local valAllign=0
    for var in ${!WMCoreVenvVars[@]}
    do
        vLen=${#var}
        [[ $valAllign -lt $vLen ]] && valAllign=$vLen
    done

    for var in $(_sort ${!WMCoreVenvVars[@]})
    do
        vLen=${#var}
        spaceLen=$(($valAllign - $vLen))
        space=""
        for ((i=0; i<=$spaceLen; i++))
        do
            space="$space "
        done
        spaceNewLineLen=$(($spaceLen + $prefixLen +$vLen))
        spaceNewLine=""
        for ((i=0; i<=$spaceNewLineLen; i++))
        do
            spaceNewLine="$spaceNewLine "
        done
        echo -e "WMCoreVenvVars[$var]${space}: ${WMCoreVenvVars[$var]//:/\\n$spaceNewLine}"

    done
}

wmaInstall() {
    # The main function to setup/add the WMAgent virtual environment variables
    # and call the install.sh script from: https://github.com/dmwm/CMSKubernetes/blob/master/docker/pypi/wmagent/install.sh

    venvPath=$(realpath -m $venvPath)
    deployRepo=https://github.com/dmwm/CMSKubernetes.git
    deployBranch=master
    echo "Cloning $deployRepo at $venvPath"
    cd $venvPath
    git clone $deployRepo --branch $deployBranch
    cd $venvPath/CMSKubernetes/docker/pypi/wmagent

    _addWMCoreVenvVar  WMA_TAG $wmTag
    _addWMCoreVenvVar  WMA_USER $(id -un)
    _addWMCoreVenvVar  WMA_GROUP $(id -gn)
    _addWMCoreVenvVar  WMA_UID $(id -u)
    _addWMCoreVenvVar  WMA_GID $(id -g)
    _addWMCoreVenvVar  WMA_ROOT_DIR $venvPath

    _addWMCoreVenvVar  WMA_VER_MINOR ${WMA_TAG#*.*.}
    _addWMCoreVenvVar  WMA_VER_MAJOR ${WMA_TAG%.$WMA_VER_MINOR}
    _addWMCoreVenvVar  WMA_VER_MINOR ${WMA_VER_MINOR%rc*}
    _addWMCoreVenvVar  WMA_VER_MINOR ${WMA_VER_MINOR%.*}
    _addWMCoreVenvVar  WMA_VER_RELEASE ${WMA_VER_MAJOR}.${WMA_VER_MINOR}
    _addWMCoreVenvVar  WMA_VER_PATCH ${WMA_TAG#$WMA_VER_RELEASE}
    _addWMCoreVenvVar  WMA_VER_PATCH ${WMA_VER_PATCH#.}

    # Basic WMAgent directory structure passed to all scripts through env variables:
    # NOTE: Those should be static and depend only on $WMA_BASE_DIR
    _addWMCoreVenvVar  WMA_BASE_DIR $WMA_ROOT_DIR/srv/wmagent
    _addWMCoreVenvVar  WMA_ADMIN_DIR $WMA_ROOT_DIR/admin/wmagent
    _addWMCoreVenvVar  WMA_CERTS_DIR $WMA_ROOT_DIR/certs
    _addWMCoreVenvVar  X509_HOST_CERT $WMA_CERTS_DIR/servicecert.pem
    _addWMCoreVenvVar  X509_HOST_KEY  $WMA_CERTS_DIR/servicekey.pem
    _addWMCoreVenvVar  X509_USER_CERT $WMA_CERTS_DIR/servicecert.pem
    _addWMCoreVenvVar  X509_USER_KEY $WMA_CERTS_DIR/servicekey.pem
    _addWMCoreVenvVar  X509_USER_PROXY $WMA_CERTS_DIR/myproxy.pem

    _addWMCoreVenvVar  WMA_CURRENT_DIR $WMA_BASE_DIR/$WMA_TAG
    _addWMCoreVenvVar  WMA_INSTALL_DIR $WMA_CURRENT_DIR/install
    _addWMCoreVenvVar  WMA_CONFIG_DIR $WMA_CURRENT_DIR/config
    _addWMCoreVenvVar  WMA_CONFIG_FILE $WMA_CONFIG_DIR/config.py
    _addWMCoreVenvVar  WMA_LOG_DIR $WMA_CURRENT_DIR/logs
    _addWMCoreVenvVar  WMA_DEPLOY_DIR $venvPath
    _addWMCoreVenvVar  WMA_MANAGE_DIR $WMA_DEPLOY_DIR/bin
    _addWMCoreVenvVar  WMA_ENV_FILE $WMA_DEPLOY_DIR/deploy/env.sh
    _addWMCoreVenvVar  WMA_SECRETS_FILE $WMA_ADMIN_DIR/WMAgent.secrets

    _addWMCoreVenvVar  RUCIO_HOME $WMA_CONFIG_DIR
    _addWMCoreVenvVar  ORACLE_PATH $WMA_DEPLOY_DIR/etc/oracle

    # # Setting up users and previleges
    # sudo groupadd -g ${WMA_GID} ${WMA_GROUP}
    # sudo useradd -u ${WMA_UID} -g ${WMA_GID} -m ${WMA_USER}
    # # sudo install -o ${WMA_USER} -g ${WMA_GID} -d ${WMA_ROOT_DIR}
    # sudo usermod -aG mysql ${WMA_USER}

    # Add WMA_USER to sudoers
    # sudo echo "${WMA_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

    # Add all deployment needed directories
    cp -rv bin/* $WMA_DEPLOY_DIR/bin/
    cp -rv etc $WMA_DEPLOY_DIR/

    # Add install script
    cp -rv install.sh ${WMA_ROOT_DIR}/install.sh

    # Add wmagent run script
    cp -rv run.sh ${WMA_ROOT_DIR}/run.sh
    cp -rv init.sh ${WMA_ROOT_DIR}/init.sh

    cd $WMA_ROOT_DIR

    # Remove the already unneeded CMKubernetes repository:
    rm -rf $venvPath/CMSKubernetes

    # Install the requested WMA_TAG.
    ${WMA_ROOT_DIR}/install.sh -t ${WMA_TAG}
    # chown -R ${WMA_USER}:${WMA_GID} ${WMA_ROOT_DIR}

    # add $wmSrcPath in front of everything if we are running from source
    if $runFromSource; then
        _addWMCoreVenvVar PYTHONPATH ${wmSrcPath}/src/python/:${wmSrcPath}/test/python/:$PYTHONPATH
        _addWMCoreVenvVar PATH ${wmSrcPath}/bin/:$PATH
    fi
}

tweakVenv(){
    # # A function to tweak some Virtual Environment specific things, which are
    # # in general hard coded in the Docker image

    echo "Copy certificates and WMAgent.secrets file from an old agent"
    mkdir -p $WMA_CERTS_DIR
    cp -v /data/certs/servicekey.pem  $WMA_CERTS_DIR/
    cp -v /data/certs/servicecert.pem  $WMA_CERTS_DIR/
    # Try to find the WMAgent.secrets file at /data/dockerMount first
    mkdir -p $WMA_ROOT_DIR/admin/wmagent
    cp -v /data/dockerMount/admin/wmagent/WMAgent.secrets $WMA_ROOT_DIR/admin/wmagent/ ||
        cp -v /data/admin/wmagent/WMAgent.secrets $WMA_ROOT_DIR/admin/wmagent/
    echo "-------------------------------------------------------"

    echo "Eliminate mount points checks"
    sed -Ei "s/^_check_mounts.*().*\{.*$/_check_mounts() \{ return \$(true)/g" $WMA_ROOT_DIR/init.sh
}

main(){
    checkNeeded      || handleReturn $?
    startSetupVenv   || handleReturn $?
    createVenv       || handleReturn $?
    activateVenv     || handleReturn $?
    if $runFromSource; then
        cloneWMCore  || handleReturn $?
    fi
    wmaInstall       || handleReturn $?
    tweakVenv        || handleReturn $?
    setupIpython     || handleReturn $?
    setupVenvHooks   || handleReturn $?
    printVenvSetup
}

startPath=$(pwd)
main
cd $startPath
