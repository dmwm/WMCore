#!/usr/bin/env bash

help(){
    echo -e $1
    cat <<EOF
    Usage: deploy-centralvenv.sh [-c <central_services_url>] [-s] [-n] [-v] [-y]
                                 [-r <wmcore_source_repository>] [-b <wmcore_source_branch>] [-t <wmcore_tag>]
                                 [-g <wmcore_config_repository>] [-d <wmcore_config_branch>]
                                 [-d wmcore_path] [-p <patches>] [-m <security string>]
                                 [-l <service_list>] [-i <pypi_index>]  [-e <python_executable>]
                                 [-h <help>]

      -c  <central_services>         Url to central services (e.g. cmsweb-test1.cern.ch)
      -s  <run_from_source>          Bool flag to setup run from source [Default: false]
      -n  <no_venv_cleanup>          Bool flag to skip virtual environment space cleanup before deployment [Default: false - ALWAYS cleanup before deployment]
      -v  <verbose_mode>             Bool flag to set verbose mode [Default: false]
      -y  <assume yes>               Bool flag to assume 'Yes' to all deployment questions.
      -r  <wmcore_source_repository> WMCore source repository [Default: git://github.com/dmwm/wmcore.git"]
      -b  <wmcore_source_branch>     WMCore source branch [Default: master]
      -t  <wmcore_tag>               WMCore tag to be used for this deployment [Default: None]
      -g  <wmcore_config_repository> WMCore configuration repository [Default: https://gitlab.cern.ch/cmsweb-k8s/services_config"]
      -j  <wmcore_config_branch>     WMCore configuration branch [Default: test]
      -d  <wmcore_path>              WMCore virtual environment target path to be used for this deployment [Default: ./WMCore.venv3]
      -p  <patches>                  List of PR numbers [Default: None]
                                     (in double quotes and space separated e.g. "5906 5934 5922")
      -m  <security string>          The security string to be used during deployment. Will be needed at startup [Default: ""]
      -l  <service_list>             List of services to be deployed [Default: "wmcore" - WMCore metapackage]
                                     The full list of installable services: "reqmgr2 workqueue reqmon t0_reqmon reqmgr2ms-transferor reqmgr2ms-monitor reqmgr2ms-output reqmgr2ms-unmerged reqmgr2ms-rulecleaner reqmgr2ms-pileup "
                                     (in double quotes and space separated e.g. "rqmgr2 reqmgr2ms-output")
                                     (pip based package version syntax is also acceptable e.g. "wmcore==2.0.0")
      -i <pypi-index>                The pypi index to use (i.e. prod or test) [Default: prod - pointing to https://pypi.org/simple/]
      -e <python_executable>         The path to the python executable to be used for this installation. To be preserved in the venv. [Default: System default python]
      -h <help>                      Provides help to the current script

    # Example: Deploy WMCore central services version 2.0.3rc1 linked with 'cmsweb-test1.cern.ch' as a frontend from 'test' pypi index
    #          at destination /data/tmp/WMCore.venv3/ and using 'Some security string' as a security string for operationss at runtime:
    # ./deploy-centralvenv.sh -c cmsweb-test1.cern.ch -i test -l wmcore==2.0.3rc1 -d /data/tmp/WMCore.venv3/ -m "Some security string"

    # Example: Same as above, but do not cleanup deployment area and reuse it from previous installtion - usefull for testing behaviour
    #          of different versions during development or mix running from source and from pypi installed packages.
    #          NOTE: The 'current' link must point to the proper deployment area e.g. either to 'srv/master' for running from source
    #                or to 'srv/2.0.3rc1' for running from pypi installed package):
    # ./deploy-centralvenv.sh -c cmsweb-test1.cern.ch -n -i test -l wmcore==2.0.3rc1 -d /data/tmp/WMCore.venv3/ -m "Some security string"

    # Example: Deploy WMCore central services from source repository, use tag 2.0.0.pre3, linked with 'cmsweb-test1.cern.ch' as a frontend
    #          at destination /data/tmp/WMCore.venv3/ and using 'Some security string' as a security string for operationss at runtime:
    # ./deploy-centralvenv.sh -c cmsweb-test1.cern.ch -s -t 2.0.0.pre3 -d /data/tmp/WMCore.venv3/ -m "Some security string"

    # Example: Same as above, but assume 'Yes' to all questions. To be used in order to chose the default flow and rely only
    #          on the pameters set for configuring the deployment steps. This will avoid human intervention during deployment:
    # ./deploy-centralvenv.sh -c cmsweb-test1.cern.ch -y -s -t 2.0.0.pre3 -d /data/tmp/WMCore.venv3/ -m "Some security string"

    # Example: Deploy WMCore central services from source repository, use tag 2.0.0.pre3, linked with a frontend defined from service_config files
    #          at destination /data/tmp/WMCore.venv3/ and using 'Some security string' as a security string for operationss at runtime:
    # ./deploy-centralvenv.sh -s -t 2.0.0.pre3 -d /data/tmp/WMCore.venv3/ -m "Some security string"

    # DEPENDENCIES: All WMCore packages have OS or external libraries/packages dependencies, which are not having a pypi equivalent.
    #               So far those has been resolved through the set of *.spec files maintained at: https://github.com/cms-sw/cmsdist/tree/comp_gcc630
    #               Here follows the list of all direct (first level) dependencies per service generated from those spec files:

    #               acdcserver           : [python3, rotatelogs, couchdb]
    #               reqmgr2              : [python3, rotatelogs, couchdb]
    #               reqmgr2ms-transferor : [python3, rotatelogs]
    #               reqmgr2ms-monitor    : [python3, rotatelogs]
    #               reqmgr2ms-output     : [python3, rotatelogs]
    #               reqmgr2ms-unmerged   : [python3, rotatelogs]
    #               reqmgr2ms-rulecleaner: [python3, rotatelogs]
    #               reqmgr2ms-pileup     : [python3, rotatelogs]
    #               reqmon               : [python3, rotatelogs]
    #               t0_reqmon            : [python3, rotatelogs]
    #               workqueue            : [python3, rotatelogs, couchdb, yui]

    #               The above list is generated from the 'cmsdist' repository by:
    #               git clone https://github.com/cms-sw/cmsdist/tree/comp_gcc630
    #               python WMCore/bin/adhoc-scripts/ParseSpecCmsswdist.py -a -d cmsdist/ -f <service.spec, e.g. reqmgr2ms.spec>

EOF
}

usage(){
    echo -e $1
    help
    exit 1
}

_realPath(){
    # A function to find the absolute path of a given entity (directory or file)
    # It also expands and follows soft links e.g. if we have the following link:
    #
    # $ ll ~/WMCoreDev.d
    # lrwxrwxrwx 1 user user 21 Apr 15  2020 /home/user/WMCoreDev.d -> Projects/WMCoreDev.d/
    #
    # An entity from inside the linked path will be expanded as:
    # $ _realPath ~/WMCoreDev.d/DBS
    # /home/user/Projects/WMCoreDev.d/DBS
    #
    # It uses only bash internals for compatibility with any Unix-like OS capable of running bash.
    # For simplicity reasons, it does not support shell path expansions like:
    # /home/*/WMCoreDev.d, but can be used with single paths solely.
    #
    # :param $1: The path to be followed to the / (root) base
    # :return:   Echos the absolute path of the entity

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
# e.g. serviceList="admin reqmgr2 reqmgr2ms workqueue reqmon acdcserver"

serviceList="wmcore"                                                      # default is the WMCore meta package
venvPath=$(_realPath "./WMCore.venv3")                                    # WMCore virtual environment target path
wmSrcRepo="https://github.com/dmwm/WMCore.git"                            # WMCore source Repo
wmSrcBranch="master"                                                      # WMCore source branch
wmCfgRepo="https://:@gitlab.cern.ch:8443/cmsweb-k8s/services_config.git"  # WMCore config Repo
wmCfgBranch="test"                                                        # WMCore config branch
wmTag=""                                                                  # wmcore tag Default: no tag
serPatch=""                                                               # a list of service patches to be applied
runFromSource=false                                                       # a bool flag indicating run from source
vmName=`hostname`                                                         # hostname for central services
vmName=${vmName%%.*}
pipIndex="prod"                                                           # pypi Index to use
verboseMode=false
assumeYes=false
noVenvCleanup=false                                                       # a Bool flag to state if the virtual env is to be cleaned before deployment
secString=""                                                              # The security string to be used during deployment.
                                                                          # This one will be needed later to start the services.

# NOTE: We are about to stick to Python3 solely from now on. So if the default
#       python executable for the system we are working on (outside the virtual
#       environment) is linked to Python2, then we should try creating the environment
#       with `python3' instead of `python`. If this link is not present, we simply
#       fail during virtual environment creation. Once we are inside the virtual
#       environment the default link should always point to e Python3 executable
#       so the `pythonCmd' variable shouldn't be needed any more.

pythonCmd=python
initPythonCmd=`which $pythonCmd`
[[ $(python --version 2>&1) =~ Python[[:blank:]]+2.* ]] && pythonCmd=python3


### Searching for the mandatory and optional arguments:
# export OPTIND=1
while getopts ":t:c:r:b:g:j:d:p:m:l:i:e:snvyh" opt; do
    case ${opt} in
        d)
            venvPath=$OPTARG
            venvPath=$(_realPath $venvPath) ;;
        t)
            wmTag=$OPTARG ;;
        c)
            vmName=$OPTARG
            vmName=${vmName%%.*} ;;
        r)
            wmSrcRepo=$OPTARG ;;
        b)
            wmSrcBranch=$OPTARG ;;
        g)
            wmCfgRepo=$OPTARG ;;
        j)
            wmCfgBranch=$OPTARG ;;
        p)
            serPatch=$OPTARG ;;
        m)
            secString=$OPTARG ;;
        l)
            serviceList=$OPTARG ;;
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
        e)
            pythonCmd=$OPTARG ;;
        h)
            help
            exit 0 ;;
        \? )
            msg="Invalid Option: -$OPTARG"
            usage "$msg" ;;
        : )
            msg="Invalid Option: -$OPTARG requires an argument"
            usage "$msg" ;;
    esac
done

$verboseMode && set -x

# Swap noVenvCleanup flag with venvCleanup to avoid double negation and confusion:
venvCleanup=true && $noVenvCleanup && venvCleanup=false

# Calculate the security string md5 sum;
secString=$(echo $secString | md5sum | awk '{print $1}')

# expand the enabled services list
# TODO: Find a proper way to include the `acdcserver' in the list bellow (its config is missing from service_configs).
if [[ ${serviceList} =~ ^wmcore.* ]]; then
    _enabledListTmp="reqmgr2 reqmgr2ms workqueue reqmon t0_reqmon"
else
    _enabledListTmp=$serviceList
fi

# NOTE: The following extra expansion won't be needed once we have the set of
#       python packages we build to be identical with the set of services we run
#       Meaning we need to split them as:
#       reqmgr2ms -> [reqmgr2ms-transferor, reqmgr2ms-monitor, reqmgr2ms-output,
#                     reqmgr2ms-ruleCleaner, reqmgr2ms-unmerged]
#       reqmgr2   -> [reqmgr, reqmgr2-tasks]
#       reqmon    -> [reqmon, reqmon-tasks]
#       t0_reqmon -> [t0_reqmon, t0_reqmon-tasks]
enabledList=""
for service in $_enabledListTmp
do
    # First cut all pypi packaging version suffixes
    service=${service%%=*}
    service=${service%%~*}
    service=${service%%!*}
    service=${service%%>*}
    service=${service%%<*}

    # Then expand the final enabled list
    if [[ $service == "reqmgr2ms" ]]; then
        enabledList="$enabledList reqmgr2ms-transferor"
        enabledList="$enabledList reqmgr2ms-monitor"
        enabledList="$enabledList reqmgr2ms-output"
        enabledList="$enabledList reqmgr2ms-rulecleaner"
        enabledList="$enabledList reqmgr2ms-unmerged-t1"
        enabledList="$enabledList reqmgr2ms-unmerged-t2t3"
        enabledList="$enabledList reqmgr2ms-unmerged-t2t3us"
        enabledList="$enabledList reqmgr2ms-pileup"
    elif [[ $service == "reqmgr2" ]]; then
        enabledList="$enabledList reqmgr2"
        enabledList="$enabledList reqmgr2-tasks"
    elif [[ $service == "reqmon" ]]; then
        enabledList="$enabledList reqmon"
        enabledList="$enabledList reqmon-tasks"
    elif [[ $service == "t0_reqmon" ]] ; then
        enabledList="$enabledList t0_reqmon"
        enabledList="$enabledList t0_reqmon-tasks"
    else
        enabledList="$enabledList $service"
    fi
done

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
    echo "serviceList          : $serviceList"
    echo "enabledList          : $enabledList"
    echo "venvPath             : $venvPath"
    echo "wmCfgRepo            : $wmCfgRepo"
    echo "wmCfgBranch          : $wmCfgBranch"
    echo "wmSrcRepo            : $wmSrcRepo"
    echo "wmSrcBranch          : $wmSrcBranch"
    echo "wmTag                : $wmTag"
    echo "serPatch             : $serPatch"
    echo "pypi Index           : $pipIndex"
    echo "runFromSource        : $runFromSource"
    echo "Cleanup Virtual Env  : $venvCleanup"
    echo "verboseMode          : $verboseMode"
    echo "assumeYes            : $assumeYes"
    echo "central services host: $vmName"
    echo "secSring             : $secString"
    echo "pythonCmd            : `$pythonCmd --version` at: `which $pythonCmd`"
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
        cd -
    fi
}

setupConfig(){
    # Function for cloning WMCore service_config files from gitlab and checkout
    # to the proper config branch based on the script's runtime parameters.
    # :param: None
    echo
    echo "======================================================="
    echo "Cloning WMCore configuration files:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
    echo "..."

    [[ -d $wmCfgPath ]] ||  mkdir -p $wmCfgPath || return $?
    cd $wmCfgPath
    git clone $wmCfgRepo $wmCfgPath
    # git checkout $wmCfgBranch && git pull origin $wmCfgBranch || return $?

    # First checkout to a fresh empty branch so we never mix with the origin branches:
    git checkout -b currentConfig

    for service in $enabledList
    do
        # Add enabled links:
        echo "Touching: ${wmEnabledPath}/${service}"
        touch ${wmEnabledPath}/${service} || { err=$? ; echo "Could not create enabled link for: $service"; return $err ;}

        # Checkout needed service configs from the specified configuration branch:
        echo "Cloning configuration for: $service"
        git checkout origin/$wmCfgBranch -- $service || { err=$? ; echo "Could not checkout configuration for: $service from: origin/$wmCfgBranch"; return $err ;}
    done
    cd -
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

_pkgInstall(){
    # Helper function to follow default procedure in trying to install a package
    # through pip and eventually resolve package dependency issues.
    # :param $*: A string including a space separated list of all packages to be installed

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
        echo "List of packages failed to install: $pkgFail"
        echo -n "Should we try to reinstall them while releasing version constraint? [y]: "
        $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
        echo "..."
        echo "Retrying to satisfy dependency releasing version constraint:"
        # NOTE: by releasing the package constrains here and installing from `test'
        #       pypi index but also using the `prod' index for resolving dependency issues)
        #       we may actually downgrade a broken new package uploaded at `test'
        #       with an older but working version from `prod'. We may consider
        #       skipping the step in the default flaw and keep it only for manual
        #       setup and debugging purposes
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
    # Function for installing the service package list inside the virtual, isolating
    # every package related to WMCore inside the $wmCurrPath. Once completed with the
    # deployment, renames the target directory to the proper WMcore version installed
    # and recreates the `current' soft link pointing to point to the new destiation
    # It uses the script's runtime prameters from global scope.
    # :param: None
    echo
    echo "======================================================="
    echo "Install all requested services inside the virtual env:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
    echo "..."
    _pkgInstall $serviceList || return $?

    # if we have deployed from pip then the $wmDepPath ends with `latest'
    # so we need to change it with the actual package version deployed
    if [[ ${wmDepPath##*/} == "latest" ]]; then
        local pkgVersion=$(python -c "from WMCore import __version__ as WMCoreVersion; print(WMCoreVersion)")
        local newDepPath=${wmDepPath%latest}$pkgVersion
        echo "pkgVersion: $pkgVersion"
        echo "wmDepPath: $wmDepPath"
        echo "newDepPath: $newDepPath"
        [[ -z $pkgVersion ]] && { echo "Could not determine the WMCore package version. Leaving it as latest"; return 0;}

        # NOTE: The following steps are  dangerous, because they include some `rm -rf' commands
        #       we need to take precautions we are not touching anything beyond the scope of the virtual environment!
        [[ $newDepPath =~ ^$venvPath ]] || { echo "Halt because crossing virtual environment boundaries." ; return 1 ;}
        [[ -d $newDepPath ]] && {  echo  "Removing leftovers from old deployments.";  rm -rvf $newDepPath ; }
        mv -v $wmDepPath $newDepPath || return $?
        [[ -h $wmCurrPath ]] && rm $wmCurrPath || return $?
        ln -s $newDepPath $wmCurrPath || return $?
    fi
}

setupDependencies(){
    # Function to install all WMCore python dependencies inside the virtual environment
    # based on the default WMCore/requirements.txt file. It will be found only if we
    # are deploying from source, otherwise the dependencies will be resolved by the
    # pypi package requirements and the step will be skipped. If not all dependencies
    # are satisfied a WARNING message is printed and the setup continues.
    # :param: None
    echo
    echo "======================================================="
    echo "Install all WMCore python dependencies inside the virtual env:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
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

setupRucio(){
    # Function to create a minimal setup for the Rucio package inside the virtual
    # environment. It uses rucio integration as default server to avoid interference
    # with production installations.
    # :param: None

    # NOTE: This configuration files will be used mostly during operations and
    #       development or running from source. The different services usually rely
    #       on their own configuration service_config script for defining the Rucio
    #       instance, but we still my consider adding an extra parameter to the script

    echo
    echo "======================================================="
    echo "Create minimal Rucio client setup inside the virtual env:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
    echo "..."

    _pkgInstall rucio-clients
    # _addWMCoreVenvVar "RUCIO_HOME" "$venvPath/"
    _addWMCoreVenvVar RUCIO_HOME $wmCurrPath

    cat << EOF > $RUCIO_HOME/etc/rucio.cfg
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

setupDeplTree(){
    # Function for setting up the default WMCore deployment tree.
    # Also creates a temporary `current' soft link to the deployment and configuration
    # path, which will be properly renamed and switched to point to the actual version
    # deployed on a later stage.
    # It also adds all WMCore related virtual environments to both: currently running
    # virtual environment and the WMCore hooks to be added to bin/activate
    # It uses the script's runtime prameters from global scope.
    # :param: None
    echo
    echo "======================================================="
    echo "Setup WMCore paths inside the virtual env:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 102
    echo "..."
    # NOTE: Setting the `current' symlink pointing to the actual wmcore version
    #       deployed. We are no longer having the cmsweb deployment tag HG20***
    #       Currently we have three possible cases:
    #       * The pypi package version deployed - we must pay attention if we
    #         have version misalignment of different services installed
    #       * The WMCore tag deployed if we are running from source and having
    #         a tag specified for this deployment.
    #       * The WMCore branch deployed if we are running from source and having
    #         a branch specified for this deployment.

    # Find current pythonlib
    # TODO: first double check if we are actually inside the virtual environment
    local pythonLib=$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

    # setting the basic paths from the deployment tree
    venvPath=$(_realPath $venvPath)
    wmTopPath=${venvPath}/srv               # WMCore TopLevel Path
    wmSrcPath=${wmTopPath}/WMCore           # WMCore source code target path
    wmAuthPath=${wmTopPath}/auth            # WMCore auth target path
    wmEnabledPath=${wmTopPath}/enabled      # WMCore enabled services path
    wmStatePath=${wmTopPath}/state          # WMCore services state path
    wmLogsPath=${wmTopPath}/logs            # WMCore services logs path
    [[ -d $wmAuthPath    ]] || mkdir -p $wmAuthPath     || return $?
    [[ -d $wmEnabledPath ]] || mkdir -p $wmEnabledPath  || return $?
    [[ -d $wmStatePath   ]] || mkdir -p $wmStatePath    || return $?
    [[ -d $wmLogsPath    ]] || mkdir -p $wmLogsPath     || return $?

    # Finding the version we are  about to deploy
    wmDepPath=""                            # WMCore deployment target path
    if $runFromSource; then
        [[ -z $wmDepPath ]] && [[ -n $wmTag ]] && wmDepPath=$wmTag
        [[ -z $wmDepPath ]] && [[ -n $wmSrcBranch ]] && wmDepPath=$wmSrcBranch
    else
        [[ -z $wmDepPath ]] && wmDepPath="latest"
    fi

    wmDepPath=${wmTopPath}/$wmDepPath
    [[ -d $wmDepPath ]] || mkdir -p $wmDepPath || return $?

    # creating the current symlink
    wmCurrPath=${wmTopPath}/current
    [[ -h $wmCurrPath ]] && rm $wmCurrPath
    ln -s $wmDepPath $wmCurrPath

    # adding $wmCurrPath as a --prefix option to pip command
    pipOpt="$pipOpt --prefix=$wmCurrPath"

    # add deployment path as the first path in PYTHONPATH (we cut the $venvPath part
    # from $pythonLib and substitute it with the $wmCurrPath which uses the `current' symlink )
    newPythonLib=${pythonLib#$venvPath}
    newPythonLib=${wmCurrPath}/${newPythonLib#/}

    _addWMCoreVenvVar PYTHONPATH ${newPythonLib}:${pythonLib}
    _addWMCoreVenvVar PATH ${wmCurrPath}/bin/:$PATH

    # setting the config and tmp paths to be inside `current'
    wmCfgPath=${wmCurrPath}/config           # WMCore cofig target path
    wmTmpPath=${wmCurrPath}/tmp              # WMCore tmp path

    [[ -d $wmCfgPath ]] || mkdir -p $wmCfgPath || return $?
    [[ -d $wmTmpPath ]] || mkdir -p $wmTmpPath || return $?

    # Creating auth, logs and state paths per enabled service:
    # NOTE: We do need to hold at least the ${service}Secrets.py files inside $wmCurrPath,
    #       and export them in the PYTHONPATH so that the secrets file can be reachable
    #       because those are deployment flavor dependent (e.g. prod, preprod, test)
    [[ -d ${wmCurrPath}/auth/ ]] || mkdir -p ${wmCurrPath}/auth || return $?
    for service in $enabledList
    do
        [[ -d ${wmCurrPath}/auth/${service} ]] || mkdir -p ${wmCurrPath}/auth/${service} || { err=$?; echo "could not create auth path for: $service";  return $err  ;}
        _addWMCoreVenvVar PYTHONPATH ${wmCurrPath}/auth/${service}:$PYTHONPATH

        [[ -d ${wmStatePath}/${service} ]] || mkdir -p ${wmStatePath}/${service} || { err=$?; echo "could not create state path for: $service";  return $err  ;}
        [[ -d ${wmLogsPath}/${service} ]] || mkdir -p ${wmLogsPath}/${service} || { err=$?; echo "could not create logs path for: $service";  return $err  ;}
    done

    _addWMCoreVenvVar X509_USER_CERT ${wmAuthPath}/dmwm-service-cert.pem
    _addWMCoreVenvVar X509_USER_KEY ${wmAuthPath}/dmwm-service-key.pem
    _addWMCoreVenvVar WMCORE_SERVICE_CONFIG ${wmCfgPath}
    _addWMCoreVenvVar WMCORE_SERVICE_ENABLED ${wmEnabledPath}
    _addWMCoreVenvVar WMCORE_SERVICE_AUTH ${wmAuthPath}
    _addWMCoreVenvVar WMCORE_SERVICE_STATE ${wmStatePath}
    _addWMCoreVenvVar WMCORE_SERVICE_LOGS ${wmLogsPath}
    _addWMCoreVenvVar WMCORE_SERVICE_TMP ${wmTmpPath}
    _addWMCoreVenvVar WMCORE_SERVICE_ROOT ${wmTopPath}

    # add $wmSrcPath in front of everything if we are running from source
    if $runFromSource; then
        _addWMCoreVenvVar PYTHONPATH ${wmSrcPath}/src/python/:$PYTHONPATH
        _addWMCoreVenvVar PATH ${wmSrcPath}/bin/:$PATH
        _addWMCoreVenvVar WMCORE_SERVICE_SRC ${wmSrcPath}
    fi
}

setupInitScripts(){
    # Function to build the WMcore init scripts, based on the current setup
    # configuration and set of enabled services
    # Two types of scripts are created:
    #  * `manage' script per enabled service, supporting basic operation like:
    #     start, stop, restart, status
    #  * `wmcmanage' global script for iterating through all enabled services. Supports:
    #     start[:service], stop[:service], restart[:service], status[:service], version[:service]
    # It uses the script's runtime prameters from global scope.
    #:param: None
    echo
    echo "======================================================="
    echo "Setup WMCore init scripts inside the virtual env:"
    echo -n "Continue? [y]: "
    $assumeYes || read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && return 101
    echo "..."

    # DONE: To create the init.sh scripts
    # First creating all the service level `manage' scripts:
    local wmVersion=$(python -c "from WMCore import __version__ as WMCoreVersion; print(WMCoreVersion)")
    for service in $enabledList
    do
        local manageScript=${wmCfgPath}/${service}/manage
        [[ -d ${wmCfgPath}/${service} ]] && touch $manageScript && chmod 755 $manageScript || { err=$?; echo "could not setup startup scripts for $service";  return $err  ;}
        cat<<EOF>$manageScript
#!/bin/bash

help(){
echo -e \$1
cat <<EOH
Usage: manage ACTION [SECURITY-STRING]

Available actions:
  help              show this help
  version           get current version of the service
  status            show current service's status
  sysboot           start server from crond if not running
  restart           (re)start the service
  start             (re)start the service
  stop              stop the service
EOH
}

usage(){
echo -e \$1
help
exit 1
}

ME=$service
WMVERSION=$wmVersion

ROOT=\${WMCORE_SERVICE_ROOT}
CFGDIR=\${WMCORE_SERVICE_CONFIG}/\$ME
LOGDIR=\${WMCORE_SERVICE_LOGS}/\$ME
STATEDIR=\${WMCORE_SERVICE_STATE}/\$ME

# NOTE: we need a better naming conventin for config-* files
# first expand all config files found into a simple indexed array:
CFGFILE=(\$CFGDIR/config*.py)

# check if we have array length grater than 1 - we have more than a single config in the same directory:
[[ \${#CFGFILE[*]} -gt 1 ]] && { echo "Found more than a single configuration file for the current service: \${CFGFILE[*]}" ; exit 1 ;}

# check if the file is actually readable:
[[ -r \$CFGFILE ]] || { echo "Could not find the service configuration file for: \$ME at: \$CFGFILE "; exit 1 ;}

# find auxiliary jemalloc.sh script for running the service with memeory usage optimizations.
jemalloc=\$(command -v jemalloc.sh)

LOG=$service
AUTHDIR=\$ROOT/current/auth/\$ME
COLOR_OK="\\033[0;32m"
COLOR_WARN="\\033[0;31m"
COLOR_NORMAL="\\033[0;39m"

# export PYTHONPATH=\$ROOT/auth/\$ME:\$PYTHONPATH
# export REQMGR_CACHE_DIR=\$STATEDIR
# export WMCORE_CACHE_DIR=\$STATEDIR

# Start service conditionally on crond restart.
sysboot()
{
  if [ -f \$CFGFILE ]; then
    \$jemalloc wmc-httpd -v -d \$STATEDIR -l "|rotatelogs \$LOGDIR/\$LOG-%Y%m%d-`hostname -s`.log 86400" \$CFGFILE
  fi
}

# Start the service.
start()
{
  echo "starting \$ME"
  if [ -f \$CFGFILE ]; then
    \$jemalloc wmc-httpd -r -d \$STATEDIR -l "|rotatelogs \$LOGDIR/\$LOG-%Y%m%d-`hostname -s`.log 86400" \$CFGFILE
  fi
}


# Stop the service.
stop()
{
  echo "stopping \$ME"
  if [ -f \$CFGFILE ]; then
    wmc-httpd -k -d \$STATEDIR \$CFGFILE
  fi
}

# Check if the server is running.
status()
{
  if [ -f \$CFGFILE ]; then
    wmc-httpd -s -d \$STATEDIR \$CFGFILE
  fi
}

# Verify the security string.
check()
{
  CHECK=\$(echo "\$1" | md5sum | awk '{print \$1}')
  if [ \$CHECK != $secString ]; then
    echo "\$0: cannot complete operation, please check documentation." 1>&2
    exit 2;
  fi
}

# Main routine, perform action requested on command line.
case \${1:-status} in
  sysboot ) sysboot ;;
  start | restart ) check "\$2"; stop; start ;;
  status )   status ;;
  stop ) check "\$2";  stop ;;
  help ) help ;;
  version ) echo "\$WMVERSION" ;;
  * )  echo "\$0: unknown action '\$1', please try '\$0 help' or documentation." 1>&2; exit 1 ;;
esac

EOF
    done

    # Creating the top level `wmcmanage' script:
    # NOTE: We need to put it directly into the virtual environment bin/
    local wmcManageScript=${VIRTUAL_ENV}/bin/wmcmanage
    touch $wmcManageScript && chmod 755 $wmcManageScript || { err=$?; echo "could not setup the top level wmcmanage script.";  return $err  ;}
    cat <<EOF>$wmcManageScript
#!/bin/bash

### The high level manage script for all WMCore enabled services.
### It applies the chosen action on either the full set of enabled services for the
### current virtual environment or to a single services pointed at the commandline

help(){
echo -e \$1
cat <<EOH
Usage: wmcmanage -h
Usage: wmcmanage status[:what]
Usage: wmcmanage start[:what] <security_string>
Usage: wmcmanage stop[:what] <security_string>
Usage: wmcmanage restart[:what] <security_string>
Usage: wmcmanage version[:what]
EOH
}

usage(){
echo -e \$1
help
exit 1
}

[[ \$# -eq 0 ]] && usage
STAGE="\$1"
SEC_STRING=\$2
[[ X"\$STAGE" == X ]] && usage

case \$STAGE in
    status:* | start:* | stop:* | restart:* | version:* )
        WHAT=\${STAGE#*:} STAGE=\${STAGE%:*} ;;
    status | start | stop | restart | version )
        WHAT="*" ;;
esac

case \$STAGE in
    status | start | stop | restart | version )
        for service in \${WMCORE_SERVICE_ENABLED}/\$WHAT; do
            [[ -f "\$service" ]] || continue
            service=\${service##*/}
            \${WMCORE_SERVICE_ROOT}/current/config/\$service/manage \$STAGE \$SEC_STRING
        done
        ;;
    * )
        echo "\$STAGE: bad stage, try -h for help" 1>&2
        exit 1
        ;;
esac
exit 0
EOF
}

setupIpython(){
    # Helper function to install Ipython during manual installation, it is skipped by default.
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
    #       ./deploy-centralvenv.sh: line 280: declare: -A: invalid option
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
        echo -e "WMCoreVenvVars[$var]${space}: ${WMCoreVenvVars[$var]//:/\n$spaceNewLine}"

    done
}

main(){
    checkNeeded      || handleReturn $?
    startSetupVenv   || handleReturn $?
    createVenv       || handleReturn $?
    activateVenv     || handleReturn $?
    setupDeplTree    || handleReturn $?
    if $runFromSource;
    then
        cloneWMCore  || handleReturn $?
        setupDependencies|| handleReturn $?
    else
        pkgInstall       || handleReturn $?
    fi
    setupRucio       || handleReturn $?
    setupConfig      || handleReturn $?
    setupInitScripts || handleReturn $?
    setupIpython     || handleReturn $?
    setupVenvHooks   || handleReturn $?
    printVenvSetup
}

startPath=$(pwd)
main
cd $startPath

