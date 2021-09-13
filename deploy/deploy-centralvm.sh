#!/bin/bash

### Usage: Usage: deploy-centralvm.sh -d <deployment_tag> -c <central_services_url> [-s reqmgr2ms -r comp.tivanov] [-r  <repository>] [-p  <patches>] [-s  <service_names>] [-l  <component_list>]
### Usage:
### Usage:   -d  <deployment_tag>   CMSWEB deployment tag used for this deployment
### Usage:   -b  <deployment_patch> List of PR numbers to be applied to the deployment scripts
### Usage:                          (in double quotes and space separated e.g. "967 968")
### Usage:   -r  <repository>       Comp repository to look for the RPMs (defaults to -r comp)
### Usage:   -p  <patches>          List of PR numbers
### Usage:                          (in double quotes and space separated e.g. "5906 5934 5922")
### Usage:   -s  <service_names>    List of service names to be patched
### Usage:                          (in double quotes and space separated (e.g. "rqmgr2 reqmgr2ms")
### Usage:   -c  <central_services> Url to central services (e.g. tivanov-unit01.cern.ch)
### Usage:   -l  <component_list>   List of components to be deployed
### Usage:                          (in double quotes and space separated e.g. "frontend couchdb reqmgr2")
### Usage:   -h <help>              Provides help to the current script
### Usage:
### Usage: Example: ./deploy-centralvm.sh -d HG2011a -c tivanov-unit01.cern.ch
### Usage: Example: ./deploy-centralvm.sh -d HG2011a -b "967" -p "10003" -s reqmgr2ms -r comp.tivanov -c tivanov-unit01.cern.ch
### Usage: Example: yes | ./deploy-centralvm.sh -d HG2011a -b "967" -p "10003" -s reqmgr2ms -r comp.tivanov -c tivanov-unit01.cern.ch
### Usage:

FULL_SCRIPT_PATH="$(realpath "${0}")"

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

# componentList="admin frontend couchdb reqmgr2 reqmgr2ms workqueue reqmon t0_reqmon acdcserver"
# repo="comp"                        # comp repository to be used for rpm downloads
# depTag="HG2011a"                   # deployment tag
# depPatch="967"                     # a list of deplpyment Patched to be applied
# serPatch="10003"                   # a list of service patches to be applied
# serNameToPatch="reqmgr2ms"         # a list of service Names to patch
# vmName="tivanov-unit01.cern.ch"    # hostname for central services
# vmName=${vmName%%.*}

componentList="admin frontend couchdb reqmgr2 reqmgr2ms workqueue reqmon t0_reqmon acdcserver"
repo="comp"                      # comp repository to be used for rpm downloads
depTag=""                        # deployment tag
depPatch=""                      # a list of deplpyment Patched to be applied
serPatch=""                      # a list of service patches to be applied
serNameToPatch=""                # a list of service Names to patch
vmName=""                        # hostname for central services
vmName=${vmName%%.*}

### Searching for the mandatory and optional arguments:
# export OPTIND=1
while getopts ":c:d:r:b:s:p:l:h" opt; do
    case ${opt} in
        c)
            vmName=$OPTARG
            vmName=${vmName%%.*}
            ;;
        d)
            depTag=$OPTARG
            ;;
        r)
            repo=$OPTARG
            ;;
        b)
            depPatch=$OPTARG
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
[[ -z $depTag ]] && usage "Missing mandatory argument: -d <deployment_tag"


startSetup()
{
    echo "======================================================="
    echo "Deployment parameters:"
    echo "-------------------------------------------------------"
    echo "componentList: $componentList"
    echo "repo: $repo"
    echo "depTag: $depTag"
    echo "depPatch: $depPatch"
    echo "serPatch: $serPatch"
    echo "serNameToPatch: $serNameToPatch"
    echo "vmName: $vmName"
    echo "======================================================="
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."
}

initSetup()
{
    # Initial setup
    echo
    echo "======================================================="
    echo -n "This is NOT an initial installation, right? [y]: "
    read x && [[ $x =~ (n|N) ]] && {
        echo "Initial setup ..."
        sudo -l
        sudo yum -y install git zip unzip emacs-nox libXcursor libXrandr libXi libXinerama
        mkdir -p /tmp/foo
        cd /tmp/foo
        git clone git://github.com/dmwm/deployment.git cfg
        cfg/Deploy -t dummy -s post $PWD system/devvm
        # OPTIONAL: review what happened: less /tmp/foo/.deploy/*
        rm -fr /tmp/foo/
        # su - $USER
        sudo su $USER
    }
}

cleanVM()
{
    # Cleanup your VM
    echo
    echo "======================================================="
    echo "Cleaning UP your VM"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    ([ "$(hostname -f)" = "$vmName.cern.ch" ] || exit;
        echo "Deleting...";
        cd /data;
        $PWD/cfg/admin/InstallDev -s stop;
        crontab -r;
        killall python;
        sudo rm -fr [^aceu]* .??* current enabled)
}

cloneDep()
{
    # clone deployment scripts
    echo
    echo "======================================================="
    echo "Cloning deployment scripts:"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    [[ -d cfg ]] && mv -f cfg cfg.`date -Im`
    (cd /data; git clone git://github.com/dmwm/deployment.git cfg && cd cfg && git reset --hard $depTag)
}

enableCherrypy()
{
    # enable cherrypy threads
    echo
    echo "======================================================="
    echo "Enabling cherripy threads"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo  "..."

    sed -i "s/HOST.startswith(\"vocms0117\")/HOST.startswith(\"vocms0117\") or HOST.startswith(\"$vmName\")/g" cfg/{reqmgr2,reqmon,workqueue}/config.py
    grep "HOST.startswith(\"$vmName\").*"  cfg/{reqmgr2,reqmon,workqueue}/config.py
}

swithToRucioProd()
{
    # enable cherrypy threads
    echo
    echo "======================================================="
    echo "Switch to Rucio production instance"
    echo -n "Should we switch to prod? [n]: "
    read x && [[ $x =~ (yes|Yes|YES|y|Y) ]] ||  { echo "Continue using Rucio integration instance"; return ;}
    echo  "Switching to rucio prod instance ..."

    sed -i "s/BASE_URL.*==.*\"https:\/\/cmsweb\.cern\.ch\"/BASE_URL == \"https:\/\/cmsweb\.cern\.ch\" or BASE_URL.startswith(\"https:\/\/$vmName\")/g" cfg/reqmgr2ms/config-transferor.py cfg/reqmgr2ms/config-monitor.py cfg/workqueue/config.py
    grep "BASE_URL.startswith(\"https://$vmName\").*"  cfg/{reqmgr2,reqmgr2ms,workqueue}/*config*.py
}

patchDep()
{
    # patch deployment scripts
    echo
    echo "======================================================="
    echo "Patching deploytment scripts"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    [[ -n $depPatch ]] && { echo "Patching now: "
        for patch in $depPatch
        do
            echo "-------------------------------------------------------"
            echo -e "patch: $patch :"
            curl https://patch-diff.githubusercontent.com/raw/dmwm/deployment/pull/$patch.patch | patch -d cfg/ -p 1
        done ;}
}

serviceDeployment()
{
    # service deploymnt
    echo
    echo "======================================================="
    echo "Service Deployment"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    (VER=$depTag REPO="-r comp=$repo" A=/data/cfg/admin; ARCH=slc7_amd64_gcc630;
        cd /data;
        $A/InstallDev -R comp@$VER -A $ARCH -s image -v $VER -a $PWD/auth $REPO -p "$componentList")
}

updateCert()
{
    # Update the fake service certificate files placed under each service area, e.g.:
    echo
    echo "======================================================="
    echo "Update certificates"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."


    sudo chmod 660 /data/srv/current/auth/{reqmgr2,workqueue,acdcserver,reqmon,t0_reqmon,reqmgr2ms}/dmwm-service-{cert,key}.pem
    sudo cp /data/auth/dmwm-service-cert.pem /data/srv/current/auth/reqmgr2/dmwm-service-cert.pem
    sudo cp /data/auth/dmwm-service-cert.pem /data/srv/current/auth/workqueue/dmwm-service-cert.pem
    sudo cp /data/auth/dmwm-service-cert.pem /data/srv/current/auth/acdcserver/dmwm-service-cert.pem
    sudo cp /data/auth/dmwm-service-cert.pem /data/srv/current/auth/reqmon/dmwm-service-cert.pem
    sudo cp /data/auth/dmwm-service-cert.pem /data/srv/current/auth/t0_reqmon/dmwm-service-cert.pem
    sudo cp /data/auth/dmwm-service-cert.pem /data/srv/current/auth/reqmgr2ms/dmwm-service-cert.pem
    sudo cp /data/auth/dmwm-service-key.pem /data/srv/current/auth/reqmgr2/dmwm-service-key.pem
    sudo cp /data/auth/dmwm-service-key.pem /data/srv/current/auth/workqueue/dmwm-service-key.pem
    sudo cp /data/auth/dmwm-service-key.pem /data/srv/current/auth/acdcserver/dmwm-service-key.pem
    sudo cp /data/auth/dmwm-service-key.pem /data/srv/current/auth/reqmon/dmwm-service-key.pem
    sudo cp /data/auth/dmwm-service-key.pem /data/srv/current/auth/t0_reqmon/dmwm-service-key.pem
    sudo cp /data/auth/dmwm-service-key.pem /data/srv/current/auth/reqmgr2ms/dmwm-service-key.pem
    sudo chmod 440 /data/srv/current/auth/{reqmgr2,workqueue,acdcserver,reqmon,t0_reqmon,reqmgr2ms}/dmwm-service-{cert,key}.pem
    sudo chmod 400 /data/srv/current/auth/{workqueue,reqmgr2ms}/dmwm-service-key.pem
    sudo chown _reqmgr2ms:_config /data/srv/current/auth/reqmgr2ms/dmwm-service-key.pem
    sudo chown _workqueue:_config /data/srv/current/auth/workqueue/dmwm-service-key.pem
}

patchService()
{
    # patch the Service
    echo
    echo "======================================================="
    echo "Patching the Service"
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    echo "..."

    [[ -n $serPatch ]] && { echo "Patching now: "
        cd /data/srv/current ;
        for service in $serNameToPatch
        do
            for patch in $serPatch
            do
                echo "-------------------------------------------------------"
                echo -e "service: $service && patch: $patch :"
                wget -nv https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/$patch.patch -O - | patch -d apps/$service/lib/python2.7/site-packages/ -p 3
            done
        done ;}
}

startService()
{
    # start services:
    echo
    echo "======================================================="
    echo "Starting services Now:"
    echo "(A=/data/cfg/admin; cd /data; \$A/InstallDev -s start)"
    echo "======================================================="
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    (A=/data/cfg/admin; cd /data; $A/InstallDev -s start)

}

statusService()
{
    # check service status:
    echo
    echo "======================================================="
    echo "Checking for services Status Now:"
    echo "(A=/data/cfg/admin; cd /data; \$A/InstallDev -s status)"
    echo "======================================================="
    echo -n "Continue? [y]: "
    read x && [[ $x =~ (n|N) ]] && exit 1
    (A=/data/cfg/admin; cd /data; $A/InstallDev -s status)
}

main()
{
    startSetup
    initSetup
    cleanVM
    cloneDep
    patchDep
    enableCherrypy
    swithToRucioProd
    serviceDeployment
    updateCert
    patchService
    startService
    statusService
}

cd /data
main
cd -
