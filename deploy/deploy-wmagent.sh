#!/bin/sh

### This script clones the CMSWEB deployment repository - master/HEAD branch - and
### then uses the Deploy script with the arguments provided in the command line to
### deploy WMAgent in a VOBox.
###
### It deploys the agent, apply all the required patches populate the
### resource-control database, apply final tweaks to the configuration and
### finally, download and create some utilitarian cronjobs.
###
### You also can choose whether you want to separate the WMAgent from the Couchdb
### deployment. By default Couch databases will be available in /data partition.
### Unless there is a /data1 partition and you select to use it.
###
### If you are deploying a testbed agent (with "testbed" in the team name), it will
### point to cmsweb-testbed DBSUrl.
###
### Usage: deploy-wmagent.sh -h
### Usage:               -w <wma_version>  WMAgent version (tag) available in the WMCore repository
### Usage:               -t <team_name>    Team name in which the agent should be connected to
### Usage:               -s <scram_arch>   The RPM architecture (defaults to slc5_amd64_gcc461)
### Usage:               -r <repository>   Comp repository to look for the RPMs (defaults to comp=comp)
### Usage:               -p <patches>      List of PR numbers in double quotes and space separated (e.g., "5906 5934 5922")
### Usage:               -n <agent_number> Agent number to be set when more than 1 agent connected to the same team (defaults to 0)
### Usage:
### Usage: deploy-wmagent.sh -w <wma_version> -t <team_name> [-s <scram_arch>] [-r <repository>] [-n <agent_number>]
### Usage: Example: sh deploy-wmagent.sh -w 2.2.5 -t production -n 30
### Usage: Example: sh deploy-wmagent.sh -w 2.1.4-b954b0745339a347ea28afd5b5767db4 -t testbed-vocms001 -p "11001" -r comp=comp.amaltaro
### Usage:

IAM=`whoami`
HOSTNAME=`hostname -f`
MY_IP=`host $HOSTNAME | awk '{print $4}'`
HPC_PEND_JOBS=2000
HPC_RUNN_JOBS=3000

BASE_DIR=/data/srv
DEPLOY_DIR=$BASE_DIR/wmagent
CURRENT_DIR=$BASE_DIR/wmagent/current
ADMIN_DIR=/data/admin/wmagent
ENV_FILE=/data/admin/wmagent/env.sh
CERTS_DIR=/data/certs/
OP_EMAIL=cms-comp-ops-workflow-team@cern.ch
DEPLOY_TAG=master

# These values may be overwritten by the arguments provided in the command line
WMA_ARCH=slc7_amd64_gcc630
REPO="comp=comp"
AG_NUM=0
FLAVOR=mysql
RPM_NAME=wmagentpy3

### Usage function: print the usage of the script
usage()
{
  perl -ne '/^### Usage:/ && do { s/^### ?//; print }' < $0
  exit 1
}

### Help function: print help for this script
help()
{
  perl -ne '/^###/ && do { s/^### ?//; print }' < $0
  exit 0
}

### Runs some basic checks before actually starting the deployment procedure
basic_checks()
{
  echo -n "Checking whether this node has the very basic setup for the agent deployment..."
  set -e
  if [ -d $DEPLOY_DIR/v$WMA_TAG ]; then
    echo -e "  FAILED!\n  You need to remove the previous $DEPLOY_DIR/v$WMA_TAG installation"
    exit 4
  elif [ ! -d $ADMIN_DIR ]; then
    echo -e "  FAILED!\n Could not find $ADMIN_DIR, creating it now."
    mkdir -p $ADMIN_DIR
    wget -nv https://raw.githubusercontent.com/dmwm/WMCore/master/deploy/env.sh -O $ENV_FILE
    download_secrets_file
    update_secrets_file
    echo "  Both env.sh and WMAgent.secrets files were created."
    echo "  Make sure to update the WMAgent.secrets file and run this script once again"
    exit 5
  elif [ ! -f $ADMIN_DIR/WMAgent.secrets ]; then
    echo -e "  FAILED!\n Could not find $ADMIN_DIR/WMAgent.secrets, downloading it now."
    download_secrets_file
    update_secrets_file
    echo "  It's just a template, so make sure to update the WMAgent.secrets file and run this script once again"
    exit 6
  elif [ ! -f $ENV_FILE ]; then
    echo -e "\n  Could not find $ENV_FILE, but I'm downloading it now."
    wget -nv https://raw.githubusercontent.com/dmwm/WMCore/master/deploy/env.sh -O $ENV_FILE
  fi

  if [ ! -d $CERTS_DIR ]; then
    echo -e "\n  Could not find $CERTS_DIR, but I'm creating it now"
    mkdir -p $CERTS_DIR
    chmod 755 $CERTS_DIR
    check_certs
  else
    check_certs
  fi

  check_process
  set +e
}

download_secrets_file(){
  cd $ADMIN_DIR
  if [[ "$TEAMNAME" == production ]]; then
    echo "  Dowloading a prodution agent secrets template..."
    wget -nv https://raw.githubusercontent.com/dmwm/WMCore/master/deploy/WMAgent.production -O $ADMIN_DIR/WMAgent.secrets
  else
    echo "  Dowloading a testbed agent secrets template..."
    wget -nv https://raw.githubusercontent.com/dmwm/WMCore/master/deploy/WMAgent.testbed -O $ADMIN_DIR/WMAgent.secrets
  fi
  cd -
}

update_secrets_file(){
  cd $ADMIN_DIR
  sed -i "s+MYSQL_USER=+MYSQL_USER=$IAM+" WMAgent.secrets
  sed -i "s+COUCH_USER=+COUCH_USER=$IAM+" WMAgent.secrets
  sed -i "s+COUCH_HOST=127.0.0.1+COUCH_HOST=$MY_IP+" WMAgent.secrets
  cd -
}

check_certs()
{
  echo -ne "\nChecking whether the certificates and proxy are in place ..."
  if [ ! -f $CERTS_DIR/myproxy.pem ] || [ ! -f $CERTS_DIR/servicecert.pem ] || [ ! -f $CERTS_DIR/servicekey.pem ]; then
    echo -e "\n  ... nope, trying to copy them from another node, you might be prompted for the cmst1 password."
    set -e
    if [[ "$IAM" == cmst1 ]]; then
      scp cmst1@vocms0255:/data/certs/* /data/certs/
    else
      scp cmsdataops@cmsgwms-submit3:/data/certs/* /data/certs/
    fi
    set +e
    chmod 600 $CERTS_DIR/servicecert.pem
    chmod 400 $CERTS_DIR/servicekey.pem
  else
    chmod 600 $CERTS_DIR/servicecert.pem
    chmod 400 $CERTS_DIR/servicekey.pem
  fi
  echo -e "  OK!\n"
}

check_process()
{
  echo -n "Checking whether there are any leftover processes ..."
  output=`ps aux | egrep 'couch|wmcore|mysql|beam' | grep -v deploy-wmagent.sh | wc -l`
  if [ "$output" -gt 1 ]; then
    echo "  FAILED!\n There are still $output WMCore process running. Quitting!"
    exit 8
  else
    echo -e "  OK!\n"
  fi
}

check_oracle()
{
  echo "Checking whether the oracle database is clean and not used by other agents ..."

  tmpdir=`mktemp -d`
  cd $tmpdir

  wget -nv https://raw.githubusercontent.com/dmwm/deployment/master/${RPM_NAME}/manage -O manage
  chmod +x manage
  echo -e "SELECT COUNT(*) from USER_TABLES;" > check_db_status.sql
  ### FIXME: new nodes do not have sqlplus ... what to do now?
  ./manage db-prompt < check_db_status.sql > db_check_output
  tables=`cat db_check_output | grep -A1 '\-\-\-\-' | tail -n 1`
  if [ "$tables" -gt 0 ]; then
    echo "  FAILED!\n This database is likely being used by another agent! Found $tables tables. Quitting!"
    exit 9
  else
    echo -e "  OK!\n"
  fi
  cd -
  rm -rf $tmpdir
}

for arg; do
  case $arg in
    -h) help ;;
    -w) WMA_TAG=$2; shift; shift ;;
    -t) TEAMNAME=$2; shift; shift ;;
    -s) WMA_ARCH=$2; shift; shift ;;
    -r) REPO=$2; shift; shift ;;
    -p) PATCHES=$2; shift; shift ;;
    -n) AG_NUM=$2; shift; shift ;;
    -*) usage ;;
  esac
done

if [[ -z $WMA_TAG ]] || [[ -z $TEAMNAME ]]; then
  usage
  exit 2
fi

basic_checks

source $ENV_FILE;

### Are we using Oracle or MySQL
MATCH_ORACLE_USER=`cat $WMAGENT_SECRETS_LOCATION | grep ORACLE_USER | sed s/ORACLE_USER=//`
MATCH_REQMGR2_URL=`cat $WMAGENT_SECRETS_LOCATION | grep REQMGR2_URL | sed s/REQMGR2_URL=//`

if [ "x$MATCH_ORACLE_USER" != "x" ]; then
  FLAVOR=oracle
  check_oracle
fi

if [[ "$HOSTNAME" == *cern.ch ]]; then
  MYPROXY_CREDNAME="amaltaroCERN"
  FORCEDOWN="'T3_US_NERSC', 'T3_US_SDSC', 'T3_US_ANL', 'T3_US_TACC', 'T3_US_PSC'"
elif [[ "$HOSTNAME" == *fnal.gov ]]; then
  MYPROXY_CREDNAME="amaltaroFNAL"
  FORCEDOWN=""
else
  echo "Sorry, I don't know this network domain name"
  exit 3
fi

DATA_SIZE=`lsblk -bo SIZE,MOUNTPOINT | grep ' /data1' | sort | uniq | awk '{print $1}'`
DATA_SIZE_GB=`lsblk -o SIZE,MOUNTPOINT | grep ' /data1' | sort | uniq | awk '{print $1}'`
if [[ $DATA_SIZE -gt 200000000000 ]]; then  # greater than ~200GB
  echo "Partition /data1 available! Total size: $DATA_SIZE_GB"
  sleep 0.5
  while true; do
    read -p "Would you like to deploy couchdb in this /data1 partition (yes/no)? " yn
    case $yn in
      [Y/y]* ) DATA1=true; break;;
      [N/n]* ) DATA1=false; break;;
      * ) echo "Please answer yes or no.";;
    esac
  done
else
  DATA1=false
fi && echo

echo "Starting new agent deployment with the following data:"
echo " - WMAgent version : $WMA_TAG"
echo " - RPM Name        : $RPM_NAME"
echo " - Team name       : $TEAMNAME"
echo " - WMAgent Arch    : $WMA_ARCH"
echo " - Repository      : $REPO"
echo " - Agent number    : $AG_NUM"
echo " - DB Flavor       : $FLAVOR"
echo " - ReqMgr2 URL     : $MATCH_REQMGR2_URL"
echo " - Use /data1      : $DATA1" && echo

mkdir -p $DEPLOY_DIR || true
cd $BASE_DIR
rm -rf deployment deployment.zip deployment-${DEPLOY_TAG};

set -e 
wget -nv -O deployment.zip --no-check-certificate https://github.com/dmwm/deployment/archive/refs/heads/${DEPLOY_TAG}.zip
unzip -q deployment.zip
cd deployment-${DEPLOY_TAG}
set +e 

echo -e "\n*** Removing the current crontab ***"
/usr/bin/crontab -r;
echo "Done!"

cd $BASE_DIR/deployment-$DEPLOY_TAG
set -e
for step in prep sw post; do
  echo -e "\n*** Deploying WMAgent: running $step step ***"
  ./Deploy -R ${RPM_NAME}@$WMA_TAG -s $step -A $WMA_ARCH -r $REPO -t v$WMA_TAG $DEPLOY_DIR ${RPM_NAME}
done
set +e

echo -e "\n*** Creating wmagent symlinks ***"
cd $CURRENT_DIR
ln -s ../sw${REPO##comp=comp}/${WMA_ARCH}/cms/${RPM_NAME}/${WMA_TAG} apps/wmagent
ln -s ../config/${RPM_NAME} config/wmagent

cd -
echo "Done!" && echo

# XXX: update the PR number below, if needed :-)
echo -e "\n*** Applying database schema patches ***"
cd $CURRENT_DIR
#  curl https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/11001.patch | patch -d apps/${RPM_NAME}/ -p 1
cd -
echo "Done!" && echo

# By default, it will only work for official WMCore patches in the general path
echo -e "\n*** Applying agent patches ***"
if [ "x$PATCHES" != "x" ]; then
  cd $CURRENT_DIR
  for pr in $PATCHES; do
    curl https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/$pr.patch | patch -d apps/${RPM_NAME}/lib/python*/site-packages/ -p 3
  done
cd -
fi
echo "Done!" && echo

# Update the manage location according to the RPM getting deployed
MANAGE_DIR=$BASE_DIR/wmagent/current/config/${RPM_NAME}/

echo -e "\n*** Activating the agent ***"
cd $MANAGE_DIR
./manage activate-agent
echo "Done!" && echo

### Enabling couch watchdog; couchdb fix for file descriptors
echo "*** Enabling couch watchdog ***"
sed -i "s+RESPAWN_TIMEOUT=0+RESPAWN_TIMEOUT=5+" $CURRENT_DIR/sw*/$WMA_ARCH/external/couchdb*/*/bin/couchdb
sed -i "s+exec 1>&-+exec 1>$CURRENT_DIR/install/couchdb/logs/stdout.log+" $CURRENT_DIR/sw*/$WMA_ARCH/external/couchdb*/*/bin/couchdb
sed -i "s+exec 2>&-+exec 2>$CURRENT_DIR/install/couchdb/logs/stderr.log+" $CURRENT_DIR/sw*/$WMA_ARCH/external/couchdb*/*/bin/couchdb
echo "Done!" && echo

echo "*** Starting services ***"
./manage start-services
echo "Done!" && echo
sleep 5

echo "*** Initializing the agent ***"
./manage init-agent
echo "Done!" && echo
sleep 5

echo "*** Checking if couchdb migration is needed ***"
echo -e "\n[query_server_config]\nos_process_limit = 50" >> $CURRENT_DIR/config/couchdb/local.ini
if [ "$DATA1" = true ]; then
  ./manage stop-services
  sleep 5
  if [ -d "/data1/database/" ]; then
    echo "Moving old database away... "
    mv /data1/database/ /data1/database_old/
    FINAL_MSG="5) Remove the old database when possible (/data1/database_old/)"
  fi
  rsync --remove-source-files -avr /data/srv/wmagent/current/install/couchdb/database /data1
  sed -i "s+database_dir = .*+database_dir = /data1/database+" $CURRENT_DIR/config/couchdb/local.ini
  sed -i "s+view_index_dir = .*+view_index_dir = /data1/database+" $CURRENT_DIR/config/couchdb/local.ini
  ./manage start-services
fi
echo "Done!" && echo

###
# tweak configuration
### 
echo "*** Tweaking configuration ***"
sed -i "s+REPLACE_TEAM_NAME+$TEAMNAME+" $MANAGE_DIR/config.py
sed -i "s+Agent.agentNumber = 0+Agent.agentNumber = $AG_NUM+" $MANAGE_DIR/config.py
if [[ "$TEAMNAME" == relval ]]; then
  sed -i "s+config.TaskArchiver.archiveDelayHours = 24+config.TaskArchiver.archiveDelayHours = 336+" $MANAGE_DIR/config.py
  sed -i "s+config.RucioInjector.metaDIDProject = 'Production'+config.RucioInjector.metaDIDProject = 'RelVal'+" $MANAGE_DIR/config.py
elif [[ "$TEAMNAME" == *testbed* ]] || [[ "$TEAMNAME" == *dev* ]]; then
  GLOBAL_DBS_URL=https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader
  sed -i "s+DBSInterface.globalDBSUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'+DBSInterface.globalDBSUrl = '$GLOBAL_DBS_URL'+" $MANAGE_DIR/config.py
  sed -i "s+DBSInterface.DBSUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'+DBSInterface.DBSUrl = '$GLOBAL_DBS_URL'+" $MANAGE_DIR/config.py
  sed -i "s+config.RucioInjector.metaDIDProject = 'Production'+config.RucioInjector.metaDIDProject = 'Test'+" $MANAGE_DIR/config.py
fi

### Populating resource-control
echo "*** Populating resource-control ***"
cd $MANAGE_DIR
if [[ "$TEAMNAME" == relval* || "$TEAMNAME" == *testbed* ]]; then
  echo "Adding only T1 and T2 sites to resource-control..."
  ./manage execute-agent wmagent-resource-control --add-T1s --plugin=SimpleCondorPlugin --pending-slots=50 --running-slots=50 --down
  ./manage execute-agent wmagent-resource-control --add-T2s --plugin=SimpleCondorPlugin --pending-slots=50 --running-slots=50 --down
else
  echo "Adding ALL sites to resource-control..."
  ./manage execute-agent wmagent-resource-control --add-all-sites --plugin=SimpleCondorPlugin --pending-slots=50 --running-slots=50 --down
fi
echo "Done!" && echo

echo "*** Setting up US opportunistic resources ***"
if [[ "$HOSTNAME" == *fnal.gov ]]; then
  sed -i "s+forceSiteDown = \[\]+forceSiteDown = \[$FORCEDOWN\]+" $MANAGE_DIR/config.py
  for resourceName in {T3_US_NERSC,T3_US_OSG,T3_US_PSC,T3_US_SDSC,T3_US_TACC,T3_US_Anvil,T3_US_Lancium,T3_ES_PIC_BSC,T3_US_Ookami};
  do
    ./manage execute-agent wmagent-resource-control --plugin=SimpleCondorPlugin --opportunistic \
      --pending-slots=$HPC_PEND_JOBS --running-slots=$HPC_RUNN_JOBS --add-one-site $resourceName
  done
else
  sed -i "s+forceSiteDown = \[\]+forceSiteDown = \[$FORCEDOWN\]+" $MANAGE_DIR/config.py
  ./manage execute-agent wmagent-resource-control --plugin=SimpleCondorPlugin --opportunistic \
    --pending-slots=$HPC_PEND_JOBS --running-slots=$HPC_RUNN_JOBS --add-one-site T3_ES_PIC_BSC
fi
echo "Done!" && echo

echo "*** Tweaking central agent configuration ***"
if [[ "$TEAMNAME" == production ]]; then
  echo "Agent connected to the production team, setting it to drain mode"
  agentExtraConfig='{"UserDrainMode":true}'
elif [[ "$TEAMNAME" == *testbed* ]]; then
  echo "Testbed agent, setting MaxRetries to 0..."
  agentExtraConfig='{"MaxRetries":0}'
elif [[ "$TEAMNAME" == *devvm* ]]; then
  echo "Dev agent, setting MaxRetries to 0..."
  agentExtraConfig='{"MaxRetries":0}'
fi
echo "Done!" && echo

### Upload WMAgentConfig to AuxDB
echo "*** Upload WMAgentConfig to AuxDB ***"
cd $MANAGE_DIR
./manage execute-agent wmagent-upload-config $agentExtraConfig
echo "Done!" && echo

###
# set scripts and specific cronjobs
###
echo "*** Downloading utilitarian scripts ***"
cd /data/admin/wmagent
wget -nv https://raw.githubusercontent.com/dmwm/WMCore/master/deploy/checkProxy.py -O checkProxy.py
wget -nv https://raw.githubusercontent.com/dmwm/WMCore/master/deploy/restartComponent.sh -O restartComponent.sh
wget -nv https://raw.githubusercontent.com/dmwm/WMCore/master/deploy/renew_proxy.sh -O renew_proxy.sh
chmod +x renew_proxy.sh restartComponent.sh
sed -i "s+CREDNAME+$MYPROXY_CREDNAME+" /data/admin/wmagent/renew_proxy.sh
echo "Done!" && echo

### Populating cronjob with utilitarian scripts
echo "*** Creating cronjobs for them ***"
if [[ "$TEAMNAME" == *testbed* || "$TEAMNAME" == *dev* ]]; then
  ( crontab -l 2>/dev/null | grep -Fv ntpdate
  echo "55 */6 * * * /data/admin/wmagent/renew_proxy.sh"
  ) | crontab -
else
  ( crontab -l 2>/dev/null | grep -Fv ntpdate
  echo "55 */6 * * * /data/admin/wmagent/renew_proxy.sh"
  echo "58 */12 * * * python /data/admin/wmagent/checkProxy.py --proxy /data/certs/myproxy.pem --time 150 --send-mail True --mail alan.malta@cern.ch"
  echo "#workaround for the ErrorHandler silence issue"
  echo "*/15 * * * *  /data/admin/wmagent/restartComponent.sh ErrorHandler JobSubmitter AgentStatusWatcher > /dev/null"
  ) | crontab -
fi
echo "Done!" && echo

echo && echo "Deployment finished!! However you still need to:"
echo "  1) Source the new WMA env: source /data/admin/wmagent/env.sh"
echo "  2) Double check agent configuration: less config/${RPM_NAME}/config.py"
echo "  3) Start the agent with: \$manage start-agent"
echo "  4) Remove the old WMAgent version when possible"
echo "  $FINAL_MSG"
echo "Have a nice day!" && echo

exit 0
