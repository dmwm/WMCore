#!/bin/sh

### This script downloads a CMSWEB deployment tag and then use the Deploy script
### with the arguments provided in the command line to deploy WMAgent in a VOBox.
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
### Usage:               -c <cmsweb_tag>   CMSWEB deployment tag used for the WMAgent deployment
### Usage:               -t <team_name>    Team name in which the agent should be connected to
### Usage:               -s <scram_arch>   The RPM architecture (defaults to slc5_amd64_gcc461)
### Usage:               -r <repository>   Comp repository to look for the RPMs (defaults to comp=comp)
### Usage:               -p <patches>      List of PR numbers in double quotes and space separated (e.g., "5906 5934 5922")
### Usage:               -n <agent_number> Agent number to be set when more than 1 agent connected to the same team (defaults to 0)
### Usage:
### Usage: deploy-wmagent.sh -w <wma_version> -c <cmsweb_tag> -t <team_name> [-s <scram_arch>] [-r <repository>] [-n <agent_number>]
### Usage: Example: sh deploy-wmagent.sh -w 1.1.4.patch2 -c HG1706e -t production -n 2
### Usage: Example: sh deploy-wmagent.sh -w 1.1.6.pre11 -c HG1709c -t testbed-cmssrv214 -p "7926" -s slc7_amd64_gcc630 -r comp=comp.amaltaro
### Usage:
 
BASE_DIR=/data/srv 
DEPLOY_DIR=$BASE_DIR/wmagent 
ENV_FILE=/data/admin/wmagent/env.sh 
CURRENT=/data/srv/wmagent/current
MANAGE=/data/srv/wmagent/current/config/wmagent/ 
OP_EMAIL=cms-comp-ops-workflow-team@cern.ch
HOSTNAME=`hostname -f`

# These values may be overwritten by the arguments provided in the command line
WMA_ARCH=slc6_amd64_gcc493
REPO="comp=comp"
AG_NUM=0
FLAVOR=mysql

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

### Cleanup function: it cleans up the oracle database
cleanup_oracle()
{
  cd $CURRENT/config/wmagent/
  cat > clean-oracle.sql << EOT
BEGIN
   FOR cur_rec IN (SELECT object_name, object_type
                        FROM user_objects
                        WHERE object_type IN
                                ('TABLE',
                                'VIEW',
                                'PACKAGE',
                                'PROCEDURE',
                                'FUNCTION',
                                'SEQUENCE'
                                ))
   LOOP
        BEGIN
        IF cur_rec.object_type = 'TABLE'
        THEN
                EXECUTE IMMEDIATE  'DROP '
                                || cur_rec.object_type
                                || ' "'
                                || cur_rec.object_name
                                || '" CASCADE CONSTRAINTS';
        ELSE
                EXECUTE IMMEDIATE  'DROP '
                                || cur_rec.object_type
                                || ' "'
                                || cur_rec.object_name
                                || '"';
        END IF;
        EXCEPTION
        WHEN OTHERS
        THEN
                DBMS_OUTPUT.put_line (   'FAILED: DROP '
                                || cur_rec.object_type
                                || ' "'
                                || cur_rec.object_name
                                || '"'
                                );
        END;
   END LOOP;
END;
/

EOT

  while true; do
    tmpf=`mktemp`
    ./manage db-prompt < clean-oracle.sql > $tmpf
    if grep -iq "PL/SQL procedure successfully completed" $tmpf; then
      break
    fi
  done
  rm -f $tmpf
  echo -e "PURGE RECYCLEBIN;\nselect tname from tab;" > purging.sql
  ./manage db-prompt < purging.sql
  rm -f clean-oracle.sql purging.sql
  echo "Done!" && echo
}

for arg; do
  case $arg in
    -h) help ;;
    -w) WMA_TAG=$2; shift; shift ;;
    -c) CMSWEB_TAG=$2; shift; shift ;;
    -t) TEAMNAME=$2; shift; shift ;;
    -s) WMA_ARCH=$2; shift; shift ;;
    -r) REPO=$2; shift; shift ;;
    -p) PATCHES=$2; shift; shift ;;
    -n) AG_NUM=$2; shift; shift ;;
    -*) usage ;;
  esac
done

if [[ -z $WMA_TAG ]] || [[ -z $CMSWEB_TAG ]] || [[ -z $TEAMNAME ]]; then
  usage
  exit 1
fi

source $ENV_FILE;
MATCH_ORACLE_USER=`cat $WMAGENT_SECRETS_LOCATION | grep ORACLE_USER | sed s/ORACLE_USER=//`
if [ "x$MATCH_ORACLE_USER" != "x" ]; then
  FLAVOR=oracle
fi

if [[ "$HOSTNAME" == *cern.ch ]]; then
  MYPROXY_CREDNAME="amaltaroCERN"
  FORCEDOWN="'T3_US_NERSC'"
elif [[ "$HOSTNAME" == *fnal.gov ]]; then
  MYPROXY_CREDNAME="amaltaroFNAL"
  FORCEDOWN=""
else
  echo "Sorry, I don't know this network domain name"
  exit 1
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
echo " - WMAgent version: $WMA_TAG"
echo " - CMSWEB tag     : $CMSWEB_TAG"
echo " - Team name      : $TEAMNAME"
echo " - WMAgent Arch   : $WMA_ARCH"
echo " - Repository     : $REPO"
echo " - Agent number   : $AG_NUM"
echo " - DB Flavor      : $FLAVOR"
echo " - Use /data1     : $DATA1" && echo

mkdir -p $DEPLOY_DIR || true
cd $BASE_DIR
rm -rf deployment deployment.zip deployment-${CMSWEB_TAG};

set -e 
wget -nv -O deployment.zip --no-check-certificate https://github.com/dmwm/deployment/archive/$CMSWEB_TAG.zip
unzip -q deployment.zip
cd deployment-$CMSWEB_TAG
set +e 

echo -e "\n*** Applying (for couchdb1.6, etc) cert file permission ***"
chmod 600 /data/certs/service{cert,key}.pem
echo "Done!"

echo -e "\n*** Removing the current crontab ***"
/usr/bin/crontab -r;
echo "Done!"

cd $BASE_DIR/deployment-$CMSWEB_TAG
set -e
for step in prep sw post; do
  echo -e "\n*** Deploying WMAgent: running $step step ***"
  ./Deploy -R wmagent@$WMA_TAG -s $step -A $WMA_ARCH -r $REPO -t v$WMA_TAG $DEPLOY_DIR wmagent
done
set +e

echo -e "\n*** Activating the agent ***"
cd $MANAGE
./manage activate-agent
echo "Done!" && echo

### Checking the database backend
echo "*** Cleaning up database instance ***"
if [ "$FLAVOR" == "oracle" ]; then
  cleanup_oracle
elif [ "$FLAVOR" == "mysql" ]; then
  echo "Mysql, nothing to clean up" && echo
fi

# By default, it will only work for official WMCore patches in the general path
echo -e "\n*** Applying agent patches ***"
if [ "x$PATCHES" != "x" ]; then
  cd $CURRENT
  for pr in $PATCHES; do
    wget -nv https://github.com/dmwm/WMCore/pull/$pr.patch -O - | patch -d apps/wmagent/lib/python2*/site-packages/ -p 3
  done
cd -
fi
echo "Done!" && echo

### Enabling couch watchdog; couchdb fix for file descriptors
echo "*** Enabling couch watchdog ***"
sed -i "s+RESPAWN_TIMEOUT=0+RESPAWN_TIMEOUT=5+" $CURRENT/sw*/$WMA_ARCH/external/couchdb*/*/bin/couchdb
sed -i "s+exec 1>&-+exec 1>$CURRENT/install/couchdb/logs/stdout.log+" $CURRENT/sw*/$WMA_ARCH/external/couchdb*/*/bin/couchdb
sed -i "s+exec 2>&-+exec 2>$CURRENT/install/couchdb/logs/stderr.log+" $CURRENT/sw*/$WMA_ARCH/external/couchdb*/*/bin/couchdb
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
echo -e "\n[query_server_config]\nos_process_limit = 50" >> $CURRENT/config/couchdb/local.ini
if [ "$DATA1" = true ]; then
  ./manage stop-services
  sleep 5
  if [ -d "/data1/database/" ]; then
    echo "Moving old database away... "
    mv /data1/database/ /data1/database_old/
    FINAL_MSG="5) Remove the old database when possible (/data1/database_old/)"
  fi
  rsync --remove-source-files -avr /data/srv/wmagent/current/install/couchdb/database /data1
  sed -i "s+database_dir = .*+database_dir = /data1/database+" $CURRENT/config/couchdb/local.ini
  sed -i "s+view_index_dir = .*+view_index_dir = /data1/database+" $CURRENT/config/couchdb/local.ini
  ./manage start-services
fi
echo "Done!" && echo

###
# tweak configuration
### 
echo "*** Tweaking configuration ***"
sed -i "s+cmsdataops+$TEAMNAME+" $MANAGE/config.py
sed -i "s+Agent.agentNumber = 0+Agent.agentNumber = $AG_NUM+" $MANAGE/config.py
if [[ "$TEAMNAME" == relval ]]; then
  sed -i "s+'LogCollect': 1+'LogCollect': 2+" $MANAGE/config.py
  sed -i "s+config.TaskArchiver.archiveDelayHours = 24+config.TaskArchiver.archiveDelayHours = 336+" $MANAGE/config.py
elif [[ "$TEAMNAME" == hlt ]]; then
  sed -i "s+JobSubmitter.maxJobsPerPoll = 1000+JobSubmitter.maxJobsPerPoll = 3000+" $MANAGE/config.py
  sed -i "s+JobSubmitter.cacheRefreshSize = 30000+JobSubmitter.cacheRefreshSize = 1000+" $MANAGE/config.py
elif [[ "$TEAMNAME" == *testbed* ]] || [[ "$TEAMNAME" == *dev* ]]; then
  GLOBAL_DBS_URL=https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader
  sed -i "s+{'default': 3, 'Merge': 4, 'Cleanup': 2, 'LogCollect': 1, 'Harvesting': 2}+0+" $MANAGE/config.py
  sed -i "s+DBSInterface.globalDBSUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'+DBSInterface.globalDBSUrl = '$GLOBAL_DBS_URL'+" $MANAGE/config.py
  sed -i "s+DBSInterface.DBSUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'+DBSInterface.DBSUrl = '$GLOBAL_DBS_URL'+" $MANAGE/config.py
fi

if [[ "$HOSTNAME" == *fnal.gov ]]; then
  sed -i "s+forceSiteDown = \[\]+forceSiteDown = \[$FORCEDOWN\]+" $MANAGE/config.py
else
  sed -i "s+forceSiteDown = \[\]+forceSiteDown = \[$FORCEDOWN\]+" $MANAGE/config.py
fi
# TODO remove this hack once AlertProcessor gets fixed
sed -i "s+config.AlertProcessor.critical.sinks.email.fromAddr = 'noreply@cern.ch'+#config.AlertProcessor.critical.sinks.email.fromAddr = 'noreply@cern.ch'+" $MANAGE/config.py
sed -i "s+config.AlertProcessor.critical.sinks.email.smtpServer = 'cernmx.cern.ch'+#config.AlertProcessor.critical.sinks.email.smtpServer = 'cernmx.cern.ch'+" $MANAGE/config.py
sed -i "s+config.AlertProcessor.critical.sinks.email.toAddr = \['wmagentalerts@gmail.com'\]+#config.AlertProcessor.critical.sinks.email.toAddr = \['wmagentalerts@gmail.com'\]+" $MANAGE/config.py
sed -i "s+config.AlertProcessor.soft.sinks.email.fromAddr = 'noreply@cern.ch'+#config.AlertProcessor.soft.sinks.email.fromAddr = 'noreply@cern.ch'+" $MANAGE/config.py
sed -i "s+config.AlertProcessor.soft.sinks.email.smtpServer = 'cernmx.cern.ch'+#config.AlertProcessor.soft.sinks.email.smtpServer = 'cernmx.cern.ch'+" $MANAGE/config.py
sed -i "s+config.AlertProcessor.soft.sinks.email.toAddr = \['wmagentalerts@gmail.com'\]+#config.AlertProcessor.soft.sinks.email.toAddr = \['wmagentalerts@gmail.com'\]+" $MANAGE/config.py
echo "Done!" && echo

### Populating resource-control
echo "*** Populating resource-control ***"
cd $MANAGE
if [[ "$TEAMNAME" == relval* || "$TEAMNAME" == *testbed* ]]; then
  echo "Adding only T1 and T2 sites to resource-control..."
  ./manage execute-agent wmagent-resource-control --add-T1s --plugin=SimpleCondorPlugin --pending-slots=50 --running-slots=50 --down
  ./manage execute-agent wmagent-resource-control --add-T2s --plugin=SimpleCondorPlugin --pending-slots=50 --running-slots=50 --down
else
  echo "Adding ALL sites to resource-control..."
  ./manage execute-agent wmagent-resource-control --add-all-sites --plugin=SimpleCondorPlugin --pending-slots=50 --running-slots=50 --down
fi
echo "Done!" && echo

### Upload WMAgentConfig to AuxDB
echo "*** Upload WMAgentConfig to AuxDB ***"
cd $MANAGE
./manage execute-agent wmagent-upload-config
echo "Done!" && echo

###
# set scripts and specific cronjobs
###
echo "*** Downloading utilitarian scripts ***"
cd /data/admin/wmagent
rm -f checkProxy.py restartComponent.sh
wget -q --no-check-certificate https://raw.githubusercontent.com/amaltaro/scripts/master/checkProxy.py
wget -q --no-check-certificate https://raw.githubusercontent.com/amaltaro/ProductionTools/master/restartComponent.sh
echo "Done!" && echo

### Populating cronjob with utilitarian scripts
echo "*** Creating cronjobs for them ***"
if [[ "$TEAMNAME" == *testbed* || "$TEAMNAME" == *dev* ]]; then
  ( crontab -l 2>/dev/null | grep -Fv ntpdate
  echo "55 */12 * * * (export X509_USER_CERT=/data/certs/servicecert.pem; export X509_USER_KEY=/data/certs/servicekey.pem; myproxy-get-delegation -v -l amaltaro -t 168 -s 'myproxy.cern.ch' -k $MYPROXY_CREDNAME -n -o /data/certs/mynewproxy.pem && voms-proxy-init -rfc -voms cms:/cms/Role=production -valid 168:00 -noregen -cert /data/certs/mynewproxy.pem -key /data/certs/mynewproxy.pem -out /data/certs/myproxy.pem)"
  ) | crontab -
else
  ( crontab -l 2>/dev/null | grep -Fv ntpdate
  echo "55 */12 * * * (export X509_USER_CERT=/data/certs/servicecert.pem; export X509_USER_KEY=/data/certs/servicekey.pem; myproxy-get-delegation -v -l amaltaro -t 168 -s 'myproxy.cern.ch' -k $MYPROXY_CREDNAME -n -o /data/certs/mynewproxy.pem && voms-proxy-init -rfc -voms cms:/cms/Role=production -valid 168:00 -noregen -cert /data/certs/mynewproxy.pem -key /data/certs/mynewproxy.pem -out /data/certs/myproxy.pem)"
  echo "58 */12 * * * python /data/admin/wmagent/checkProxy.py --proxy /data/certs/myproxy.pem --time 120 --send-mail True --mail alan.malta@cern.ch"
  echo "#workaround for the ErrorHandler silence issue"
  echo "*/15 * * * *  source /data/admin/wmagent/restartComponent.sh > /dev/null"
  ) | crontab -
fi
echo "Done!" && echo

echo && echo "Deployment finished!! However you still need to:"
echo "  1) Source the new WMA env: source /data/admin/wmagent/env.sh"
echo "  2) Double check agent configuration: less config/wmagent/config.py"
echo "  3) Start the agent with: \$manage start-agent"
echo "  4) Remove the old WMAgent version when possible"
echo "  $FINAL_MSG"
echo "Have a nice day!" && echo

exit 0
