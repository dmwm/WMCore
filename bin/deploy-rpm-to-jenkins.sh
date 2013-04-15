#!/bin/bash

# Handle maintaining the RPM checkout on jenkis slaves

WMAGENT_VERSION="$1"
SCRAM_ARCH="$2"

# Make a /deploy somewhere common that will be found by all these jobs, so there's not like 30 deployments
if [ ! -h deploy ]; then
  rm -rf deploy
  mkdir /jenkins/deploy || true
  ln -s /jenkins/deploy deploy
fi

if [ ! -h install ]; then
  rm -rf install
  mkdir /jenkins/install || true
  ln -s /jenkins/install install
fi

# fix -comp issue
patch -N -d cfg/Deployment -p2 < $HOME/wmagent_deploy_dash_name.patch || true

# Get the most current install on this machine
current="none"
[ `ls -d /jenkins/deploy/current/sw*/slc5_amd64_gcc461/cms/wmagent/*` ] && current=$(basename $(ls -d /jenkins/deploy/current/sw*/slc5_amd64_gcc461/cms/wmagent/*))

# Get the most recent install from the repository
if [ X$WMAGENT_VERSION == X ]; then
  WMAGENT_VERSION=$(curl -s http://cms-dmwm-builds.web.cern.ch/cms-dmwm-builds/wmagent.$SCRAM_ARCH.comp | awk '{print $4}' | cut -d+ -f3)
fi

DEVTOOLS_VERSION=$(curl -s http://cms-dmwm-builds.web.cern.ch/cms-dmwm-builds/wmcore-devtools.$SCRAM_ARCH.comp | awk '{print $4}' | cut -d+ -f3)


# stop the recursive link screwing things up (needs to be after bootstrap)
# jenkins hangs recursively scanning dir if left in
rm /jenkins/deploy/*/sw*/var || /bin/true
rm /jenkins/deploy/*/sw/var || /bin/true
for DIR in /jenkins/deploy/*/sw/jenkins; do
  unlink $DIR || /bin/true
done
unlink /jenkins/deploy/current/sw/jenkins || /bin/true
rm  /jenkins/deploy/*/sw*/jenkins || /bin/true
rm /jenkins/deploy/*/sw/jenkins || /bin/true
rm /jenkins/deploy/*/jenkins || /bin/true

# TODO: if a previous build leaves a corrupt install this will fail - how solve that - redeploy each time?
if [ X$current != X$WMAGENT_VERSION ]; then
  echo "Deploying wmagent@$WMAGENT_VERSION"
  if [ -e /jenkins/deploy/current ]; then
    echo "Stopping agent"
    /jenkins/deploy/current/config/wmagent/manage stop-agent || true
    echo "Stopping services"
    /jenkins/deploy/current/config/wmagent/manage stop-services || true
    # remove old crons
    crontab -r || true

    # be sure everything died
    set +e
    killall mysqld
    killall couchdb
    killall beam.cmp
    set -e

    # each deploy is about a gig, get rid of it
    if [ -e /jenkins/deploy/$current ]; then
      rm -rf /jenkins/deploy/$current
    fi
    if [ -e /jenkins/deploy/$WMAGENT_VERSION ]; then
      rm -rf /jenkins/deploy/$WMAGENT_VERSION
    fi
  fi

  # deploy
  $PWD/cfg/Deployment/Deploy -R wmagent@${WMAGENT_VERSION} -r comp=comp -t $WMAGENT_VERSION -A $SCRAM_ARCH -s 'prep sw post' /jenkins/deploy wmagent@${WMAGENT_VERSION} admin/devtools@${DEVTOOLS_VERSION}



  # force mysql to a reasonable size
  perl -p -i -e 's/set-variable = innodb_buffer_pool_size=2G/set-variable = innodb_buffer_pool_size=50M/' /jenkins/deploy/current/config/mysql/my.cnf
  perl -p -i -e 's/set-variable = innodb_log_file_size=512M/set-variable = innodb_log_file_size=20M/' /jenkins/deploy/current/config/mysql/my.cnf
  perl -p -i -e 's/key_buffer=4000M/key_buffer=100M/' /jenkins/deploy/current/config/mysql/my.cnf
  perl -p -i -e 's/max_heap_table_size=2048M/max_heap_table_size=100M/' /jenkins/deploy/current/config/mysql/my.cnf
  perl -p -i -e 's/tmp_table_size=2048M/tmp_table_size=100M/' /jenkins/deploy/current/config/mysql/my.cnf

  /jenkins/deploy/current/config/wmagent/manage activate-agent
fi

# stop the recursive link screwing things up (needs to be after bootstrap)
# jenkins hangs recursively scanning dir if left in
rm /jenkins/deploy/*/sw*/var || /bin/true
rm /jenkins/deploy/*/sw/var || /bin/true
for DIR in /jenkins/deploy/*/sw/jenkins; do
  unlink $DIR || /bin/true
done
unlink /jenkins/deploy/current/sw/jenkins || /bin/true
rm  /jenkins/deploy/*/sw*/jenkins || /bin/true
rm /jenkins/deploy/*/sw/jenkins || /bin/true
rm /jenkins/deploy/*/jenkins || /bin/true

# make a simple secrets file if it doesn't exists
rm $HOME/WMAgent.secrets || true
if [ ! -e $HOME/WMAgent.secrets ]; then
  echo "MYSQL_USER=test" >> $HOME/WMAgent.secrets
  echo "MYSQL_PASS=test" >> $HOME/WMAgent.secrets
  echo "COUCH_USER=test" >> $HOME/WMAgent.secrets
  echo "COUCH_PASS=test" >> $HOME/WMAgent.secrets
  echo "COUCH_PORT=5984" >> $HOME/WMAgent.secrets
  echo "COUCH_HOST=127.0.0.1" >> $HOME/WMAgent.secrets
  echo "WORKLOAD_SUMMARY_HOSTNAME=127.0.0.1" >> $HOME/WMAgent.secrets
  echo "WORKLOAD_SUMMARY_PORT=5984" >> $HOME/WMAgent.secrets
  echo "WORKLOAD_SUMMARY_DBNAME=workload_summary" >> $HOME/WMAgent.secrets
fi


# load libeatmydata to speed up the databases
if [ ! -e /jenkins/additionalCode/eatmydata ]; then
  mkdir -p /jenkins/additionalCode/eatmydata
  ( cd /jenkins/additionalCode/eatmydata ; git clone git://github.com/dmwm/libeatmydata.git . ; make )
fi

# divorce mysql and couchdb processes so they dont get killed
# https://wiki.jenkins-ci.org/display/JENKINS/ProcessTreeKiller

# if the deploy fails, we need to go crazy and delete everything
set +e
BUILD_ID=dontKillMe LD_PRELOAD=/jenkins/additionalCode/eatmydata/libeatmydata.so /jenkins/deploy/current/config/wmagent/manage start-services
if [ $? -ne 0 ]; then
  # Get the most recent install from the repository
  if [ X$WMAGENT_VERSION == X ]; then
    WMAGENT_VERSION=$(curl -s http://cms-dmwm-builds.web.cern.ch/cms-dmwm-builds/wmagent.$SCRAM_ARCH.comp | awk '{print $4}' | cut -d+ -f3)
  fi



  echo "Stopping agent"
  /jenkins/deploy/current/config/wmagent/manage stop-agent
  echo "Stopping services"
  /jenkins/deploy/current/config/wmagent/manage stop-services
  # remove old crons
  crontab -r || true

  # be sure everything died
  set +e
  killall mysqld
  killall couchdb
  killall beam.cmp
  set -e

  # each deploy is about a gig, get rid of it
  if [ -e /jenkins/deploy/$current ]; then
    rm -rf /jenkins/deploy/$current
  fi
  if [ -e /jenkins/deploy/$WMAGENT_VERSION ]; then
    rm -rf /jenkins/deploy/$WMAGENT_VERSION
  fi
  echo "FATAL: Couldn't start/deploy required services. Exiting."
  exit 3
fi

