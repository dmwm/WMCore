#!/bin/sh
## Pass the component names as command line arguments, e.g.:
## ./restartComponent.sh ErrorHandler JobSubmitter AgentStatusWatcher

HOST=`hostname`
DATENOW=`date +%s`
AMTOOL=/data/admin/wmagent/amtool
AMURL=http://cms-monitoring.cern.ch:30093
EXPIRE=`date -d '+5 min' --rfc-3339=ns | tr ' ' 'T'`
if [ ! -f "$AMTOOL" ]; then
  echo "Could not find amtool at path: ${$AMTOOL}"
  exit 1
fi

# Get a few environment variables in, like $install and $manage
source /data/admin/wmagent/env.sh
# Figure whether it's a python2 or python3 agent
if [ ! -d "$install" ]; then
  install="/data/srv/wmagent/current/install/wmagentpy3"
fi

echo "List of components to be monitored: $@"
for comp in $@; do
  COMPLOG=$install/$comp/ComponentLog
  echo "Checking logs from: $COMPLOG"
  LASTCHANGE=`stat -c %Y $COMPLOG`
  INTERVAL=`expr $DATENOW - $LASTCHANGE`
  if (("$INTERVAL" >= 1800)); then
    OTHERS=`ps aux | grep wmcore | grep -v grep`
    if [[ -z "$OTHERS" ]]; then
      echo "Since the agent is not running, don't do anything ..."
      exit 2
    fi

    TAIL_LOG=`tail -n 50 $COMPLOG`
    echo -e "ComponentLog for $comp quiet for $INTERVAL secs\n"
    $manage execute-agent wmcoreD --restart --components=$comp
    ALERT_NAME="WMAgent_${HOST}_${comp}"
    ${AMTOOL} alert add wmagent_component_alert \
      alertname=${ALERT_NAME} severity=medium tag=wmagent alert=amtool \
      --annotation=summary="Component ${comp} was quiet for ${INTERVAL} seconds. Restarted." \
      --annotation=date="${DATENOW}" \
      --annotation=hostname="${HOST}" \
      --annotation=log="${TAIL_LOG}" \
      --end="${EXPIRE}" \
      --alertmanager.url="${AMURL}"
  fi
done

