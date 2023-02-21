#!/bin/sh
## Pass the component names as command line arguments, e.g.:
## ./restartComponent.sh ErrorHandler JobSubmitter AgentStatusWatcher

HOST=`hostname`
DATENOW=`date +%s`

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
      exit 1
    fi

    TAIL_LOG=`tail -n100 $COMPLOG`
    $manage execute-agent wmcoreD --restart --components=$comp
    echo -e "ComponentLog quiet for $INTERVAL secs\n\nTail of the log is:\n$TAIL_LOG" |
      mail -s "$HOST : $comp restarted" alan.malta@cern.ch,todor.trendafilov.ivanov@cern.ch
  fi
done

