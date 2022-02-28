#!/bin/sh

HOST=`hostname`
DATENOW=`date +%s`

# Figure whether it's a python2 or python3 agent
if [ -d "/data/srv/wmagent/current/install/wmagent" ]; then
  WMA_PATH="/data/srv/wmagent/current/install/wmagent"
else
  WMA_PATH="/data/srv/wmagent/current/install/wmagentpy3"
fi

COMPONENTS="ErrorHandler JobSubmitter AgentStatusWatcher"
for comp in $COMPONENTS; do
  COMPLOG=$WMA_PATH/$comp/ComponentLog
  LASTCHANGE=`stat -c %Y $COMPLOG`
  INTERVAL=`expr $DATENOW - $LASTCHANGE`
  if (("$INTERVAL" >= 1800)); then
    OTHERS=`ps aux | grep wmcore | grep -v grep`
    if [[ -z "$OTHERS" ]]; then
      echo "Since the agent is not running, don't do anything ..."
      exit 1
    fi

    . /data/admin/wmagent/env.sh
    TAIL_LOG=`tail -n100 $COMPLOG`
    $WMA_PATH/manage execute-agent wmcoreD --restart --components=$comp
    echo -e "ComponentLog quiet for $INTERVAL secs\n\nTail of the log is:\n$TAIL_LOG" |
      mail -s "$HOST : $comp restarted" alan.malta@cern.ch,todor.trendafilov.ivanov@cern.ch
  fi
done

