#!/bin/sh

HOST=`hostname`
DATENOW=`date +%s`
COMPONENTS="ErrorHandler JobSubmitter AgentStatusWatcher"
for comp in $COMPONENTS; do
  COMPLOG=/data/srv/wmagent/current/install/wmagent/$comp/ComponentLog
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
    /data/srv/wmagent/current/config/wmagent/manage execute-agent wmcoreD --restart --components=$comp
    echo -e "ComponentLog quiet for $INTERVAL secs\n\nTail of the log is:\n$TAIL_LOG" |
      mail -s "$HOST : $comp restarted" alan.malta@cern.ch
  fi
done

