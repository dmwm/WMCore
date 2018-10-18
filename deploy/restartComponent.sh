#!/bin/sh

HOST=`hostname`
DATENOW=`date +%s`
COMPONENTS="ErrorHandler JobSubmitter"
for comp in $COMPONENTS; do
  LASTCHANGE=`stat -c %Y /data/srv/wmagent/current/install/wmagent/$comp/ComponentLog`
  INTERVAL=`expr $DATENOW - $LASTCHANGE`
  if (("$INTERVAL" >= 900)); then
    OTHERS=`ps aux | grep wmcore | grep -v grep`
    if [[ -z "$OTHERS" ]]; then
      echo "Since the agent is not running, don't do anything ..."
      exit 1
    fi

    . /data/admin/wmagent/env.sh
    /data/srv/wmagent/current/config/wmagent/manage execute-agent wmcoreD --restart --components=$comp
    echo "ComponentLog quiet for $INTERVAL secs" | mail -s "$HOST : $comp restarted" alan.malta@cern.ch,sryu@fnal.gov
  fi
done

