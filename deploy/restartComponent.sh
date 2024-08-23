#!/bin/sh
### Script to check the tail of each WMAgent component and evaluate
# whether they are running or not, based on file meta-data (stat).
# Component is automatically restarted if deemed down.
# NOTE that this script may not catch multi-thread components down,
# when only one of the threads is down.
###

HOST=$(hostname)
DATENOW=$(date +%s)
DEST_NAME=cms-wmcore-team

# Figure whether it's a python2 or python3 agent
if [ ! -d "$install" ]; then
  install="/data/srv/wmagent/current/install/"
fi

echo -e "\n###Checking agent logs at: $(date)"
comps=$(ls $install)
for comp in $comps; do
  COMPLOG=$install/$comp/ComponentLog
  if [ ! -f $COMPLOG ]; then
    echo "Not a component or $COMPLOG does not exist"
    continue
  fi
  echo "Checking logs from: $COMPLOG"
  LASTCHANGE=$(stat -c %Y $COMPLOG)
  INTERVAL=$(expr $DATENOW - $LASTCHANGE)
  if (("$INTERVAL" >= 1800)); then
    OTHERS=$(ps aux | grep wmcore | grep -v grep)
    if [[ -z "$OTHERS" ]]; then
      echo "Since the agent is not running, don't do anything ..."
      exit 1
    fi

    TAIL_LOG=$(tail -n100 $COMPLOG)
    $manage execute-agent wmcoreD --restart --components=$comp
    echo -e "ComponentLog quiet for $INTERVAL secs\n\nTail of the log is:\n$TAIL_LOG" |
      mail -s "$HOST : $comp restarted" $DEST_NAME@cern.ch
  fi
done

