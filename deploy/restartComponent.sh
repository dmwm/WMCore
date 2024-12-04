#!/bin/bash
### Script to check the tail of each WMAgent component and evaluate
# whether they are running or not, based on file meta-data (stat).
# Component is automatically restarted if deemed down.
# NOTE that this script may not catch multi-thread components down,
# when only one of the threads is down.
###

HOST=$(hostname)
DATENOW=$(date +%s)
DEST_NAME=cms-wmcore-team

[[ -z $WMA_INSTALL_DIR ]] && { echo "ERROR: Trying to run without having the full WMAgent environment set!";  exit 1 ;}

echo -e "\n###Checking agent logs at: $(date)"
comps=$(manage execute-agent wmcoreD --status |awk '{print $1}' |awk -F \: '{print $2}')
for comp in $comps; do
  COMPLOG=$WMA_INSTALL_DIR/$comp/ComponentLog
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
    echo -e "Restarting component: $comp"
    manage execute-agent wmcoreD --restart --components=$comp
    echo -e "ComponentLog quiet for $INTERVAL secs\n\nTail of the log is:\n$TAIL_LOG" |
      mail -s "$HOST : $comp restarted" $DEST_NAME@cern.ch
  fi
done

