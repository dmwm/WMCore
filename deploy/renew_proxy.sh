#!/bin/bash
credname=CREDNAME

source /data/admin/wmagent/env.sh
### FNAL and CERN have different versions of voms-proxy-init
if [[ "$credname" == amaltaroCERN ]]; then
    source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
fi
myproxy-get-delegation -v -l amaltaro -t 168 -s 'myproxy.cern.ch' -k $credname -n \
  -o /data/certs/mynewproxy.pem && voms-proxy-init -rfc -voms cms:/cms/Role=production -valid 168:00 -bits 2048 \
  -noregen -cert /data/certs/mynewproxy.pem -key /data/certs/mynewproxy.pem -out /data/certs/myproxy.pem
