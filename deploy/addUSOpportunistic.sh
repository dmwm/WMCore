#!/bin/sh
PEND_JOBS=3000
RUNN_JOBS=3000
# default manage location
manage=/data/srv/wmagent/current/config/wmagent/manage
for site in {T3_US_NERSC,T3_US_OSG,T3_US_PSC,T3_US_SDSC,T3_US_TACC,T3_US_Anvil,T3_US_Lancium};
do
  echo "Adding site: $site into the resource-control with $PEND_JOBS pending and $RUNN_JOBS running slots"
  $manage execute-agent wmagent-resource-control --site-name=$site --cms-name=$site --ce-name=$site --pnn=$site --plugin=SimpleCondorPlugin  --pending-slots=1000 --running-slots=1000;
  $manage execute-agent wmagent-resource-control --site-name=$site --task-type=Processing --pending-slots=0 --running-slots=0;
  $manage execute-agent wmagent-resource-control --site-name=$site --task-type=Production --pending-slots=0 --running-slots=0;
  $manage execute-agent wmagent-resource-control --site-name=$site --task-type=Merge --pending-slots=500 --running-slots=500;
  $manage execute-agent wmagent-resource-control --site-name=$site --task-type=LogCollect --pending-slots=500 --running-slots=500;
  $manage execute-agent wmagent-resource-control --site-name=$site --task-type=Cleanup --pending-slots=500 --running-slots=500;
  $manage execute-agent wmagent-resource-control --site-name=$site --task-type=Skim --pending-slots=500 --running-slots=500;
  $manage execute-agent wmagent-resource-control --site-name=$site --task-type=Harvesting --pending-slots=500 --running-slots=500;
  $manage execute-agent wmagent-resource-control --site-name=$site --pending-slots=$PEND_JOBS --running-slots=$RUNN_JOBS --apply-to-all-tasks;
done
