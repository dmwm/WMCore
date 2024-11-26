cd /data/srv/wmagent/current

# exporting the variables to the enviroment
export WMAGENT_SECRETS_LOCATION=/data/admin/wmagent/WMAgent.secrets
export install=/data/srv/wmagent/current/install/wmagent
export config=/data/srv/wmagent/current/config/wmagent
export manage=$config/manage

alias condorq='condor_q -format "%i." ClusterID -format "%s " ProcId -format " %i " JobStatus  -format " %d " ServerTime-EnteredCurrentStatus -format "%s" UserLog -format " %s\n" DESIRED_Sites'
alias condor_overview='python ~/condor_overview.py'
