cd /data/srv/wmagent/current

# exporting the variables to the enviroment
export WMAGENT_SECRETS_LOCATION=/data/admin/wmagent/WMAgent.secrets
export X509_HOST_CERT=/data/certs/servicecert.pem
export X509_HOST_KEY=/data/certs/servicekey.pem
export X509_USER_CERT=/data/certs/servicecert.pem
export X509_USER_KEY=/data/certs/servicekey.pem
export X509_USER_PROXY=/data/certs/myproxy.pem
export install=/data/srv/wmagent/current/install/wmagent
export config=/data/srv/wmagent/current/config/wmagent
export manage=$config/manage
export RUCIO_HOME=/data/srv/wmagent/current/config/rucio/

alias condorq='condor_q -format "%i." ClusterID -format "%s " ProcId -format " %i " JobStatus  -format " %d " ServerTime-EnteredCurrentStatus -format "%s" UserLog -format " %s\n" DESIRED_Sites'
alias condor_overview='python ~/condor_overview.py'