import subprocess
import json

def condorChirpAttrDelayed(key, value):
    """
    Call condor_chirp and publish the key/value pair
    """
    condorChirpBin = None
    condorConfig = os.getenv('CONDOR_CONFIG', None)
    if condorConfig:
        condorConfigDir = os.path.dirname(condorConfig)
        condorChirpBin = os.path.join(condorConfigDir, 'main/condor/libexec/condor_chirp')
        if not os.path.isfile(condorChirpBin):
            condorChirpBin = None

    if condorChirpBin:
        args = [ condorChirpBin, 'set_job_attr_delayed', key, json.dumps(value) ]
        returncode = subprocess.call(args)
    return

