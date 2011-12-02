import json
import time, datetime
import random, string

from uuid import uuid1
from optparse import OptionParser

from WMCore.Database.CMSCouch import CouchServer

def parse_opts():
  parser = OptionParser()
  parser.add_option("-u", "--users",
                  dest="users",
                  default=10,
                  type="int",
                  help="The number of users, default=10")
  parser.add_option("-s", "--sites",
                  dest="sites",
                  default=5,
                  type="int",
                  help="The number of sites, default=5")
  parser.add_option("-a", "--agents",
                  dest="agents",
                  default=2,
                  type="int",
                  help="The number of agents, default=2")
  parser.add_option("-i", "--iterations",
                  dest="iterations",
                  default=5,
                  type="int",
                  help="The number of iterations to make, default=5")
  parser.add_option("-r", "--requests",
                  dest="requests",
                  default=5,
                  type="int",
                  help="The number of requests to simulate, default=5")
  parser.add_option("-w", "--wait",
                  dest="wait",
                  default=0,
                  type="int",
                  help="Wait W seconds between iterations, default=0")


  return parser.parse_args()[0]

def rand_str(size):
  return ''.join(random.choice(string.ascii_lowercase) for x in xrange(size))

def generate_requests(number=5, agents=2, users=10):
  requests = []
  for i in xrange(number):
    r ={
      "type": "request", # probably redundant
      "workload": "workflow-%s" % rand_str(10),
      "input_dataset": "/%s/%s/RECO" % (rand_str(10), rand_str(20)),
      "owner": "person-%s" % random.randint(0, users), # a user or a team
      "agent": "agent-%s" % random.randint(0, agents),
      "total_jobs": random.randint(0, 1000) # maybe only needed for the raw request object?
    }
    requests.append(r)
  return requests

def generate_sites(request):

  sites = [ 'T2_AT_Vienna', 'T2_BE_IIHE', 'T2_BE_UCL', 'T2_BR_SPRACE',
            'T2_BR_UERJ', 'T2_CH_CAF', 'T2_CH_CSCS', 'T2_CN_Beijing', 'T2_DE_DESY',
            'T2_DE_RWTH', 'T2_EE_Estonia', 'T2_ES_CIEMAT', 'T2_ES_IFCA',
            'T2_FI_HIP', 'T2_FR_CCIN2P3', 'T2_FR_GRIF_IRFU', 'T2_FR_GRIF_LLR',
            'T2_FR_IPHC', 'T2_HU_Budapest', 'T2_IN_TIFR', 'T2_IT_Bari',
            'T2_IT_Legnaro', 'T2_IT_Pisa', 'T2_IT_Rome', 'T2_KR_KNU', 'T2_PK_NCP',
            'T2_PL_Cracow', 'T2_PL_Warsaw', 'T2_PT_LIP_Lisbon', 'T2_PT_NCG_Lisbon',
            'T2_RU_IHEP', 'T2_RU_INR', 'T2_RU_ITEP', 'T2_RU_JINR', 'T2_RU_PNPI',
            'T2_RU_RRC_KI', 'T2_RU_SINP', 'T2_TR_METU', 'T2_TW_Taiwan',
            'T2_UA_KIPT', 'T2_UK_London_Brunel', 'T2_UK_London_IC',
            'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP', 'T2_US_Caltech',
            'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue',
            'T2_US_UCSD', 'T2_US_Wisconsin']
  if sites not in request.keys():
    request["sites"] = {}
    # jobs run at 1-10 sites
    req_sites = random.sample(sites, random.randint(1, 10))
    # can't use a defaultdict because it doesn't thunk
    for site in req_sites:
      request["sites"][site] = {}

  status = {}
  status.update(request['status'])

  for site in request["sites"]:
    for k, v in status.items():
      j = random.randint(0, v)
      request["sites"][site][k] = j
      status[k] -= j

  # Mop up - must be a better way to do this...
  site = request["sites"].keys()[-1]
  for k, v in status.items():
    request["sites"][site][k] += v

def generate_status(request):
  if 'status' not in request.keys():
    # Make some initial status
    njobs = request['total_jobs']
    # TODO running split by attempts
    # TODO "inWorkQueue", "inWMBS"
    request['status'] = {}
    for state in ["queued", "pending", "running", "cooloff", "success", "failure"]:
      sjobs = random.randint(0, njobs)
      request['status'][state] = sjobs
      njobs = njobs - sjobs
    request['status'][state] += njobs
  elif request['status']["success"] + request['status']["failure"] == request['total_jobs']:
    # Request is complete
    return False
  else:
    # propagate state
    released = random.randint(0, request['status']["queued"])
    request['status']["queued"] -= released
    cooled = random.randint(0, request['status']["cooloff"])
    request['status']["cooloff"] -= cooled
    released += cooled
    for state in ["pending", "running"]:
      # add jobs from queue
      sjobs = random.randint(0, released)
      request['status'][state] += sjobs
      released = released - sjobs
      # subtract done jobs
      done = random.randint(0, request['status'][state])
      request['status'][state] -= done
      success = random.randint(0, done)
      request['status']["success"] += success
      request['status']["failure"] += done - success
    # jobs that didn't propagate can go into cool off
    request['status']["cooloff"] += released

  generate_sites(request)
  # TODO: errors
  return True

def start_clock(iterations):
  difference = iterations * datetime.timedelta(minutes=15)
  weeks, days = divmod(difference.days, 7)
  minutes, seconds = divmod(difference.seconds, 60)
  hours, minutes = divmod(minutes, 60)

  print "Running %s iterations " % iterations
  print "Equivalent to running for %s weeks, %s days, %s hours, %s minutes" % (weeks, days, hours, minutes)

  now = datetime.datetime.now()
  dt = datetime.timedelta(minutes=15)

  return now, dt

def main(options):
  # TODO make the server and db an option
  db = CouchServer().connectDatabase('wmagent-status')
  t, dt = start_clock(options.iterations)
  requests = generate_requests(options.requests, options.agents, options.users)
  print "Have %s requests" % len(requests)
  for i in xrange(options.iterations):
    n_req = len(requests)
    for r in requests:
      if generate_status(r):
        r["timestamp"] = str(t + i * dt)
        db.queue(r)
      else:
        requests.remove(r)
      # TODO add some random duplication
    if len(requests) != n_req:
      print "%s requests remaining" % len(requests)
    # Commit at the end of each iteration, so the request list is stateful
    db.commit()
    time.sleep(options.wait)


if __name__ == "__main__":
	main(parse_opts())