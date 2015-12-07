function(doc) {
	
  function retrydoneStatusHandler(stateIndex) {
      var status;
      if (doc['states'][stateIndex].oldstate == 'jobfailed' ||
          doc['states'][stateIndex].oldstate == 'jobcooloff' ||
          doc['states'][stateIndex].oldstate == 'jobpaused') {
          status = 'failure_exception';
	  } else if (doc['states'][stateIndex].oldstate == 'submitfailed' ||
	      doc['states'][stateIndex].oldstate == 'submitcooloff' ||
	      doc['states'][stateIndex].oldstate == 'submitpaused') {
	      status = 'failure_submit';
	  } else if (doc['states'][stateIndex].oldstate == 'createfailed' ||
	      doc['states'][stateIndex].oldstate == 'createcooloff' ||
	      doc['states'][stateIndex].oldstate == 'createpaused') {
	      status = 'failure_create';
	  } else {
	      throw "not valid transition";
	  };
	  return status;
  }
    
  function statusMap(){
      var status;
      switch (doc['states'][lastStateIndex].newstate) {
          case 'created':
              if (doc['states'][lastStateIndex].oldstate == 'new') {
                  status = 'queued_first';
              } else if (doc['states'][lastStateIndex].oldstate == 'submitpaused') {
                  status = 'queued_first';
              } else if (doc['states'][lastStateIndex].oldstate == 'submitcooloff') {
                  status = 'queued_first';
              } else if (doc['states'][lastStateIndex].oldstate == 'jobcooloff') {
                  status = 'queued_retry';
              } else if (doc['states'][lastStateIndex].oldstate == 'jobpaused') {
                  status = 'queued_retry';
              } else {
                  throw "not valid transition";
              };
              break;
          case 'createcooloff':
              status = 'cooloff_create';
              break;
          case 'submitcooloff':
              status = 'cooloff_submit';
              break;
          case 'jobcooloff':
              status = 'cooloff_job';
              break;
          case 'createpaused':
              status = 'paused_create';
              break;
          case 'submitpaused':
              status = 'paused_submit';
              break;
          case 'jobpaused':
              status = 'paused_job';
              break;
          case 'executing':
              if (doc['states'][lastStateIndex - 1].oldstate == 'new') {
                  status = 'submitted_first';
              } else if (doc['states'][lastStateIndex - 1].oldstate == 'submitpaused') {
                  status = 'submitted_first';
              } else if (doc['states'][lastStateIndex - 1].oldstate == 'submitcooloff') {
                  status = 'submitted_first';
              } else if (doc['states'][lastStateIndex - 1].oldstate == 'jobpaused')  {
                  status = 'submitted_retry';
              } else if (doc['states'][lastStateIndex - 1].oldstate == 'jobcooloff') {
                  status = 'submitted_retry';
              } else {
                  throw "not valid transition";
              };
              break;
          // this case can be removed but in case of state transition update failure 
          case 'success':
              status = 'success';
              break;
          case 'retrydone':
              status = retrydoneStatusHandler(lastStateIndex);
              break;
          // this case can be removed but in case of state transition update failure 
          case 'exhausted':
              if (doc['states'][lastStateIndex].oldstate == 'retrydone') {
                  status = retrydoneStatusHandler(lastStateIndex - 1);
              } else {
                  throw "not valid transition";
              };
              break;
          // this case can be removed but in case of state transition update failure 
          case 'killed':
              status = 'canceled';
              break;
          case 'cleanout':
              if (doc['states'][lastStateIndex].oldstate == 'success') {
                  status = 'success';
              } else if (doc['states'][lastStateIndex].oldstate == 'killed') {
                  status = 'canceled';
              } else if (doc['states'][lastStateIndex].oldstate == 'exhausted') {
                  if (doc['states'][lastStateIndex - 1].oldstate == 'retrydone') {
                  	  status = retrydoneStatusHandler(lastStateIndex - 2);
                  } else {
                      throw "not valid transition";
                  };
              } else {
                  throw "not valid transition";
              };
              break;
          default:
              status = "transition";
      }
      return status;
  }
  
  if (doc['type'] == 'job') {
      var tmpSite = null;
      var siteLocation = null;
      var lastStateIndex = 0;
      //var lastStateIndex = doc['states'].length - 1
      //search from last state. 
      //if job is retried in different site, it will only count the last site. 
      //if inter mediate site information is needed modify code (don't break)
      
      //TODO need to get the last number by comparing the i. 11 might come first then 2
      // Is it depend on the interpreter? Otherwise this can be outside the loop
      for (var lastStateIndex in doc['states']) {
          tmpSite = doc['states'][lastStateIndex].location;
          if (tmpSite !== "Agent") {
              siteLocation  = tmpSite;
          };
      };
      if (siteLocation == null) {
          // tmpSite should be 'Agent'
          siteLocation = tmpSite;
      };
      
      emit([doc['workflow'], doc['task'], statusMap(), siteLocation], 1);
  };
}

