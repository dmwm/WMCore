function(doc) {
  var wm_latency = null;
  if(doc.eventPercents['90'] && doc.acquireTime && doc.completedTime) {
    wm_latency = (doc.completedTime - doc.eventPercents['90'])/(doc.completedTime-doc.acquireTime)
    if (wm_latency < 0 || wm_latency > 1) {
      wm_latency = null;
    }
  }

  var wf_latency = null;
  if (doc.eventPercents['90'] && doc.acquireTime && doc.announcedTime) {
    wf_latency = (doc.announcedTime - doc.eventPercents['90'])/(doc.announcedTime-doc.acquireTime)
    if (wf_latency < 0 || wf_latency > 1) {
      wf_latency = null;
    }
  }

  var wm_time = null;
  if (doc.acquireTime && doc.completedTime) {
    wm_time = (doc.completedTime-doc.acquireTime)/3600.0;
  }

  var wf_time = null;
  if (doc.acquireTime && doc.announcedTime) {
    wf_time = (doc.announcedTime-doc.acquireTime)/3600.0;
  }


  if (wm_latency || wf_latency) {
    emit([doc._id, doc._rev], [doc.type, doc.status, doc.priority, wf_latency, wm_latency, wf_time, wm_time, doc.updateTime]);
  }
}
