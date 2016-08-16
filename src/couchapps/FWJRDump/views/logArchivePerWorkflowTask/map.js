function (doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var task = doc.fwjr.task.split('/');
    var taskType = task.slice(-1)[0];
    var isLogCollect = taskType.indexOf('LogCollect');
    if (isLogCollect >= 0) {
        var logCollectStep = doc.fwjr.steps.logCollect1;
        if ((logCollectStep != null) && (logCollectStep.output.LogCollect != undefined)) {
            var logArchivePFN = logCollectStep.output.LogCollect[0].lfn;
            emit(doc.fwjr.task, logArchivePFN);
        }
    }
  }
}
