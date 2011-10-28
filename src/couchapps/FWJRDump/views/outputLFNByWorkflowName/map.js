function(doc) {
    if (doc['type'] == 'fwjr') {
        if (doc['fwjr'].task == null) {
            return;
        }

        var request = doc['fwjr'].task.split('/')[1];
        var res = '';

        for (var stepName in doc['fwjr']['steps']) {
            var stepOutput = doc['fwjr']['steps'][stepName]['output'];
            for (var outputModuleName in stepOutput) {
                var outputFile = stepOutput[outputModuleName][outputFileIndex];
                for (var outputFileIndex in stepOutput[outputModuleName]) {
                    if (stepName=='asyncStageOut1' && outputFile['lfn']!='') {
                        emit([request], {"jobid" : doc['jobid'], "lfn" : outputFile['lfn'], "location" : outputFile['location'], "checksums" : outputFile['checksums']});
                    }
                    else if (stepName=='cmsRun1' && outputFile['lfn']!='' && asyncFound==false) {
                        res = {"jobid" : doc['jobid'], "lfn" : outputFile['lfn'], "location" : outputFile['location'], "checksums" : outputFile['checksums']};
                    }
                }
            }
            if (res != '') {
                emit([request], res);
            }
        }
    }
}
