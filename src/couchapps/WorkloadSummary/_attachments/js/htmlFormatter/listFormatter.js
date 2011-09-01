keyFormatter = function (key, keyMap){
        if (keyMap && keyMap[key]){
            return keyMap[key];
        } else {
            return key;
        }
    }

valueFormatter = function(func, values, key, valueMap, keyMap) {
    if (valueMap && valueMap[key]) {
        return valueMap[key](values, func, keyMap);
    } 
    return null;
}
listFormatter = function(data, valueMap, keyMap) {
    var outputHtml = "";
    var formattedVal = null;
    //data is either array or object
    if (data instanceof Object){
        //array case
        if (data.length) {
            outputHtml += "<ol>";
            for (var i in data) {
                outputHtml += ("<li>" + listFormatter(data[i], valueMap, keyMap) + "</li>");
            };
            outputHtml += "</ol>";
        } else {
            outputHtml += "<ul>";
            for (var name in data) {
                outputHtml += ("<li> <b>" + keyFormatter(name, keyMap) + "</b>");
                formattedVal = valueFormatter(listFormatter, data[name], name,  valueMap, keyMap)
                if (formattedVal !== null) {
                    outputHtml += formattedVal;
                } else {
                    outputHtml += listFormatter(data[name], valueMap, keyMap);
                }
                outputHtml += "</li>";
            };
            outputHtml += "</ul>";
        }
    } else {
        outputHtml = ":   " + data;
    };
    return outputHtml;
}


/** custom format for output data **/
outputKeyMap = {'nFiles': 'files'};

outputValueMap = {};
outputValueMap.size = function(data){
        var outputHtml  = ":   "  + data + " (bytes)";
        return outputHtml;
    };

/** custom format for error data **/
errorKeyMap = {'exitCode': 'exit code', 
               'runLumis': 'run and lumi range', 
               'lumiRange' : 'lumi range'};

errorValueMap = {};
errorValueMap.lumiRange = function(data) {
        var outputHtml = ":  "
        for (var i in data) {
            outputHtml += "[" + data[i][0] + " - " + data[i][1] + "] ";
        }
        return outputHtml;
    };

errorValueMap.details = function(data){
        var outputHtml  = ":<pre> "  + data + "</pre> ";
        return outputHtml;
    };

errorValueMap.type = errorValueMap.details;



