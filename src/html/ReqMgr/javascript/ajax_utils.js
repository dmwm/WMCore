function ajaxRequest(path, parameters) {
    // base is a URL base, e.g. https://cmsweb.cern.ch
    // method is request method, e.g. /request
    // ids are request ids which needs to be approved
    new Ajax.Updater('response', path,
    { method: 'post' ,
      parameters : parameters,
      onException: function() {return;},
      onComplete : function() {return;}
    });
}
function ajaxScript(base) {
    // base is a URL base, e.g. https://cmsweb.cern.ch
    // get placehoder for code textarea
    var cid=document.getElementById('code');
    // get value of snippets drop-down menu (see create.tmpl)
    var val=document.getElementById('snippets').value;
    new Ajax.Updater('response', base+'/scripts',
    { method: 'get' ,
      parameters : {'name': val},
      onException: function() {return;},
      onSuccess : function(response) {
          // update code textarea with returned value of script server API
          cid.value=response.responseText;
      }
    });
}
