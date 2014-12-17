function ajaxRequest(path, parameters) {
    // path is an URI
    // parameters is JSON dict
    new Ajax.Updater('response', path,
    { method: 'post' ,
      parameters : parameters,
      onException: function() {return;},
      onComplete : function() {return;}
    });
}
