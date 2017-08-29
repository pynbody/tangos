var pre_nav_callbacks = [];
var nav_callbacks = [];


function updateElementsFromResponse(data) {

    pre_nav_callbacks.forEach(function(fn) {
        fn();
    });

    playdata = data;
    $(".dynamic-update").each(function() {
        var elementId = '#'+this.id;
        var newData = $(data).find(elementId);
        console.info(elementId,newData);
        if(newData.length>0) $(this).replaceWith(newData);
        else console.info("(no action: was null)");
    })


    updatePositionsAfterScroll();

    nav_callbacks.forEach(function(fn) {
        fn();
    });

}


function prePageUpdate(fn) {
    // Register a callback to occur before a page update is performed
    pre_nav_callbacks.push(fn)
}

function postPageUpdate(fn) {
    // Register a callback to occur immediately after a page update is performed
    nav_callbacks.push(fn)
}


function ajaxNavigate(rel, isPoppingFromHistory) {

    $("#navigation").html("<h2>Loading...</h2>");

    $.ajax({
           type: "GET",
           url: rel,
           beforeSend: function(){ },
           dataType: "html",
           success: updateElementsFromResponse
       });


    if(!isPoppingFromHistory) {
        console.log("push:"+rel);
        window.history.pushState(rel, "AJAX", rel);
    }

    return false;
}

function ajaxEnableLinks() {
    $('a.ajaxenabled').click(function() {
        ajaxNavigate(this.href);
        return false;
    })
}


function popState(event) {
    console.log("pop:"+event.state);
    if(event.state)
        ajaxNavigate(event.state, true);
}

window.onpopstate = popState;
