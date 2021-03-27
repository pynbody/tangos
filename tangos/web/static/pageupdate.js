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
        if(newData.length>0) $(this).replaceWith(newData);
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

    var request = $.ajax({
           type: "GET",
           url: rel,
           beforeSend: function(){ },
           dataType: "html",
           success: updateElementsFromResponse,

       });

    request.fail(function(jq, status) {
        window.location = rel;
    });


    if(!isPoppingFromHistory) {
        window.history.pushState(rel, "Tangos", rel);
    }

    return false;
}

function ajaxEnableLinks(inElement="") {
    if (inElement.length>0) inElement=inElement+" "
    $(inElement+'a.ajaxenabled').click(function() {
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
