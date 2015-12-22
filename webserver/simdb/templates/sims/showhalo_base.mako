<%inherit file="/base.mako"/> \

<%def name="header()">${c.name}<small> N<sub>DM</sub>=${c.ndm}; N<sub>Bar</sub>=${c.nbar}</small></%def>

<%def name="breadcrumbs()">
${h.link_to('simdb',url(controller='sims',action='index'))} | ${h.link_to('simulation',url(controller='sims',action='showsim',id=c.simulation_id))}  | ${h.link_to('timestep',url(controller='sims',action='showstep',id=c.timestep_id))}
</%def>

<script type="text/javascript">


var nav_callbacks = []

function updateElementsFromResponse(data) {
    // $('#myform').replaceWith($(data).find('#myform'));

    playdata = data;
    $(".dynamic-update").each(function() {
        var elementId = '#'+this.id;
        var newData = $(data).find(elementId);
        console.info(elementId,newData);
        if(newData.size()>0) $(this).replaceWith(newData);
        else console.info("(no action: was null)");
    })


    updatePositionsAfterScroll();

    nav_callbacks.forEach(function(fn) {
        fn();
    });
}



function addNavigationCallback(fn) {
    nav_callbacks.push(fn)
}


function timeNav(rel, popping) {

    $("#navigation").html("<h2>Loading...</h2>");

    $("#navigation").html("<h2>Loading...</h2>");

    $.ajax({
           type: "GET",
           url: rel,
           beforeSend: function(){ },
           dataType: "html",
           success: updateElementsFromResponse
       });


    if(!popping) {
        console.log("push:"+rel);
        window.history.pushState(rel, "AJAX", rel);
    }

    return false;
}


function popState(event) {
    console.log("pop:"+event.state);
    if(event.state)
        timeNav(event.state, true);
}

window.onpopstate = popState;


var scrollTop = {};

function initScrollOffsetData() {
    console.info("initScrollOffsetData");
    $(".keeponscreen").each(function() {
        if($(this).css('position')!='absolute') {
            // generate clone that keeps the space for this element
            var clone = $(this).clone();
            clone.removeClass("keeponscreen");
            clone.attr('id', clone.attr('id') + "-placeholder");
            clone.css('visibility', 'hidden');
            $(this).after(clone);
        }
        scrollTop[this.id]=this.getBoundingClientRect().top;

    });
}

function updatePositionsAfterScroll() {
    var windowTop = $(window).scrollTop();
    var current=0;
    $(".keeponscreen").each(function() {
        if(windowTop<scrollTop[this.id]-current) {
            $(this).css({position:"absolute",
                         top: scrollTop[this.id]});
        } else {
            $(this).css({position:"fixed",
                         top: current});
        }

        clone = $("#"+this.id+"-placeholder");
        if (clone!=null)
            clone.css({height: this.getBoundingClientRect().height+10});

        current = this.getBoundingClientRect().bottom
    });

}

function setupScrollAdjustment() {
    initScrollOffsetData();
    $(window).scroll(updatePositionsAfterScroll);
    updatePositionsAfterScroll();

}

function findInOtherSimulation() {
    timeNav(document.forms['select-othersimulation']['target_sim_id'].value);
}


var hasInitialized = false;

$(function() {
    if(hasInitialized) return;
    hasInitialized=true;

    setupScrollAdjustment();

 });


</script>




<div class="keeponscreen dynamic-update" id="navigation">
% for f in c.flash :
<p id="flash-message">${f}</p>
%endfor

<p>At z=${"%.2f"%c.timestep_z}, t=${c.timestep_t}, dbid=${c.this_id}; show this halo in another step (if available):
% for rel in ["earliest","-10","earlier","later","+10","latest"] :
%if rel=="-10" :
    <% linkurl = url(controller='sims', action=c.this_action,id=c.this_id,rel="earlier",num=10)%>
%elif rel=="+10" :
    <% linkurl = url(controller='sims', action=c.this_action,id=c.this_id,rel="later",num=10)%>
%else :
    <% linkurl = url(controller='sims', action=c.this_action,id=c.this_id,rel=rel,num=1)%>
%endif
${h.link_to(rel,linkurl,onclick="return timeNav('"+linkurl+"');")}
%endfor
    | ${h.link_to(h.literal(c.alternative_action_name), url(controller='sims', action=c.alternative_action, id=c.this_id))}
    </p>
    <form id="select-othersimulation">
        In this timestep: jump to halo
    <% linkurl = url(controller='sims', action=c.this_action,id=c.this_id,rel="other_in_ts",num=c.halo_number-1)%>
    ${h.link_to(str(c.halo_number-1),linkurl,onclick="return timeNav('"+linkurl+"');")} |
    <% linkurl = url(controller='sims', action=c.this_action,id=c.this_id,rel="other_in_ts",num=c.halo_number+1)%>
    ${h.link_to(str(c.halo_number+1),linkurl,onclick="return timeNav('"+linkurl+"');")}.
        <label for="target_sim_id">Find the same halo in another simulation:</label>
<select name="target_sim_id" onchange="findInOtherSimulation();">
    %for sim_name, sim_id, ticked in c.sims:
    <option value="${url(controller='sims', action=c.this_action, id=c.this_id, rel='insim',num=sim_id)}" ${'selected' if ticked else ''}>${sim_name}</option>
    %endfor
</select>
</form>

</div>

${next.body()}
