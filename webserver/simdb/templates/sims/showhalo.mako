<%inherit file="/base.mako"/> \

<%def name="header()">${c.name}<small> N<sub>DM</sub>=${c.ndm}; N<sub>Bar</sub>=${c.nbar}</small></%def>

<%def name="breadcrumbs()">
${h.link_to('simdb',url(controller='sims',action='index'))} | ${h.link_to('simulation',url(controller='sims',action='showsim',id=c.simulation_id))}  | ${h.link_to('timestep',url(controller='sims',action='showstep',id=c.timestep_id))}
</%def>

<script type="text/javascript">
serialize = function(obj) {
  var str = [];
  for(var p in obj)
     str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
  return str.join("&");
}

function gograph(base_id)
{
	var query = $('#myform').serialize();
	objImg = new Image();
	objImg.src = base_id+"?"+query;
	objImg.onload = function() {
		$('#imgbox').empty()
		$('#imgbox').append(objImg);
		$('#imgbox').css('width',objImg.width);
		// $('liveplot').src = objImg.src;
	}
	objImg.onerror = function() {
		errorlink = '<a href="'+objImg.src+'" target="_blank">';
	        $('#imgbox').empty().html("<h2>Sorry, there was an error generating your plot.</h2><p>Click "+errorlink+"here</a> for more information (opens in a new window)");
	}

	$('#imgbox').empty().html("<img src='/spinner.gif' />&nbsp;Generating plot...");
	return true;
}

var interpret_name = "";
var interpret_axis ="";

function ar_interp(name,axis)
{
	interpret_name = name;
	interpret_axis = axis;
	$('#popup_parent').css('visibility','visible');
	$('#interpret_box').css('visibility','visible');
	$('#whattoplot').val("");
	$('#whattoplot').focus();
}

function ar_interp_finish()
{
	$('#popup_parent').css('visibility','hidden');
	$('#interpret_box').css('visibility','hidden');

	$("#RI"+interpret_axis+"_"+interpret_name).val(interpret_name+"//"+$('#whattoplot').val());
}

function moveScroller() {
    var move = function() {
        var st = $(window).scrollTop();
        var s = $("#imgbox");
        if(st > 200) {
            s.css({
                position: "fixed",
                top: "0px",
		right: "0px"
            });
        } else {
            s.css({
		  position: "absolute",
                  top: "200px",
		  right: "0px",
		  z_index: "-1"
            });
        }
    };
    $(window).scroll(move);
    move();
}

moveScroller();

$('.collapsibletable').click(function(){
    $(this).nextUntil('tr.header').slideToggle(1000);
});

</script>


% for f in c.flash :
<p style="color:#f00;">${f}</p>
%endfor

<h2>Time sequence</h2>

<p>NEW: ${h.link_to('merger tree', url(controller='sims', action='mergertree', id=c.this_id))}</p>
<p>At z=${"%.2f"%c.timestep_z}, t=${c.timestep_t}; show this halo in another step (if available):
% for rel in ["earliest","-10","earlier","later","+10","latest"] :
%if rel=="-10" :
${h.link_to(rel,url(controller='sims', action='showhalo',id=c.this_id,rel="earlier",num=10))}
%elif rel=="+10" :
${h.link_to(rel,url(controller='sims', action='showhalo',id=c.this_id,rel="later",num=10))}
%else :
${h.link_to(rel,url(controller='sims', action='showhalo',id=c.this_id,rel=rel))}
%endif

%endfor

${c.bh}

<p>

<div class="popup_parent" id="popup_parent"></div>
<div class="popup_box" id="interpret_box">
<p>You have selected to plot quantities from an array. Which element (starting at 0) do you want to plot?
You can also type + to extract the maximum, or - to extact the minimum.</p>
<form onsubmit="ar_interp_finish(); return false;">
<input id="whattoplot" name="whattoplot" type="text" size=4>
<input type="submit" value="Continue">
</form>
</div>

<h2>Properties</h2>

<p>
<form id="myform" action="${url(controller='plot', action='xy_img', id='txt')}" target="_blank">
<input type="hidden" name="timestep_id" value="${c.timestep_id}">
<input type="hidden" name="halo_id" value="${c.this_id}" >
<table>
<tr class="collapsibletable"><th>x</th><th>y</th><th>Plot</th><th>Property</th><th>Creator</th></tr>
<tr><td><input name="x" type="radio" value="t" checked/></td><td><input name="y" type="radio" value="t"/></td><td> </td><td>time =${c.timestep_t}</td> <td class="smallinfo"></td></tr>
<tr><td><input  name="x" type="radio" value="z"/></td><td><input name="y" type="radio" value="z"/></td><td> </td><td>redshift =${c.timestep_z}</td> <td class="smallinfo"></td></tr>
${c.props}
</table>
<div>
<div id="imgspace">

<p id="imgbox">Plots will appear here</p>

<h2>x vs y plot</h2>
<div>First select what your x and y variables are above, then choose:<br/>
<input type="radio" name="type" value="thishalo" checked>x vs y for this halo (or its major progenitors) over all time</input><br/>
<input type="radio" name="type" value="thistimestep">x vs y for all halos in this timestep</input>, optionally <input type="checkbox" name="nosubs" id="nosubs" checked>excluding subhalos</input><br/>
</div>
<div>
</div>
<div>
<input type="checkbox" name="xlog" id="xlog">X logarithmic</input><input type="checkbox" name="ylog" id="ylog">Y logarithmic</input>
</div><div>
<input type="submit" value="Plot!" onclick="gograph('${url(controller='plot', action='xy_img')}'); return false;" />
<input type="submit" href="gograph('); return false;" value="Get!" />
</div>
</div>

</form>


<!--<p>Plot <a href="#" onclick="gograph('${url(controller='plot', action='xy_img', id=c.timestep_id)}'); return false;">x vs y</a> for this timestep</p>
<p>Plot <a href="${url(controller='plot', action='xy_img', id="h"+str(c.this_id))}" target="blank">x vs y</a> for this halo at all timesteps</p>
-->
% if len(c.dep_props)>0 :
<h2> Deprecated Properties</h2>
<table>
${c.dep_props}
</table>
% endif
<h2>Links</h2>
${c.links}
