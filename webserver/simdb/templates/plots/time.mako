<%inherit file="/base.mako"/> \

<%def name="header()">Plot of ${c.name} against redshift</%def>


<p>
<img src = ${url(controller='plot', action='time_img', id=c.id)} />
</p>

<p>
[Back to ${h.link_to('parent halo', url(controller='sims', action='showhalo', id=c.haloid))}]
</p>

<form id="options_update" method="post" action="${url(controller='plot', action='time_opt', id=c.id)}">
<div id="editbox">
<input name="xaxis" value="z" type="radio" checked /> Redshift
<input name="xaxis" value="t" type="radio" /> Against time
<input name="xlog" type="checkbox" />Logarithmic X
<input name="ylog" type="checkbox" />Logarithmic Y
<input type="submit" value="Update" />
</div>
</form>

</div>

