<%inherit file="/base.mako"/> \

<%def name="header()">${c.basename}</%def>

<div style="float: right;">
<table>
<tr><th>Property</th><th>Value</th></tr>
% for (p,v),cn in zip(c.props, h.oddeven()) :
<tr class="${cn}"><td>${p}</td><td>${v}</td></tr>
% endfor
</table>
<small>${c.creator}</small>

</div>


<table class="sortable draggable">
<tr><th>Redshift</th><th>Time</th><th>n_halos</th><th>filename</th><th>Go</th></tr>
% for t,cn in zip(c.timesteps, h.oddeven()):
<%
ext = t.extension

if len(ext)>7 : 
  ext="..."+ext[-5:] 

if t.available :
  cl = "step_available"
else :
  cl = "step_unavailable"
  ext+=" unavailable "
%>
<tr class="${cn}">
<td>
${'%.2f'%t.redshift}
</td>
<td sorttable_customkey="${'%.3f'%t.time_gyr}">
% if t.time_gyr < 1 :
${'%.0f Myr'%(1e3*t.time_gyr)}
% else:
${'%.2f Gyr'%(t.time_gyr)}
% endif
</td>
<td>
${'%d'%t.halos.count()}
</td>
<td>
${ext}
</td>
<td class="unsortable">
${h.link_to("TS",url(controller='sims',action='showstep',id=t.id))}
</td>
</tr>
%endfor
</table>



