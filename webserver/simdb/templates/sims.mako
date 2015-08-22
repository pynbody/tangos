<%inherit file="base.mako"/>

<%def name="header()">
Index
</%def>

<small>Go to <a href="${h.url(controller="creator")}">runs</a></small>

<table class="draggable sortable">
<tr>
% for x in c.titles :
<th>${x}</th>
% endfor
</tr>
% for ss,classname in zip(c.sims,h.oddeven()) :
<tr class="${classname}">
% for d in ss["table"] :
<td>${d}</td>
% endfor
</tr>
% endfor
</table>