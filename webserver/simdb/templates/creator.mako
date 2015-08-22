<%inherit file="base.mako"/>

<%def name="header()">
Creators
</%def>


<table>
<tr>
<th>User</th><th>Host</th><th>Time</th><th>Command</th><th>Link</th>
</tr>
% for x,y in zip(c.creators, h.oddeven()) :
<tr class="${y}">
<td>${x["username"]}</td><td>${x["host"]}</td><td>${x["time"]}</td><td><code>${x["command"]}</code></td><td><a href="${x["link"]}">Run ${x["id"]}</a></td>
</tr>
% endfor
</table>
