<form id="options_update" method="post" action='${url(controller='plot',action='plotopt',id=c.id, return_array_id=c.return_id)}' return false;">
${h.literal(c.fs.render())}
<input type="submit" value="Edit"/>
</form>


