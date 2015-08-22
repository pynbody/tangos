<%inherit file="/base.mako"/> \

<%def name="header()">Plot options</%def>

<form method="post">
${h.literal(c.fs.render())}
<input type="submit" value="Edit"/>
</form>


