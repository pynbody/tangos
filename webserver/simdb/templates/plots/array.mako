<%inherit file="/base.mako"/> \

<%def name="header()">Plot of ${c.name}</%def>


<p>
<img src = "${c.image_url}" class="plotimg" />
</p>


<div>Time sequence: 
${h.link_to('previous',url(controller='plot', action='array',id=c.id,rel="previous"))}
|
${h.link_to('next',url(controller='plot', action='array',id=c.id,rel="next"))}
</div>
<div id="editbox" style="display:none;"></div>

<div>
<form id="options_update" method="post" action='${url(controller='plot',action='plotopt',id=c.dictid, return_array_id=c.id)}' >
${h.literal(c.fs.render())}
<input type="submit" value="Edit"/>
</form>
</div>
<div>
[Get ${h.link_to('as text', url=url(controller='plot', action='array_img',id=c.id,text=True))}]
</div>

<script type="text/javascript">
function openedit() {

new Ajax.Request('${url(controller='plot', action='plotopt', id=c.dictid, return_array_id=c.id)}',
{
  method:'get',
  onSuccess: function(transport) {
   $('editbox').innerHTML = transport.responseText;
   Effect.BlindDown('editbox');
  }
});

return false;

}
</script>

