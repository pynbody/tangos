<%inherit file="/base.mako"/> \

<form method="get">
<input type='hidden' name='update' value='yes'>
<input type='checkbox' name='nosub' id='nosub' ${'checked' if session.get('nosub',False) else ''}> Hide subhalos.<br/>
<input type='checkbox' name='nocontam' id='nocontam' ${'checked' if session.get('nocontam',False) else ''}> Hide contaminated halos (>0.001%). <small>If all your halos disappear, the contaminated fractions probably haven't been entered into the DB.</small><br/>
<input type='submit' value="Update">
</form>

<%def name="header()">Halos</%def>

<p>
<span class="large">
% for t in c.halos[:10]:
${h.link_to(str(t.halo_number),url(controller='sims',action='showhalo',id=str(t.id)))}
%endfor
</span>
% for t in c.halos[10:20]:
${h.link_to(str(t.halo_number),url(controller='sims',action='showhalo',id=str(t.id)))}
%endfor
<span class="small_big">
% for t in c.halos[20:]:
${h.link_to(str(t.halo_number),url(controller='sims',action='showhalo',id=str(t.id)))}
%endfor
</span>

% if len(c.tinyhalos)>0 :
<h1>Tiny halos</h1>
<span class="small_big">
(Fewer than 5000 DM particles but more than 100)<br/>
% for t in c.tinyhalos:
${h.link_to(str(t.halo_number),url(controller='sims',action='showhalo',id=str(t.id)))}
%endfor
</span>
%endif

% if len(c.trackerhalos)>0 :
<h1>Tracker halos</h1>
% for t in c.trackerhalos :
${h.link_to(str(t.halo_number),url(controller='sims',action='showhalo',id=str(t.id)))}
% endfor

%endif
</p>
