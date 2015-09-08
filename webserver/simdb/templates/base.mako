<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN"
  "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html>
  <head>
    <title>Simulation DB</title>
 ${h.stylesheet_link('/stylesheet.css')}
<!-- <script type="text/javascript" src="${url('/scripts/prototype.js')}"></script>
<script type="text/javascript"
  src="${url('/scripts/scriptaculous.js')}"></script>
-->
<script type="text/javascript"
  src="${url('/scripts/jquery-1.4.2.min.js')}"></script>

<script type="text/javascript"
  src="${url('/scripts/sorttable.js')}"></script>
<script type="text/javascript" src="${url('/scripts/dragtable.js')}"></script>
<script type="text/javascript">

</script>

  </head>

  <body>
    <div class="content">
      <div id="breadcrumbs-top" class="dynamic-update">${c.breadcrumbs}</div>
      <h1 class="main dynamic-update" id="header">${self.header()}</h1>
      ${next.body()}
      <div id="breadcrumbs-bottom" class="dynamic-update">${c.breadcrumbs}</div>
    </div>
    <div><small>halodb by <a href="mailto:a.pontzen@ucl.ac.uk">Andrew Pontzen</a></small></div>
  </body>
</html>

