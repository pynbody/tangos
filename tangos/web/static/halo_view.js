function resetRadio (nameToReset) {
  var resetDict = {}
  resetDict[nameToReset] = '--------'
  getPropertiesFormJquery().values(resetDict)
}

$.fn.markAsRowInsertPoint = function () {
  return $(this).makeEditableTemplate(addBlankRow, removeRow, updateRowData, $('#object_typetag').text())
}

function updateRowData (miniLanguageQuery, rowId) {
  plotFetchingDisabled = true
  $('#nametg-' + rowId).html("<img src='/static/spinner.gif'/>" + miniLanguageQuery)

  // Plot controls need to be in DOM immediately, then rejigged later if they are not appropriate.
  // This is so that the correct radio buttons get ticked after a page update (otherwise the
  // update happens while the DOM elements are not yet available). It also allows the state of the
  // radio buttons to be carried over when a query is updated.
  updatePlotControlElements('#plotctl-' + rowId, miniLanguageQuery, false, false, false)

  $.ajax({
    type: 'GET',
    url: $('#calculate_url').text() + uriEncodeQuery(miniLanguageQuery) + '.json',
    success: function (data) {
      var selected_row = $('#' + rowId)
      if (data.error) {
        $('#nametg-' + rowId).html("<span class='load_table_failed'>" + miniLanguageQuery + '</span>')
        $('#contents-' + rowId).html("<span class='load_table_failed'>" + data.error_class + '</span>')
        // alert(data.error_class+": "+data.error);
      } else {
        $('#nametg-' + rowId).html(miniLanguageQuery)
        $('#contents-' + rowId).html(data.data_formatted)

        // See above for why the plot controls are put in place then updated
        updatePlotControlElements('#plotctl-' + rowId, miniLanguageQuery,
          data.can_use_in_plot, data.can_use_as_filter, data.is_array)
        plotFetchingDisabled = false
        fetchPlot(true)
      }
      plotFetchingDisabled = false
    }
  })
}

function addBlankRow (after) {
  var new_name = 'custom-row-' + Math.random().toString(36).substring(7)

  $('#' + after).after("<tr id='" + new_name + "'><td class='plotcontrols' id='plotctl-" + new_name + "'></td><td class='editable' id='nametg-" + new_name + "'></td><td id='contents-" + new_name + "'></td>")
  $('#nametg-' + new_name).markAsRowInsertPoint()
  return new_name
}

function removeRow (name) {
  $('#' + name).remove()
}

function findInOtherSimulation () {
  ajaxNavigate(document.forms['select-othersimulation'].target_sim_id.value)
}

function followHaloLink () {
  ajaxNavigate(document.forms['select-othersimulation'].halo_link.value)
}

var objImg

function getPropertiesFormJquery () {
  var currentType = $('#object_typetag').text()
  return $('#properties_form_' + currentType)
}

function getFilterUri () {
  var vals = getPropertiesFormJquery().values()
  var filters = []
  for (var k in vals) {
    if (k.startsWith('filter-')) {
      filters.push('(' + k.substr(7) + ')')
    }
  }
  filters = filters.join(encodeURIComponent('&'))
  if (filters.length > 0) { filters = 'filter=' + filters }
  return filters
}

function getPlotUriOneVariable (name, extension) {
  uri = $('#cascade_url').text() + name + '.' + extension
  var plotformvals = $('#image_form').values()
  if (plotformvals.logimage) { uri += '?logimage=1' }
  return uri
}

function getPlotUriTwoVariables (name1, name2, typetag, extension) {
  var uri
  var plotformvals = $('#image_form').values()
  var plotType = plotformvals.type
  if (plotType === 'gather') { uri = $('#gather_url').text() + name1 + '/vs/' + name2 + '.' + extension } else if (plotType === 'cascade') {
    uri = $('#cascade_url').text() +
            name1 + '/vs/' + name2 + '.' + extension
  }

  return uri + '?' + $('#image_form').serialize() + '&' + getFilterUri() + '&object_typetag=' + typetag
}

var plotFetchingDisabled = false
var existingImgSrc

function fetchTree (isUpdate) {
  if (!isUpdate) { $('#imgbox').empty().html("<img src='/static/spinner.gif' />&nbsp;Generating tree...") } else { $('#imgbox').append("<img src='/static/spinner.gif' />&nbsp;Updating tree...") }
  var url = $('#tree_url').text()

  $.ajax({
    type: 'POST',
    data: { evaluate: JSON.stringify(getAllEditables()) },
    url: url,
    success: function (data) {
      $('#imgbox').empty()
      if (data.tree === undefined) { treeError() }
      buildTree('#imgbox', data.tree)
    }
  }).fail(function () {
    treeError()
  })

  d3.select('#download-merger-tree').on('click', function () {
    d3.select(this)
      .attr('href', 'data:image/svg+xml;base64,' + btoa(d3.select('#imgbox').html()))
      .attr('download', 'merger_tree.svg')
  })
}

function updateDownloadButtons () {
  if ($('#image_form').values().type === 'tree') {
    $('#download-merger-tree').show()
    $('#download-csv-link').hide()
  } else {
    $('#download-csv-link').show()
    $('#download-merger-tree').hide()
  }
}

function fetchPlot (isUpdate) {
  if (plotFetchingDisabled) { return }

  updateDownloadButtons()

  if ($('#image_form').values().type === 'tree') { return fetchTree(isUpdate) }

  var formvals = getPropertiesFormJquery().values()

  var name1 = formvals.x
  var name2 = formvals.y
  var namethis = formvals.justthis
  var extension = $('#image_format').val()
  var uri
  if (namethis !== undefined) { uri = getPlotUriOneVariable(namethis, extension) } else {
    if (name1 === undefined || name2 === undefined) { return }

    uri = getPlotUriTwoVariables(name1, name2, formvals.object_typetag, extension)

    if (uri === undefined) {
      alert('Unknown plot type')
      return false
    }
  }

  if (existingImgSrc === uri) { return }

  loadImage(uri, extension)
  if (isUpdate === undefined || isUpdate === false) { $('#imgbox').empty().html("<img src='/static/spinner.gif' />&nbsp;Generating plot...") } else { $('#imgbox').append("<img src='/static/spinner.gif' />&nbsp;Updating...") }
  return true
}

function updateDownloadLink (url, extension) {
  var csv_url = url.replace(extension, 'csv')
  $('#download-csv-link').attr('href', csv_url)
}

function loadImage (url, extension) {
  existingImgSrc = url
  if (extension === 'pdf') {
    objImg = $('<object type="application/pdf" data="' + url + '" height="100%" width="100%">')
    $(objImg).ready(placeImage)
    $('#imgbox').empty()
    $('#imgbox').append(objImg)
    objImg.onerror = placeImageError
  } else {
    objImg = new Image()
    objImg.src = url
    objImg.onload = placeImage
  }
  objImg.onerror = placeImageError

  updateDownloadLink(url, extension)
}

function placeImageError () {
  var url = objImg.src
  errorlink = '<a href="' + url + '" target="_blank">'
  $('#imgbox').empty().html('<h2>Sorry, there was an error generating your plot.</h2><p>Click ' + errorlink + 'here</a> for more information (opens in a new window)')
}

function treeError () {
  $('#imgbox').empty().html('<h2>Sorry, there was an error generating the tree.</h2><p>Consult the server log for more information.</p>')
}

function placeImage () {
  $('#imgbox').empty()
  $('#imgbox').append(objImg)
  $('#imgbox').css('width', objImg.width)
}

function ensurePlotTypeIsNotTree () {
  if ($('#image_form').values().type === 'tree') {
    $('#type-cascade').prop('checked', true)
  }
}
function plotSelectionUpdate () {
  ensurePlotTypeIsNotTree()
  fetchPlot(true)
}

$('#nametg-custom-row-1').markAsRowInsertPoint()
ajaxEnableLinks()

$(function () {
  prePageUpdate(function () {
    persistAllEditables()
    persistFormStates()
  })

  postPageUpdate(function () {
    allEditables = []
    $('#nametg-custom-row-1').markAsRowInsertPoint()
    restoreAllEditables()
    restoreFormStates()
    fetchPlot(true)
    ajaxEnableLinks()
    updatePositionsAfterScroll()
  })

  fetchPlot()
})
