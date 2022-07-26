var allEditables = [];

var addLabelText = "Add +";


function extractColumnName(s) {
    const extractColumnNameRegexp = /(label|plotctl)-(.*)/;
    return s.match(extractColumnNameRegexp)[2];
}

Array.prototype.removeItem = function() {
    // from https://stackoverflow.com/questions/3954438/how-to-remove-item-from-array-by-value
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);
        }
    }
    return this;
};

$.fn.makeEditableTemplate = function(add, remove, update, editable_tag) {
    /* Mark a DOM element as a place to

    When a new editable element should be added, the add function is called.
    When an existing element should be updated, the update function is called.
    If the user asks for it to be removed, the remove function is called.

    Editable elements are automatically restored when a page is reloaded; this is
    accomplished by firing the add/update methods on the matching editable_tag.
     */


    var $this = $(this);

    allEditables.push($this);


    $this.data('editable_type_tag', editable_tag);
    $this.data('editable_add', add);
    $this.data('editable_remove', remove);
    $this.data('editable_update', update);

    $this.html(addLabelText);
    $this[0].contentEditable=true;
    enableAutocomplete($this);

    var savedContent;
    var column_id = extractColumnName($this.attr('id'));

    $this.css('cursor','pointer');

    $this.on({
        'keydown': function(e) {
            if(e.which=='13') {
                // enter
                $this.trigger('saveEditable');
                savedContent = undefined;
                $this.blur();
                return false;
            } else if(e.which=='27') {
                // escape
                $this.blur();
                $this.trigger('revertEditable');
                return false;
            }
        },
        'blur': function() {
            if(savedContent!==undefined) {
                $this.trigger('revertEditable');
            }
        },
        'focus': function() {
            $this.css('cursor','text');
            savedContent = $this.text();
            if($this.text()===addLabelText) {
                putCursorAt(this, 0);
                $this.text("");
            }
            console.log("savedContent=",savedContent);
        },
        'deleteEditable': function() {
            remove(column_id, editable_tag);
            allEditables.removeItem($this);
        },
        'revertEditable': function() {
            $this.css('cursor','pointer');
            $this.text(savedContent);
        },
        'saveEditable': function() {
            $this.css('cursor','pointer');
            var content = $this.text();
            if(savedContent===addLabelText) {
                add(column_id, editable_tag);
            }

            if(content==="") {
                $this.trigger('deleteEditable');
            } else {
                update(content, column_id, editable_tag);
            }
        }
    });

    return this;
}

function uriEncodeQuery(name) {
    name = name.replace(/\//g,"_slash_")
    name = encodeURIComponent(name);
    return name;
}

function uriDecodeQuery(name) {
    name = name.replace(/_slash_/g,"/")
    name = decodeURIComponent(name);
    return name;
}

function getAllEditables() {
    var editables = [];
    forEach(allEditables, function(row) {
        var text = row.text();
        if(text!==addLabelText)
           editables.push(text);
    });
    return editables;
}

function persistAllEditables() {
    if(allEditables.length==0)
        return; // don't waste time – there's nothing to be stored from this page

    if(sessionStorage['editables']!==undefined)
        var oldEditables = JSON.parse(sessionStorage['editables']);
    else
        var oldEditables = {};

    var editables = {};

    forEach(allEditables, function(editable) {
        var type_tag = editable.data('editable_type_tag');
        if(!(type_tag in editables)) {
            editables[type_tag] = [];
        }
        var editables_of_type = editables[type_tag];
        if(editable.text()!="" && editable.text()!==addLabelText) {
            editables_of_type.push(editable.text());
        }
    });

    // for any editable categories that do not appear on this page, but that have
    // stored information, carry that stored information over.

    for(var key in oldEditables) {
        if(!(key in editables)) {
            editables[key] = oldEditables[key];
        }
    }

    sessionStorage['editables'] = JSON.stringify(editables);

}

function restoreAllEditables() {
    if(sessionStorage['editables']==undefined)
        return;

    var editables = JSON.parse(sessionStorage['editables'])

    forEach(allEditables, function(editable) {
        var type_tag = editable.data('editable_type_tag');
        if(type_tag in editables) {
            var editables_of_type = editables[type_tag];
            var old_column_id = extractColumnName(editable.attr('id'));
            forEach(editables_of_type, function(name_to_add) {
                var add_fn = editable.data('editable_add');
                var update_fn = editable.data('editable_update');

                update_fn(name_to_add, old_column_id, type_tag);
                old_column_id = add_fn(old_column_id, type_tag);

            });

        }

    });

}

function getPlotControlElements(query, isScalar) {
    var uriQuery = uriEncodeQuery(query);
    if(isScalar)
        return '<label class="x-plot-radio"><input name="x" type="radio" onclick="resetRadio(\'justthis\');"  value="'+uriQuery+'"/></label>' +
               '<label class="y-plot-radio"><input name="y" type="radio" onclick="resetRadio(\'justthis\');" value="'+uriQuery+'"/></label>'
    else
        return '<label class="plot-radio"><input name="justthis" type="radio" value="'+uriQuery+'" onclick="resetRadio(\'x\'); resetRadio(\'y\');"/></label>'
}

function getFilterElements(query) {
    var uriQuery = uriEncodeQuery(query);
    return '<input id="filter-'+uriQuery+'" name="filter-'+uriQuery+'" type="checkbox" class="filter-checkbox"/><label class="filter-checkbox" for="filter-'+uriQuery+'" title="Filter"></label>'
}

function sortTableColumn(element, ascending) {
    let object_tag = $(element).parents('table').attr('id').substr(6);
    let mini_language_query = $("#label-"+extractColumnName(element)).data('miniLanguageQuery');
    reorderByColumn(object_tag, mini_language_query, ascending);
}
function getSorterElements(element) {
    return '<a href="#" onclick="sortTableColumn(\'' + element + '\', true); return false;" class="sort asc" title="Sort ascending"></a>' +
        '<a href="#" onclick="sortTableColumn(\'' + element + '\', false); return false;" class="sort desc" title="Sort descending"></a>'
}

function getDeleteElements(element) {
    let headerElement = $("#label-"+extractColumnName(element));
    if (headerElement.hasClass('editable'))
        return '<a href="#" onclick="$(\''+"#label-"+extractColumnName(element)+'\').trigger(\'deleteEditable\'); return false;" class="delete" title="Remove"></a>';
    else
        return '';
}

function updatePlotControlElements(element, query, isScalar, isFilter, isArray, canDelete, isColumnHeading) {
    var controls = {};
    $(element).find("input").each(function() {
       controls[this.name] = this.checked;
    });
    scalarControls = getPlotControlElements(query,true);
    arrayControls = getPlotControlElements(query,false);
    filterControls = getFilterElements(query)
    if(isColumnHeading) {
        arrayControls = "";
        scalarControls = getSorterElements(element);
    }

    var buttonsHtml;
    if(isFilter) {
        hiddenHtml = arrayControls+scalarControls;
        visibleHtml = filterControls;
    } else if(isScalar) {
        hiddenHtml = arrayControls+filterControls;
        visibleHtml = scalarControls;
    } else if(isArray) {
        hiddenHtml = scalarControls+filterControls;
        visibleHtml = arrayControls;
    } else {
        hiddenHtml = scalarControls+filterControls+arrayControls;
        visibleHtml = "";
    }
    visibleHtml = "<div class='leftbuttons'>" + visibleHtml +
        "</div><div class='rightbuttons'>" + getDeleteElements(element) + "</div>";

    buttonsHtml = "<div class='hidden'>"+hiddenHtml+"</div>"+visibleHtml;

    $(element).html(buttonsHtml);
    $(element).find("input").each(function() {
        this.checked = controls[this.name];
    });
}

$(function() {
   restoreAllEditables();
});

$(window).on('beforeunload',function() {
    persistAllEditables();
});
