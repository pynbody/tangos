var allEditables = [];

var addLabelText = "Add +";

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

jQuery.fn.positionDiv = function (divToPop) {
    var pos=$(this).offset();
    var h=$(this).height();
    var w=$(this).width();
    $(divToPop).css({ left: pos.left , top: pos.top + h + 5 , position: 'absolute', align: 'right' });

};

function popupControls(attachTo) {
    unpopupControls(attachTo);
    var id = attachTo.attr('id');
    var popupDiv = $("<div/>", {id: 'popup-controls-'+id, class: 'popup-controls'});
    popupDiv.html("&#x1F5D1;");
    popupDiv.appendTo('#content-container');
    attachTo.positionDiv(popupDiv);
    popupDiv.on('mousedown', function() {
        attachTo.trigger('deleteEditable');
    }).on( 'click', function() {});
}

function unpopupControls(attachedTo) {
    var id = attachedTo.attr('id');
    $('#popup-controls-'+id).remove();
}

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
    var column_id = $this.attr('id').substr(7);

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
            if(savedContent!==addLabelText)
                popupControls($this);
            if($this.text()===addLabelText) {
                putCursorAt(this, 0);
                $this.text("");
            }
        },
        'deleteEditable': function() {
            unpopupControls($this);
            remove(column_id, editable_tag);
            allEditables.removeItem($this);
        },
        'revertEditable': function() {
            unpopupControls($this);
            $this.css('cursor','pointer');
            $this.text(savedContent);
        },
        'saveEditable': function() {
            unpopupControls($this);
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

/*
function insertNewEditable(editable_name) {
    var previous_id = allEditables[allEditables.length-1].attr('id').substr(7);
    var id = defaultAddFn(previous_id);
    defaultUpdateFn(editable_name, previous_id);
}
*/


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
            var old_column_id = editable.attr('id').substr(7);
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
        return '<input name="x" type="radio" onclick="resetRadio(\'justthis\');"  value="'+uriQuery+'"/>' +
               '<input name="y" type="radio" onclick="resetRadio(\'justthis\');" value="'+uriQuery+'"/>'
    else
        return '<input name="justthis" type="radio" value="'+uriQuery+'" onclick="resetRadio(\'x\'); resetRadio(\'y\');"/>'
}

function getFilterElements(query) {
    var uriQuery = uriEncodeQuery(query);
    return '<label>Filter <input name="filter-'+uriQuery+'" type="checkbox"/></label>'
}

function updatePlotControlElements(element, query, isScalar, isFilter, isArray, filterOnly) {
    var controls = {};
    $(element).find("input").each(function() {
       controls[this.name] = this.checked;
    });
    scalarControls = getPlotControlElements(query,true);
    arrayControls = getPlotControlElements(query,false);
    filterControls = getFilterElements(query)
    if(filterOnly) {
        scalarControls = "";
        arrayControls = "";
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

    buttonsHtml = "<span class='hidden'>"+hiddenHtml+"</span>"+visibleHtml;

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