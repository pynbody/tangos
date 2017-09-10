var allEditables = [];

var defaultAddFn, defaultRemoveFn, defaultUpdateFn;

var addLabelText = "Add +";

$.fn.makeEditableTemplate = function(add, remove, update) {
    defaultAddFn = add;
    defaultRemoveFn = remove;
    defaultUpdateFn = update;
    allEditables.push($(this))
    $(this).html(addLabelText);
    $(this)[0].contentEditable=true;
    var savedContent;
    var column_id = $(this).attr('id').substr(7);
    $(this).on({
        'keydown': function(e) {
            if(e.which=='13') {
                // enter
                $(this).trigger('saveEditable');
                savedContent = undefined;
                $(this).blur();
                return false;
            } else if(e.which=='27') {
                // escape
                $(this).blur();
                $(this).trigger('revertEditable');
                return false;
            }
        },
        'blur': function() {
            if(savedContent!==undefined)
                $(this).trigger('revertEditable');
        },
        'focus': function() {
            savedContent = $(this).text();
            if($(this).text()===addLabelText)
                $(this).text("");
        },
        'revertEditable': function() {
            console.log('revert',savedContent);
            $(this).text(savedContent);
        },
        'saveEditable': function() {
            var content = $(this).text();
            console.log('saveEditable',savedContent,content);
            if(savedContent===addLabelText) {
                add(column_id);
            }

            if(content==="") {
                remove(column_id);
            } else {
                console.log('-> update',content);
                update(content, column_id);
            }
        }
    });

    return this;
}

function insertNewEditable(editable_name) {
    var previous_id = allEditables[allEditables.length-1].attr('id').substr(7);
    var id = defaultAddFn(previous_id);
    defaultUpdateFn(editable_name, previous_id);
}


function uriEncodeQuery(name) {
    name = name.replace(/\//g,"_slash_")
    name = encodeURIComponent(name);
    return name;
}

function persistEditables() {
    var editables = []

    forEach(allEditables, function(editable) {
        if(editable.text()!="" && editable.text()!==addLabelText)
            editables.push(editable.text());
    });
    sessionStorage['editables'] = JSON.stringify(editables);

}

function restoreEditables() {
    if(sessionStorage['editables']==undefined)
        return;

    var editables = JSON.parse(sessionStorage['editables'])

    forEach(editables, function(editable) {
        console.log(editable);
        insertNewEditable(editable);
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
    return 'Filter <input name="filter-'+uriQuery+'" type="checkbox"/>'
}

function updatePlotControlElements(element, query, isScalar, isFilter) {
    var controls = {};
    $(element).find("input").each(function() {
       controls[this.name] = this.checked;
    });
    scalarControls = getPlotControlElements(query,true);
    arrayControls = getPlotControlElements(query,false);
    filterControls = getFilterElements(query)
    var buttonsHtml;
    if(isFilter) {
        hiddenHtml = arrayControls+scalarControls;
        visibleHtml = filterControls;
    } else if(isScalar) {
        hiddenHtml = arrayControls+filterControls;
        visibleHtml = scalarControls;
    } else {
        hiddenHtml = scalarControls+filterControls;
        visibleHtml = arrayControls;
    }

    buttonsHtml = "<span class='hidden'>"+hiddenHtml+"</span>"+visibleHtml;

    $(element).html(buttonsHtml);
    $(element).find("input").each(function() {
        this.checked = controls[this.name];
    });
}

$(function() {
   restoreEditables();
});

$(window).on('beforeunload',function() {
    if(defaultAddFn!==undefined)
       persistEditables();
});