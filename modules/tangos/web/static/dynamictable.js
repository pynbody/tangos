var allEditables = [];

var defaultAddFn, defaultRemoveFn, defaultUpdateFn;

$.fn.makeEditableTemplate = function(add, remove, update) {
    defaultAddFn = add;
    defaultRemoveFn = remove;
    defaultUpdateFn = update;
    allEditables.push($(this))
    $(this).on('click',function(){
        if($(this).find('input').length>0) return this;
        var cell = $(this);
        var content = $(this).text();
        var prefilled = content;
        if(content==="Add +")
            prefilled="";

        var column_id = $(this).attr('id').substr(7);
        $(this).html('<a>(del)</a><input type="text" class="editbox" value="'+prefilled+'" />')

        $(this).find('input')
            .trigger('focus')
            .on({

                'keyup':function(e){
                    if(e.which == '13'){ // enter
                        $(this).trigger('saveEditable');
                    } else if(e.which == '27'){ // escape
                        $(this).trigger('closeEditable');
                    }
                },
                'closeEditable':function(){
                    cell.html(content);

                },
                'saveEditable':function(){
                    if(content==="Add +") {
                        add(column_id);
                    }
                    content = $(this).val();
                    $(this).trigger('closeEditable');
                    if(content==="") {
                        remove(column_id);
                    } else {
                        update(content, column_id);
                    }
                }
            });
        $(this).find("a").on({
            "click": function () {
                cell.find('input').val("");
                cell.find('input').trigger('saveEditable');
            }
        });

        $(this).on('focusout', function(){
            var input = $(this).find('input');
            setTimeout(function() {
                input.trigger('closeEditable');
            }, 100);
        });
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
        if(editable.text()!="" && editable.text()!=="Add +")
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

function getPlotControlElements(query) {
    var uriQuery = uriEncodeQuery(query);
    return '<input name="x" type="radio" value="'+uriQuery+'"/><input name="y" type="radio" value="'+uriQuery+'"/>'
}

$(function() {
   restoreEditables();
});

$(window).on('beforeunload',function() {
   persistEditables();
});