var allEditables = []

$.fn.makeEditableTemplate = function(add, remove, update) {
    allEditables.push($(this))
    $(this).on('click',function(){
        if($(this).find('input').is(':focus')) return this;
        var cell = $(this);
        var content = $(this).text();
        var prefilled = content;
        if(content==="Add +")
            prefilled="";

        var column_id = $(this).attr('id').substr(7);
        $(this).html('<input type="text" value="'+prefilled+'" />')
            .find('input')
            .trigger('focus')
            .on({
                'blur': function(){
                    $(this).trigger('closeEditable');
                },
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
                        cell.append("&nbsp;<img src='/static/spinner.gif'/>");
                        $('.' + column_id).text("");
                        update(content, column_id);
                    }
                }
            });
    });
return this;
}

function insertNewEditable(editable_name) {
    allEditables[0].text(editable_name);
    allEditables[0].trigger('click');
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