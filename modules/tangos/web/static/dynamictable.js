
$.fn.makeEditableTemplate = function(add, remove, update) {
    $(this).on('click',function(){
        if($(this).find('input').is(':focus')) return this;
        var cell = $(this);
        var content = $(this).html();
        var column_id = $(this).attr('id').substr(7);
        $(this).html('<input type="text" value="" />')
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


function uriEncodeQuery(name) {
    name = name.replace("\/","_slash_")
    name = encodeURIComponent(name);
    return name;
}

