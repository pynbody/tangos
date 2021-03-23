
// solution to easily serialize/*de*serialize form data from
// http://stackoverflow.com/questions/1489486/jquery-plugin-to-serialize-a-form-and-also-restore-populate-the-form
$.fn.values = function(data) {
    var els = $(this).find(':input').get();

    if(typeof data != 'object') {
        // return all data
        data = {};

        $.each(els, function() {
            if (this.name && !this.disabled && (this.checked
                            || /select|textarea/i.test(this.nodeName)
                            || /text|hidden|password/i.test(this.type))) {
                data[this.name] = $(this).val();
            }
        });
        return data;
    } else {
        $.each(els, function() {
            if (this.name && data[this.name]) {
                if(this.type == 'checkbox' || this.type == 'radio') {
                    $(this).prop("checked", (data[this.name] == $(this).val()));
                } else {
                    $(this).val(data[this.name]);
                }
            }
        });
        return $(this);
    }
};



function persistFormStates() {
    var formState;
    if(sessionStorage['formstates']!==undefined)
        formState = JSON.parse(sessionStorage['formstates']);
    else
        formState = {};

    $('form.autorestore').each(function() {
        formState[this.id] = $(this).values();
    });

    sessionStorage['formstates'] = JSON.stringify(formState);


}

function restoreFormStates() {
    if(sessionStorage['formstates']===undefined)
        return;

    var formState = JSON.parse(sessionStorage['formstates']);

    $('form.autorestore').each(function () {
        $(this).values(formState[this.id]);
    });

    $('form.autorestore').trigger('change');
}


$(window).on('beforeunload',function() {
    persistFormStates();
});

$(function() {
   restoreFormStates();
});