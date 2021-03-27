

var scrollTop = {};

function initScrollOffsetData() {
    $(".keeponscreen").each(function() {
        if($(this).css('position')!='absolute') {
            // generate clone that keeps the space for this element
            var clone = $(this).clone();
            clone.removeClass("keeponscreen");
            clone.attr('id', clone.attr('id') + "-placeholder");
            clone.css('visibility', 'hidden');
            $(this).after(clone);
        }
        scrollTop[this.id]=this.getBoundingClientRect().top+window.scrollY;

    });
}

function updatePositionsAfterScroll() {
    var windowTop = $(window).scrollTop();
    var current=0;
    $(".keeponscreen").each(function() {
        if(windowTop<scrollTop[this.id]-current) {
            $(this).css({position:"absolute",
                         top: scrollTop[this.id]});
        } else {
            $(this).css({position:"fixed",
                         top: current});
        }

        clone = $("#"+this.id+"-placeholder");
        if (clone!=null)
            clone.css({height: this.getBoundingClientRect().height+10});

        current = this.getBoundingClientRect().bottom
    });

}

function setupScrollAdjustment() {
    initScrollOffsetData();
    $(window).scroll(updatePositionsAfterScroll);
    updatePositionsAfterScroll();

}

var hasInitialized;

$(function() {
    if(hasInitialized) return;
    hasInitialized=true;

    setupScrollAdjustment();

 });