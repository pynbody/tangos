
let availableTags = [

];

$(function() {
    $.ajax({
        type: "GET",
        url: "/autocomplete_words.json",
        success: function (data) {
            availableTags = data;
            availableTags.sort(function (a, b) {
                return a.toLowerCase().localeCompare(b.toLowerCase());
            });
        }
    });
});


const start_word_regex = /[a-zA-Z]+[a-zA-Z0-9_]*$\s*/;
const end_word_regex = /^[a-zA-Z0-9_]*\s*/;

function extractBeforeWord(val, position) {
    const start = val.substring(0,position);
    const start_word = start_word_regex.exec(start)[0];
    if(start_word.length>0)
        return start.substring(0,start.length-start_word.length);
    else
        return start;
}

function putCursorAt(element, position) {
    let range = document.createRange();
    range.setStart(element.childNodes[0], position);
    range.setEnd(element.childNodes[0], position);
    let sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
    element.focus();
}

function extractAfterWord(val, position) {
    let end = val.substring(position);
    let end_word = end_word_regex.exec(end)[0];
    return end.substring(end_word.length);
}

function extractWord( val, position ) {
  let start = val.substring(0,position);
  let end = val.substring(position);
  let start_word = start_word_regex.exec(start);
  let end_word = end_word_regex.exec(end);
  return start_word+end_word;
}


function getCaretPosition(editableDiv) {
  let caretPos = 0,
    sel, range;
  if (window.getSelection) {
    sel = window.getSelection();
    if (sel.rangeCount) {
      range = sel.getRangeAt(0);
      if (range.commonAncestorContainer.parentNode == editableDiv) {
        caretPos = range.endOffset;
      }
    }
  } else if (document.selection && document.selection.createRange) {
    range = document.selection.createRange();
    if (range.parentElement() == editableDiv) {
      let tempEl = document.createElement("span");
      editableDiv.insertBefore(tempEl, editableDiv.firstChild);
      let tempRange = range.duplicate();
      tempRange.moveToElementText(tempEl);
      tempRange.setEndPoint("EndToEnd", range);
      caretPos = tempRange.text.length;
    }
  }
  return caretPos;
}


function enableAutocomplete(element) {
    let currentCaretPosition;

    element
    // don't navigate away from the field on tab when selecting an item
        .on("keydown keyup mousedown mouseup", function(event) {
            currentCaretPosition = getCaretPosition(element[0]);
        })
        .on("keydown", function (event) {
            if (event.keyCode === $.ui.keyCode.TAB &&
                $(this).autocomplete("instance").menu.active) {
                event.preventDefault();
            }
        })
        .autocomplete({
            autoFocus: true,
            minLength: 1,
            delay: 0.0,
            source: function (request, response) {

                let word = extractWord(request.term, currentCaretPosition);

                if(word.length===0) {
                    element.autocomplete("close");
                    return;
                }

                // check if there's a match which starts with the term the user's typing
                let results = $.map(availableTags, function (tag) {
                    if (tag.toUpperCase().indexOf(word.toUpperCase()) === 0) {
                        return tag;
                    }
                });

                // if not, look for any match
                if (results.length===0)
                    results = $.ui.autocomplete.filter(availableTags, word);

                response(results);

            },
            focus: function () {
                // prevent value inserted on focus
                return false;
            },
            select: function (event, ui) {
                let before = extractBeforeWord(this.textContent, currentCaretPosition);
                let after = extractAfterWord(this.textContent, currentCaretPosition);
                this.textContent = before+ui.item.value+after;
                let endPosition = before.length+ui.item.value.length;
                putCursorAt(this, endPosition);
                return false;
            }
        });
}
