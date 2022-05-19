

function setupTimestepTables(timestep_url) {
    if(timestep_url!==window.timestep_url) {
        window.dataTables = {}
        window.timestep_url = timestep_url
    }
}

function getGatherUrl(typetag, miniLanguageQuery) {
    return timestep_url+"/gather/"+typetag+"/"+uriEncodeQuery(miniLanguageQuery)+".json";
}

function requestColumnData(editable_tag, miniLanguageQuery, callback) {
    if(window.dataTables === undefined ) {
        console.log("Attempt to request column data but the data tables have not yet been initialised")
        return undefined; // can't do anything useful
    }

    if(window.dataTables[editable_tag] === undefined) {
        window.dataTables[editable_tag] = {}
    }


    if(window.dataTables[editable_tag][miniLanguageQuery] === undefined) {
        var updateMarker = $("#update-marker-" + editable_tag);
        if(updateMarker!==undefined)
            updateMarker.html("<div class='progress-spinner'></div>");
        let reqs = updateMarker.data("pending-requests")
        if (reqs === undefined) reqs = 0;
        updateMarker.data("pending-requests", reqs + 1)
        console.log("Requesting "+miniLanguageQuery+" for "+editable_tag+"...");
        $.ajax({
            type: "GET",
            url: getGatherUrl(editable_tag, miniLanguageQuery),
            success: function (data) {
                if(updateMarker!==undefined) {
                    let reqs = updateMarker.data("pending-requests")
                    if (reqs === 1)
                        $("#update-marker-" + editable_tag).html('');
                    updateMarker.data("pending-requests", reqs - 1)
                }
                window.dataTables[editable_tag][miniLanguageQuery] = data;
                if(callback!==undefined)
                   callback(window.dataTables[editable_tag][miniLanguageQuery]);
                autoReorderIfNeeded(editable_tag, miniLanguageQuery);
            }
        });
    } else {
        if(callback!==undefined)
            callback(window.dataTables[editable_tag][miniLanguageQuery]);
    }

}

function getFilterArray(object_tag, get_id_from='th', callbackAfterFetch = undefined) {
    let columnsToFilterOn = [];
    let dataToFilterOn = [];

    const re = /filter-(.*)/;
    $('#properties_form_'+object_tag+' input[type="checkbox"]').each(function() {
        let $this = $(this);
        if($this.prop('checked') && $this.is(":visible")) {
            let name = $this.attr('name').match(re)[1];
            columnsToFilterOn.push(uriDecodeQuery(name));
        }
    });


    for(i=0; i<columnsToFilterOn.length; i++) {
        col = columnsToFilterOn[i];
        if(window.dataTables[object_tag][col]!==undefined) {
           dataToFilterOn.push(window.dataTables[object_tag][col].data_formatted);
        } else {
           if (callbackAfterFetch!==undefined) {
               requestColumnData(object_tag, col, callbackAfterFetch);
               return undefined;
           } // if no callback supplied, we carry on and get the best answer we can
        }
    }

    let nData = 0;

    try {
        nData = window.dataTables[object_tag]['halo_number()'].data_formatted.length;
    } catch(TypeError) {
        if (callbackAfterFetch!==undefined) {

           requestColumnData(object_tag, 'halo_number()', callbackAfterFetch);
       } // if no callback supplied, we carry on and get the best answer we can
        return undefined;
    }

    let filterArray = new Array(nData).fill(true);

    $.each(dataToFilterOn, function (j, c) {
        for(var i=0; i<nData; i++) {
            if (c[i] !== 'True') filterArray[i] = false;
        }
    });

    return filterArray;
}

function autoReorderIfNeeded(object_tag, miniLanguageQ) {
    let last = "";
    let asc = false;
    if(sessionStorage['last_sort_'+object_tag]!==undefined) {
        let res = JSON.parse(sessionStorage['last_sort_'+object_tag]);
        last = res['miniLanguageQ'];
        asc = res['ascending'];
        if(last==miniLanguageQ) {
            console.log("Restoring order to table "+object_tag+", sorting by "+miniLanguageQ)
            reorderByColumn(object_tag, miniLanguageQ, asc);
        }
    }

}

function reorderByColumn(object_tag, miniLanguageQ, ascending = true) {

    let order = window.dataTables[object_tag]['*ordering'];
    let data = window.dataTables[object_tag][miniLanguageQ].data_formatted;
    let sign = ascending ? -1 : 1;

    if(order === undefined) {
        console.log("Unable to find order information when attempting to reoder "+object_tag+" by "+miniLanguageQ);
        return;
    }

    order.sort(function(a,b){
        var tda = parseFloat(data[a]);
        var tdb = parseFloat(data[b]);
                // if a < b return 1
        return tda < tdb ? sign
               // else if a > b return -1
               : tda > tdb ? -sign
               // else they are equal - return 0
               : 0;
    });

    sessionStorage['last_sort_'+object_tag] = JSON.stringify({'miniLanguageQ': miniLanguageQ, 'ascending': ascending})
    updateTableDisplay(object_tag);
}


function updateTableDisplay(object_tag) {
    let dataColumns = [];
    $("tr#header-row-"+object_tag+" th").each(function() {
       var miniLanguageQ = $(this).data('miniLanguageQuery');
       if(window.dataTables[object_tag][miniLanguageQ]!==undefined) {
           dataColumns.push(window.dataTables[object_tag][miniLanguageQ].data_formatted);
       } else {
           dataColumns.push(undefined);
       }
    });

    let nData = 0;
    $.each(dataColumns, function(i,c) {
        if(c !== undefined && c.length>nData) {
            nData=c.length;
        }
    });

    let filterArray = getFilterArray(object_tag);
    let order = window.dataTables[object_tag]['*ordering'];
    if(order === undefined)
        return; // not ready yet!


    $("#table-"+object_tag+" tr.tangos-data").remove();



    let rowsPerPage = parseInt($("#per-page-"+object_tag+" option:selected").text());
    let page = parseInt($("#page-"+object_tag+" option:selected").text());
    if (isNaN(page)) page=1;
    let startRow = (page-1)*rowsPerPage;
    let endRow = startRow+rowsPerPage;

    let nRowsTotal=0;
    let displayRows = [];

    for(var i_unsorted=0; i_unsorted<nData; i_unsorted++) {
        i = order[i_unsorted];

        let shouldDisplay = true;

        if(filterArray!==undefined)
            shouldDisplay = filterArray[i];

        if(shouldDisplay) {
            if (nRowsTotal<endRow && nRowsTotal>=startRow) {
                display = "<tr class='tangos-data'>"
                $.each(dataColumns, function(j,c) {
                    if(c!==undefined)
                        display+="<td>"+c[i]+"</td>";
                    else
                        display+="<td></td>";
                });
                display +="</tr>";
                displayRows.push(display);
            }

            nRowsTotal++;
        }
    }


    var numPages = Math.ceil(nRowsTotal/rowsPerPage);
    $("#num-pages-"+object_tag).text(numPages);
    $("#num-objects-"+object_tag).text(nRowsTotal);

    var pageSelector = $("#page-"+object_tag)
    pageSelector.find("option").remove();
    for(var i=1; i<numPages+1; i++) {
        selected = (i==page)?" selected":"";
        pageSelector.append("<option name='"+i+"'"+selected+">"+i+"</option>")
    }

    $("#table-"+object_tag).append(displayRows);


}