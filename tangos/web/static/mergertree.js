// tree render based on http://blog.pixelingene.com/demos/d3_tree/

function visit(parent, visitFn, childrenFn)
{
    if (!parent) return;

    visitFn(parent);

    var children = childrenFn(parent);
    if (children) {
        var count = children.length;
        for (var i = 0; i < count; i++) {
            visit(children[i], visitFn, childrenFn);
        }
    }
}

function ensureSelectedNodeIsVisible(containerName) {
    var selectedNode = $(containerName+' .node-dot-selected')[0]
    var container = $(containerName)[0]

    var selectedNodeRect = selectedNode.getBoundingClientRect();
    var containerRect = container.getBoundingClientRect();

    if(selectedNodeRect.bottom>containerRect.bottom || selectedNodeRect.top<containerRect.top) {
        // Move node to vertical centre of view
        var moveTop = (containerRect.top + containerRect.bottom) / 2 - selectedNodeRect.top
        container.scrollTop -= moveTop;
    }

    if(selectedNodeRect.left<containerRect.left || selectedNodeRect.right>containerRect.right) {
        // Move node to left hand edge of view
        var moveLeft = (containerRect.left)-selectedNodeRect.left;
        container.scrollLeft-=moveLeft;
    }
}

function buildTree(containerName, treeData, customOptions)
{
    // build the options object
    var options = $.extend({
        nodeRadius: 5, fontSize: 12
    }, customOptions);


    // Calculate total nodes, max label length
    var totalNodes = 0;
    var maxLabelLength = 0;
    var maxX = 0;
    var minX = 100000;
    visit(treeData, function(d)
    {
        totalNodes++;
        maxLabelLength = Math.max(d.name.length, maxLabelLength);
        maxX = Math.max(d._x, maxX);
        minX = Math.min(d._x, minX);
    }, function(d)
    {
        return d.contents && d.contents.length > 0 ? d.contents : null;
    });

    console.log(minX,maxX);

    // size of the diagram. 50 px per timestep here.
    var size = { width: treeData['maxdepth']*50+50, height: maxX-minX+100};

    var tree = d3.layout.tree()
        .sort(null)
        .size([size.height, size.width - maxLabelLength*options.fontSize])
        .children(function(d)
        {
            return (!d.contents || d.contents.length === 0) ? null : d.contents;
        });

    var nodes = tree.nodes(treeData);
    nodes.forEach(function(d) { d.x = d._x-minX+25; d.y+=25; });

    var links = tree.links(nodes);


    /*
        <svg>
            <g class="container" />
        </svg>
     */
    var layoutRoot = d3.select(containerName)
        .append("svg:svg").attr("width", size.width+500).attr("height", size.height)
        .append("svg:g")
        .attr("class", "container")
        .attr("transform", "translate(" + maxLabelLength + ",0)");

    var graphRoot = layoutRoot.append('g');
    var floatRoot = layoutRoot.append('g');

    // Edges between nodes as a <path class="link" />
    var link = d3.svg.diagonal()
        .projection(function(d)
        {
            return [d.y, d.x];
        });

    graphRoot.selectAll("path.link")
        .data(links)
        .enter()
        .append("svg:path")
        .attr("class", "link")
        .attr("d", link)
        .attr("fill", "none")
        .style("stroke", "#a8aaa6")
        .style("stroke-width", function(d) {
                return Math.min(d.source.size, d.target.size)*1.3;
            })
        .style("stroke-dasharray", function(d) {
            console.log(d);
            if(d.source.nodeclass.includes("phantom") ||
               d.target.nodeclass.includes("phantom"))
                return "5, 5";
            else
                return "";
        });


    /*
        Nodes as
        <g class="node">
            <circle class="node-dot" />
            <text />
        </g>
     */
    var nodeGroup = graphRoot.selectAll("g.node")
        .data(nodes)
        .enter()
        .append("svg:g")
        .attr("class", "node")
        .style("fill", "#aa161f")
        .attr("transform", function(d)
        {
            return "translate(" + d.y + "," + d.x + ")";
          })
          .on("mouseover", function(d) {
            var g = d3.select(this); // The node

            var bbox = g.node().getBoundingClientRect()
            var bbox_top = floatRoot.node().getBoundingClientRect()

            var top = bbox.top - bbox_top.top + 50;
            var left = bbox.left - bbox_top.left + 20;
            var height = 70;

            // console.log(bbox.left,bbox.top)


            var box = floatRoot.append('rect');
            // The class is used to remove the additional text later
            var info = floatRoot.append('text')
            .classed('info', true)
            .attr('x', left)
            .attr('y', top)
            .text(d.moreinfo);

            var info2 = floatRoot.append('text')
            .classed('info', true)
            .attr('x', left)
            .attr('y', top+15)
            .text(d.timeinfo);

            var width = Math.max(info.node().getComputedTextLength()+20,
                                 info2.node().getComputedTextLength()+20);

            box
            .classed('info',true)
            .attr('x', left -10 )
            .attr('y', top - 30 )
            .attr('width',width)
            .attr('height',height);




          })
          .on("mouseout", function() {
            // Remove the info text on mouse out.
            floatRoot.select('text.info').remove();
            floatRoot.select('text.info').remove();
            floatRoot.select('rect.info').remove();
          });

    function ajaxNavigateFromSvg(d) {
      ajaxNavigate(d.url);
      d3.event.preventDefault();
    };

    nodeGroup.append("svg:a")
        .on('click', ajaxNavigateFromSvg)
        .attr("xlink:href", function(d)
        {
          return d.url;
        })
        .append("svg:circle")
        .attr("class", function(d)
        {
          return d.nodeclass;
        })
        .attr("r", function(d) {
                return d.size;
            });



    nodeGroup.append("svg:a")
        .on('click', ajaxNavigateFromSvg)
        .attr('class', 'ajaxenabled')
        .attr("xlink:href", function(d)
        {
          return d.url;
        })
        .append("svg:text")
        .attr("text-anchor", function(d)
        {
            // return d.children ? "end" : "start";
            return "middle";
        })
        .attr("dy", function(d)
        {
            return d.size+10;
            // return d.children ? -gap : gap;
            return gap;
        })
        .attr("dx", 0 )
        .attr("fill","#aaa")
        .text(function(d)
        {
            if(!d.nodeclass.includes("phantom"))
                return d.name;
        });


    ensureSelectedNodeIsVisible(containerName);

}
