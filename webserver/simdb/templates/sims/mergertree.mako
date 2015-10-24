<%!
import simplejson as json
%>

<%inherit file="/base.mako"/> \

<%def name="header()">Merger Tree</%def>

<script type="text/javascript" src="/scripts/d3.min.js"></script>

<script type="text/javascript">


var treeData = ${ json.dumps( c.tree ) | n };

// tree render code stolen, er, borrowed, from http://blog.pixelingene.com/demos/d3_tree/

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

function buildTree(containerName, customOptions)
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
        .attr("d", link);


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

            console.log(bbox.left,bbox.top)


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

    nodeGroup.append("svg:a")
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
        .attr("xlink:href", function(d)
        {
          return d.url;
        })
        .append("svg:text")
        .attr("text-anchor", function(d)
        {
            return d.children ? "end" : "start";
        })
        .attr("dx", function(d)
        {
            var gap = 2 * options.nodeRadius;
            return d.children ? -gap : gap;
        })
        .attr("dy", 3)
        .text(function(d)
        {
            return d.name;
        });

}


</script>


<style>
    g.node {
        font-family: Verdana, Helvetica;
        font-size: 12px;
        font-weight: bold;
    }

    circle.node-dot-standard {
        fill: red;
        stroke: red;
        stroke-width: 1px;
    }

    circle.node-dot-sub {
        fill: lightsalmon;
        stroke: red;
        stroke-width: 1px;
    }

    path.link {
        fill: none;
        stroke: gray;
    }

    text.info {
      font-family: Verdana, Helvetica;
      font-size: 12px;
      fill: gray;
    }

    rect.info {
      fill: white;
      stroke: gray;
    }
</style>

<div id="tree-container"></div>



<script>
    $(function () {
        buildTree("#tree-container");
    });
</script>
