// tree render based on http://blog.pixelingene.com/demos/d3_tree/

function visit(parent, visitFn, childrenFn)
{
    if (!parent) return;

    visitFn(parent);

    const children = childrenFn(parent);
    if (children) {
        const count = children.length;
        for (let i = 0; i < count; i++) {
            visit(children[i], visitFn, childrenFn);
        }
    }
}

function ensureSelectedNodeIsVisible(containerName) {
    let selectedNode = $(containerName+' .node-dot-selected')[0]
    let container = $(containerName)[0]
    let selectedNodeRect = selectedNode.getBoundingClientRect();
    let containerRect = container.getBoundingClientRect();

    if(selectedNodeRect.bottom>containerRect.bottom || selectedNodeRect.top<containerRect.top) {
        // Move node to vertical centre of view
        let moveTop = (containerRect.top + containerRect.bottom) / 2 - selectedNodeRect.top
        container.scrollTop -= moveTop;
    }

    if(selectedNodeRect.left<containerRect.left || selectedNodeRect.right>containerRect.right) {
        // Move node to left hand edge of view
        let moveLeft = (containerRect.left)-selectedNodeRect.left;
        container.scrollLeft-=moveLeft;
    }
}

function buildTree(containerName, treeData, customOptions)
{
    // build the options object
    let options = $.extend({
        nodeRadius: 5, fontSize: 12
    }, customOptions);


    // Calculate total nodes, max label length
    let totalNodes = 0;
    let maxLabelLength = 0;
    let maxX = 0;
    let minX = 100000;
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

    // size of the diagram. 50 px per timestep here.
    let size = { width: treeData['maxdepth']*50+50, height: maxX-minX+100};

    let tree = d3.layout.tree()
        .sort(null)
        .size([size.height, size.width - maxLabelLength*options.fontSize])
        .children(function(d)
        {
            return (!d.contents || d.contents.length === 0) ? null : d.contents;
        });

    let nodes = tree.nodes(treeData);
    nodes.forEach(function(d) { d.x = d._x-minX+25; d.y+=25; });

    let links = tree.links(nodes);


    /*
        <svg>
            <g class="container" />
        </svg>
     */
    let layoutRoot = d3.select(containerName)
        .append("svg:svg").attr("width", size.width+500).attr("height", size.height)
        .append("svg:g")
        .attr("class", "container")
        .attr("transform", "translate(" + maxLabelLength + ",0)");

    let graphRoot = layoutRoot.append('g');
    let floatRoot = layoutRoot.append('g');

    // Edges between nodes as a <path class="link" />
    let link = d3.svg.diagonal()
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
    let nodeGroup = graphRoot.selectAll("g.node")
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
            let g = d3.select(this); // The node

            let bbox = g.node().getBoundingClientRect()
            let bbox_top = floatRoot.node().getBoundingClientRect()

            let top = bbox.top - bbox_top.top + 50;
            let left = bbox.left - bbox_top.left + 20;
            let height = 70;


            let box = floatRoot.append('rect');
            // The class is used to remove the additional text later
            let info = floatRoot.append('text')
            .classed('info', true)
            .attr('x', left)
            .attr('y', top)
            .text(d.moreinfo);

            let info2 = floatRoot.append('text')
            .classed('info', true)
            .attr('x', left)
            .attr('y', top+15)
            .text(d.timeinfo);

            let width = Math.max(info.node().getComputedTextLength()+20,
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
