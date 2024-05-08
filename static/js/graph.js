function fetchGraphData() {
    const type = document.getElementById('graph-type').value;
    const url = `/${type}`;
    const loadingElement = document.getElementById('loading');
    loadingElement.style.display = 'block';

    fetch(url)
        .then(response => {
            if (!response.ok) throw new Error(`Failed to fetch graph data: ${response.statusText}`);
            return response.json();
        })
        .then(data => {
            console.log("Data received from the server:", JSON.stringify(data, null, 2)); // Log the data
            if (data.error) {
                alert(`Error: ${data.error}`);
                throw new Error(data.error);
            }

            if (type === 'centrality') {
                initializeGraphForCentrality(data);
            } else if (type === 'path-analysis') {
                initializeGraphForPathAnalysis(data);
            } else if (type === 'community-detection') {
                initializeGraphForCommunityDetection(data);
            }

            loadingElement.style.display = 'none';
        })
        .catch(error => {
            console.error('Error fetching graph data:', error);
            alert(`Failed to load graph data. Check console for details.`);
            loadingElement.style.display = 'none';
        });
}


function initializeGraphForCentrality(data) {
    const container = document.getElementById('network');
    if (!container) {
        console.error('Failed to find the network HTML element');
        return;
    }

    if (!data || data.length === 0) {
        console.error('Centrality data is missing or empty:', data);
        return;
    }

    const nodes = data.map(item => ({
        id: item.user,
        label: `User ${item.user}`,
        value: item.centrality
    }));

    const visNodes = new vis.DataSet(nodes);
    const visEdges = new vis.DataSet([]); // No edges for centrality

    const options = {
        nodes: {
            shape: 'dot',
            scaling: {
                customScalingFunction: function (min,max,total,value) {
                    return value / total;
                },
                min: 10,
                max: 150
            }
        },
        physics: {
            stabilization: false
        }
    };

    const network = new vis.Network(container, { nodes: visNodes, edges: visEdges }, options);
}

function initializeGraphForPathAnalysis(data) {
    const container = document.getElementById('network');
    if (!container) {
        console.error('Failed to find the network HTML element');
        return;
    }

    if (!data || data.length === 0) {
        console.error('Path analysis data is missing or empty:', data);
        return;
    }

    const nodes = new vis.DataSet();
    const edges = new vis.DataSet();

    data.forEach(item => {
        nodes.update({id: item.user1, label: `User ${item.user1}`, color: '#97C2FC'});
        nodes.update({id: item.user2, label: `User ${item.user2}`, color: '#FFC107'});

        item.movies.forEach((movie, index) => {
            edges.add({
                from: item.user1,
                to: item.user2,
                label: movie,
                id: `edge-${item.user1}-${item.user2}-${index}`
            });
        });
    });

    const options = {
        edges: {
            arrows: 'to',
            smooth: true
        }
    };

    const network = new vis.Network(container, { nodes, edges }, options);
}

function initializeGraphForCommunityDetection(data) {
    const container = document.getElementById('network');
    if (!container) {
        console.error('Failed to find the network HTML element');
        return;
    }

    if (!data || data.length === 0) {
        console.error('Community detection data is missing or empty:', data);
        return;
    }

    console.log("Initializing Community Detection Graph with data:", data);

    const nodes = new vis.DataSet();
    const edges = new vis.DataSet(); // Community graph might not need explicit edges

    data.forEach((community, index) => {
        community.movies.forEach((movie, movieIndex) => {
            const nodeId = `${community.communityId}-${movieIndex}`;
            nodes.add({
                id: nodeId,
                label: movie,
                group: community.communityId,
                value: 10  // Adjusting node size for better visualization
            });
        });
    });

    const options = {
        nodes: {
            shape: 'dot',
            size: 10
        },
        physics: {
            enabled: true,
            barnesHut: {
                gravitationalConstant: -8000,
                centralGravity: 0.3,
                springLength: 200,
                springConstant: 0.05,
                damping: 0.09,
                avoidOverlap: 0.1
            },
            stabilization: {
                enabled: true,
                iterations: 1000  // Increased stabilization iterations for complex graphs
            }
        },
        layout: {
            improvedLayout: false
        }
    };

    const network = new vis.Network(container, { nodes, edges }, options);
    network.on("stabilizationIterationsDone", function () {
        console.log("Stabilization complete.");
        network.setOptions({ physics: false });
    });
}

