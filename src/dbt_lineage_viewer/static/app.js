/**
 * DBT Lineage Viewer - Interactive DAG with Cytoscape + ELK
 */

// Register ELK layout
cytoscape.use(cytoscapeElk);

// Color map for node types
const TYPE_COLORS = {
    source: '#4ade80',
    seed: '#a78bfa',
    staging: '#60a5fa',
    intermediate: '#fbbf24',
    mart: '#f472b6',
    output: '#f87171',
    model: '#94a3b8',
    exposure: '#2dd4bf',
    static: '#c084fc',
    snapshot: '#fb923c',
};

// Cytoscape instance
let cy = null;

// Current selection state
let selectedNode = null;
let highlightedNodes = new Set();

/**
 * Initialize the app
 */
async function init() {
    document.body.classList.add('loading');
    
    try {
        const response = await fetch('/api/graph');
        const graph = await response.json();
        
        initCytoscape(graph);
        updateMetadata(graph.metadata);
        setupEventListeners();
        
    } catch (err) {
        console.error('Failed to load graph:', err);
        document.getElementById('status').textContent = 'Failed to load graph';
    }
    
    document.body.classList.remove('loading');
}

/**
 * Initialize Cytoscape with the graph data
 */
function initCytoscape(graph) {
    cy = cytoscape({
        container: document.getElementById('cy'),
        elements: [...graph.nodes, ...graph.edges],
        
        style: [
            // Nodes
            {
                selector: 'node',
                style: {
                    'label': 'data(shortLabel)',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'font-size': '10px',
                    'color': '#fff',
                    'text-outline-color': '#1a1a2e',
                    'text-outline-width': 2,
                    'background-color': (ele) => TYPE_COLORS[ele.data('type')] || TYPE_COLORS.model,
                    'width': 30,
                    'height': 30,
                    'border-width': 0,
                }
            },
            // Edges
            {
                selector: 'edge',
                style: {
                    'width': 1.5,
                    'line-color': '#4a5568',
                    'target-arrow-color': '#4a5568',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier',
                    'arrow-scale': 0.8,
                }
            },
            // Highlighted nodes (in lineage)
            {
                selector: 'node.highlighted',
                style: {
                    'border-width': 3,
                    'border-color': '#e94560',
                    'width': 40,
                    'height': 40,
                }
            },
            // Selected node
            {
                selector: 'node.selected',
                style: {
                    'border-width': 4,
                    'border-color': '#fff',
                    'width': 45,
                    'height': 45,
                }
            },
            // Highlighted edges
            {
                selector: 'edge.highlighted',
                style: {
                    'width': 3,
                    'line-color': '#e94560',
                    'target-arrow-color': '#e94560',
                }
            },
            // Dimmed (not in lineage)
            {
                selector: 'node.dimmed',
                style: {
                    'opacity': 0.15,
                }
            },
            {
                selector: 'edge.dimmed',
                style: {
                    'opacity': 0.1,
                }
            },
            // Search match
            {
                selector: 'node.search-match',
                style: {
                    'border-width': 3,
                    'border-color': '#fbbf24',
                }
            },
            // Hidden
            {
                selector: '.hidden',
                style: {
                    'display': 'none',
                }
            },
        ],
        
        // Layout will be applied after
        layout: { name: 'preset' },
        
        // Interaction
        minZoom: 0.1,
        maxZoom: 3,
        wheelSensitivity: 0.3,
    });
    
    // Apply ELK layout
    runLayout();
    
    // Node click handler
    cy.on('tap', 'node', async (evt) => {
        const node = evt.target;
        await selectNode(node.id());
    });
    
    // Background click - deselect
    cy.on('tap', (evt) => {
        if (evt.target === cy) {
            clearSelection();
        }
    });
    
    // Update status
    document.getElementById('status').textContent = 
        `${graph.nodes.length} nodes, ${graph.edges.length} edges`;
}

/**
 * Run ELK layout
 */
function runLayout() {
    cy.layout({
        name: 'elk',
        elk: {
            algorithm: 'layered',
            'elk.direction': 'RIGHT',
            'elk.spacing.nodeNode': 50,
            'elk.layered.spacing.nodeNodeBetweenLayers': 100,
            'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
            'elk.edgeRouting': 'ORTHOGONAL',
        },
        fit: true,
        padding: 50,
    }).run();
}

/**
 * Select a node and show its lineage
 */
async function selectNode(nodeId) {
    const depth = parseInt(document.getElementById('depth').value);
    
    try {
        const response = await fetch(`/api/lineage/${encodeURIComponent(nodeId)}?depth=${depth}`);
        const lineage = await response.json();
        
        const nodeResponse = await fetch(`/api/node/${encodeURIComponent(nodeId)}`);
        const nodeData = await nodeResponse.json();
        
        // Clear previous selection
        cy.elements().removeClass('highlighted selected dimmed');
        
        // Build set of relevant nodes
        highlightedNodes = new Set([nodeId, ...lineage.upstream, ...lineage.downstream]);
        
        // Apply styles
        cy.nodes().forEach(n => {
            if (highlightedNodes.has(n.id())) {
                if (n.id() === nodeId) {
                    n.addClass('selected');
                } else {
                    n.addClass('highlighted');
                }
            } else {
                n.addClass('dimmed');
            }
        });
        
        cy.edges().forEach(e => {
            const srcHighlighted = highlightedNodes.has(e.source().id());
            const tgtHighlighted = highlightedNodes.has(e.target().id());
            if (srcHighlighted && tgtHighlighted) {
                e.addClass('highlighted');
            } else {
                e.addClass('dimmed');
            }
        });
        
        selectedNode = nodeId;
        
        // Show panel
        showNodePanel(nodeData, lineage);
        
    } catch (err) {
        console.error('Failed to get lineage:', err);
    }
}

/**
 * Clear selection and restore full view
 */
function clearSelection() {
    cy.elements().removeClass('highlighted selected dimmed');
    selectedNode = null;
    highlightedNodes.clear();
    hidePanel();
}

/**
 * Show the node details panel
 */
function showNodePanel(node, lineage) {
    const panel = document.getElementById('panel');
    const content = document.getElementById('panel-content');
    
    // Build upstream/downstream links
    const upstreamLinks = lineage.upstream.map(id => {
        const n = cy.getElementById(id);
        return `<span class="lineage-link" data-id="${id}">${n.data('shortLabel') || id}</span>`;
    }).join('');
    
    const downstreamLinks = lineage.downstream.map(id => {
        const n = cy.getElementById(id);
        return `<span class="lineage-link" data-id="${id}">${n.data('shortLabel') || id}</span>`;
    }).join('');
    
    // Tags
    const tags = (node.tags || []).map(t => `<span class="tag">${t}</span>`).join('');
    
    // Code section
    let codeSection = '';
    const code = node.compiledCode || node.rawCode;
    if (code) {
        codeSection = `
            <div class="section">
                <h3>SQL</h3>
                <pre><code>${escapeHtml(code)}</code></pre>
            </div>
        `;
    }
    
    // Columns section
    let columnsSection = '';
    if (node.columns && node.columns.length > 0) {
        const columnsList = node.columns.map(c => 
            `<li><strong>${c.name}</strong>${c.dataType ? ` (${c.dataType})` : ''}${c.description ? `: ${c.description}` : ''}</li>`
        ).join('');
        columnsSection = `
            <div class="section">
                <h3>Columns</h3>
                <ul style="font-size: 12px; padding-left: 18px;">${columnsList}</ul>
            </div>
        `;
    }
    
    content.innerHTML = `
        <h2>${node.label}</h2>
        <span class="type-badge ${node.type}">${node.type}</span>
        
        <dl class="meta">
            ${node.path ? `<dt>Path</dt><dd>${node.path}</dd>` : ''}
            ${node.schema ? `<dt>Schema</dt><dd>${node.database ? node.database + '.' : ''}${node.schema}</dd>` : ''}
            ${node.materialized ? `<dt>Materialized</dt><dd>${node.materialized}</dd>` : ''}
            ${node.description ? `<dt>Description</dt><dd>${node.description}</dd>` : ''}
        </dl>
        
        ${tags ? `<div class="section"><h3>Tags</h3>${tags}</div>` : ''}
        
        <div class="section">
            <h3>Upstream (${lineage.upstream.length})</h3>
            <div class="lineage-links">${upstreamLinks || '<span style="color: #666;">None</span>'}</div>
        </div>
        
        <div class="section">
            <h3>Downstream (${lineage.downstream.length})</h3>
            <div class="lineage-links">${downstreamLinks || '<span style="color: #666;">None</span>'}</div>
        </div>
        
        ${columnsSection}
        ${codeSection}
    `;
    
    // Add click handlers for lineage links
    content.querySelectorAll('.lineage-link').forEach(link => {
        link.addEventListener('click', () => {
            const id = link.dataset.id;
            selectNode(id);
            cy.getElementById(id).select();
            cy.center(cy.getElementById(id));
        });
    });
    
    panel.classList.remove('hidden');
}

/**
 * Hide the panel
 */
function hidePanel() {
    document.getElementById('panel').classList.add('hidden');
}

/**
 * Setup event listeners
 */
function setupEventListeners() {
    // Search
    const searchInput = document.getElementById('search');
    let searchTimeout;
    searchInput.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            const query = searchInput.value.toLowerCase().trim();
            applySearch(query);
        }, 200);
    });
    
    // Depth slider
    const depthSlider = document.getElementById('depth');
    const depthValue = document.getElementById('depth-value');
    depthSlider.addEventListener('input', () => {
        depthValue.textContent = depthSlider.value;
        if (selectedNode) {
            selectNode(selectedNode);
        }
    });
    
    // Reset view
    document.getElementById('reset-view').addEventListener('click', () => {
        clearSelection();
        cy.elements().removeClass('search-match hidden');
        searchInput.value = '';
        cy.fit(50);
    });
    
    // Reload
    document.getElementById('reload').addEventListener('click', async () => {
        try {
            const response = await fetch('/api/reload', { method: 'POST' });
            const result = await response.json();
            
            // Re-fetch and reinitialize
            const graphResponse = await fetch('/api/graph');
            const graph = await graphResponse.json();
            
            cy.destroy();
            initCytoscape(graph);
            updateMetadata(graph.metadata);
            
            document.getElementById('status').textContent = 'Reloaded!';
        } catch (err) {
            console.error('Failed to reload:', err);
            document.getElementById('status').textContent = 'Reload failed';
        }
    });
    
    // Close panel
    document.getElementById('close-panel').addEventListener('click', () => {
        clearSelection();
    });
    
    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        // Escape to clear
        if (e.key === 'Escape') {
            clearSelection();
            searchInput.value = '';
            cy.elements().removeClass('search-match hidden');
        }
        // Focus search on /
        if (e.key === '/' && document.activeElement !== searchInput) {
            e.preventDefault();
            searchInput.focus();
        }
    });
}

/**
 * Apply search filter
 */
function applySearch(query) {
    cy.nodes().removeClass('search-match');
    
    if (!query) {
        cy.elements().removeClass('hidden');
        return;
    }
    
    // Find matching nodes
    const matches = cy.nodes().filter(n => {
        const label = (n.data('label') || '').toLowerCase();
        const path = (n.data('path') || '').toLowerCase();
        return label.includes(query) || path.includes(query);
    });
    
    matches.addClass('search-match');
    
    // Don't hide non-matches if we have a selection active
    if (!selectedNode) {
        // Optionally: hide non-matches
        // cy.nodes().not(matches).addClass('hidden');
    }
    
    // Center on first match
    if (matches.length > 0) {
        cy.center(matches);
    }
    
    document.getElementById('status').textContent = 
        `${matches.length} match${matches.length === 1 ? '' : 'es'}`;
}

/**
 * Update metadata display
 */
function updateMetadata(metadata) {
    document.getElementById('metadata').innerHTML = 
        `<strong>${metadata.projectName}</strong> · dbt ${metadata.dbtVersion}`;
}

/**
 * Escape HTML
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Initialize on load
document.addEventListener('DOMContentLoaded', init);
