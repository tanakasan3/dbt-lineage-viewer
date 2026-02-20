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
            // Column source highlighting
            {
                selector: 'node.column-highlighted',
                style: {
                    'border-width': 4,
                    'border-color': '#22d3ee',
                    'border-style': 'double',
                }
            },
            {
                selector: 'node.column-source',
                style: {
                    'border-width': 3,
                    'border-color': '#a78bfa',
                }
            },
            {
                selector: 'edge.column-highlighted',
                style: {
                    'width': 3,
                    'line-color': '#a78bfa',
                    'line-style': 'dashed',
                    'target-arrow-color': '#a78bfa',
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
    
    // Columns section - always analyze from SQL (manifest columns are often incomplete)
    let columnsSection = '';
    const hasSql = node.compiledCode || node.rawCode;
    
    if (hasSql) {
        columnsSection = `
            <div class="section">
                <h3>Columns <span class="column-hint">(click to trace)</span></h3>
                <div id="parsed-columns"><span style="color: #888;">Loading...</span></div>
            </div>
        `;
    }
    
    // Column lineage panel (hidden by default)
    const columnLineageSection = `
        <div id="column-lineage-section" class="section hidden">
            <h3>🔗 Column Lineage: <span id="traced-column-name"></span></h3>
            <div id="column-lineage-content"></div>
            <button id="close-column-lineage" class="close-lineage-btn">Close</button>
        </div>
    `;
    
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
        ${columnLineageSection}
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
    
    // Auto-analyze columns if SQL is available
    if (hasSql) {
        analyzeColumns(node.id);
    }
    
    // Close column lineage button
    const closeLineageBtn = content.querySelector('#close-column-lineage');
    if (closeLineageBtn) {
        closeLineageBtn.addEventListener('click', () => {
            document.getElementById('column-lineage-section').classList.add('hidden');
            // Clear column highlighting
            cy.elements().removeClass('column-highlighted column-source');
        });
    }
    
    panel.classList.remove('hidden');
}

/**
 * Analyze columns from SQL
 */
async function analyzeColumns(nodeId) {
    const container = document.getElementById('parsed-columns');
    container.innerHTML = '<span style="color: #888;">Analyzing SQL...</span>';
    
    try {
        const response = await fetch(`/api/column-lineage/${encodeURIComponent(nodeId)}`);
        const data = await response.json();
        
        if (data.error) {
            container.innerHTML = `<span style="color: #f87171;">Error: ${data.error}</span>`;
            return;
        }
        
        const columns = Object.keys(data.columns);
        if (columns.length === 0) {
            container.innerHTML = '<span style="color: #888;">No columns detected</span>';
            return;
        }
        
        const columnsList = columns.map(col => {
            const info = data.columns[col];
            const sourceInfo = info.sources.length > 0 
                ? `<span class="source-hint">← ${info.sources.map(s => `${s.table}.${s.column}`).join(', ')}</span>`
                : '';
            return `<li class="column-item" data-column="${escapeHtml(col)}">
                <strong>${col}</strong>
                ${info.isDerived ? ' <span class="derived-badge">derived</span>' : ''}
                ${sourceInfo}
            </li>`;
        }).join('');
        
        container.innerHTML = `<ul class="columns-list">${columnsList}</ul>`;
        
        // Add click handlers
        container.querySelectorAll('.column-item').forEach(item => {
            item.addEventListener('click', () => {
                const columnName = item.dataset.column;
                traceColumnLineage(nodeId, columnName);
            });
        });
        
    } catch (err) {
        container.innerHTML = `<span style="color: #f87171;">Failed to analyze</span>`;
        console.error('Failed to analyze columns:', err);
    }
}

/**
 * Trace column lineage upstream
 */
async function traceColumnLineage(nodeId, columnName) {
    const section = document.getElementById('column-lineage-section');
    const nameSpan = document.getElementById('traced-column-name');
    const content = document.getElementById('column-lineage-content');
    
    section.classList.remove('hidden');
    nameSpan.textContent = columnName;
    content.innerHTML = '<span style="color: #888;">Tracing upstream...</span>';
    
    try {
        const depth = parseInt(document.getElementById('depth').value);
        const response = await fetch(
            `/api/column-trace/${encodeURIComponent(nodeId)}/${encodeURIComponent(columnName)}?depth=${depth}`
        );
        const data = await response.json();
        
        if (data.error) {
            content.innerHTML = `<span style="color: #f87171;">Error: ${data.error}</span>`;
            return;
        }
        
        // Show current lineage
        let html = '';
        
        if (data.currentLineage) {
            const curr = data.currentLineage;
            if (curr.expression) {
                html += `<div class="lineage-expr"><code>${escapeHtml(curr.expression)}</code></div>`;
            }
            if (curr.sources && curr.sources.length > 0) {
                html += '<div class="direct-sources"><strong>Direct sources:</strong><ul>';
                for (const src of curr.sources) {
                    const modelLink = src.upstreamModel 
                        ? `<span class="lineage-link" data-id="${src.upstreamModel}">${src.upstreamModel.split('.').pop()}</span>`
                        : src.resolvedTable || src.table;
                    html += `<li>${modelLink}.<strong>${src.column}</strong>`;
                    if (src.transformation) {
                        html += ` <span class="transform-badge">${src.transformation}</span>`;
                    }
                    html += '</li>';
                }
                html += '</ul></div>';
            }
        }
        
        // Show trace
        if (data.trace && data.trace.length > 0) {
            html += '<div class="upstream-trace"><strong>Upstream trace:</strong><ul>';
            
            // Group by model
            const byModel = {};
            for (const t of data.trace) {
                const model = t.model;
                if (!byModel[model]) byModel[model] = [];
                byModel[model].push(t);
            }
            
            for (const [model, cols] of Object.entries(byModel)) {
                const modelName = model.split('.').pop();
                html += `<li><span class="lineage-link" data-id="${model}">${modelName}</span>: `;
                html += cols.map(c => `<strong>${c.column}</strong>`).join(', ');
                html += '</li>';
            }
            
            html += '</ul></div>';
            
            // Highlight upstream models in graph
            highlightColumnSources(nodeId, data.trace);
        } else {
            html += '<div style="color: #888; margin-top: 8px;">No further upstream sources found</div>';
        }
        
        content.innerHTML = html;
        
        // Add click handlers for model links
        content.querySelectorAll('.lineage-link').forEach(link => {
            link.addEventListener('click', () => {
                const id = link.dataset.id;
                selectNode(id);
                cy.getElementById(id).select();
                cy.center(cy.getElementById(id));
            });
        });
        
    } catch (err) {
        content.innerHTML = `<span style="color: #f87171;">Failed to trace</span>`;
        console.error('Failed to trace column:', err);
    }
}

/**
 * Highlight nodes that are sources for a column
 */
function highlightColumnSources(currentNodeId, trace) {
    // Clear previous column highlighting
    cy.elements().removeClass('column-highlighted column-source');
    
    // Mark current node
    cy.getElementById(currentNodeId).addClass('column-highlighted');
    
    // Mark all source models
    const sourceModels = new Set(trace.map(t => t.model));
    for (const modelId of sourceModels) {
        const node = cy.getElementById(modelId);
        if (node.length > 0) {
            node.addClass('column-source');
        }
    }
    
    // Highlight edges between them
    cy.edges().forEach(edge => {
        const srcInTrace = sourceModels.has(edge.source().id()) || edge.source().id() === currentNodeId;
        const tgtInTrace = sourceModels.has(edge.target().id()) || edge.target().id() === currentNodeId;
        if (srcInTrace && tgtInTrace) {
            edge.addClass('column-highlighted');
        }
    });
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
