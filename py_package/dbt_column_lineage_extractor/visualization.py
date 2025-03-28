"""Functions for visualizing column lineage data."""

import os


def create_html_viewer(mermaid_code, output_dir, model_node, column):
    """Create an HTML file with Mermaid viewer."""
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>DBT Column Lineage: {model_node}.{column}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'default',
            flowchart: {{
                useMaxWidth: false,
                htmlLabels: true,
                curve: 'basis'
            }}
        }});
    </script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .mermaid {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .controls {{
            margin-bottom: 20px;
        }}
        button {{
            padding: 8px 16px;
            margin-right: 10px;
            border: none;
            border-radius: 4px;
            background-color: #4CAF50;
            color: white;
            cursor: pointer;
        }}
        button:hover {{
            background-color: #45a049;
        }}
        .title {{
            margin-bottom: 20px;
            color: #333;
        }}
    </style>
</head>
<body>
    <h1 class="title">Column Lineage: {model_node}.{column}</h1>
    <div class="controls">
        <button onclick="zoomIn()">Zoom In</button>
        <button onclick="zoomOut()">Zoom Out</button>
        <button onclick="resetZoom()">Reset Zoom</button>
    </div>
    <div class="mermaid">
{mermaid_code}
    </div>
    <script>
        let scale = 1;
        const diagram = document.querySelector('.mermaid');
        
        function zoomIn() {{
            scale *= 1.2;
            diagram.style.transform = `scale(${{scale}})`;
            diagram.style.transformOrigin = 'top left';
        }}
        
        function zoomOut() {{
            scale *= 0.8;
            diagram.style.transform = `scale(${{scale}})`;
            diagram.style.transformOrigin = 'top left';
        }}
        
        function resetZoom() {{
            scale = 1;
            diagram.style.transform = `scale(${{scale}})`;
            diagram.style.transformOrigin = 'top left';
        }}
    </script>
</body>
</html>
"""
    # Create a filename based on model and column
    safe_model_name = model_node.replace('.', '_').replace('/', '_')
    safe_column_name = column.replace('.', '_').replace('/', '_')
    viewer_file = os.path.join(output_dir, f"{safe_model_name}__{safe_column_name}.html")
    
    with open(viewer_file, 'w') as f:
        f.write(html_content)
    return viewer_file


def convert_to_mermaid(model_node, column, ancestors_structured, descendants_structured):
    """Convert lineage data to Mermaid flowchart format."""
    mermaid_lines = [
        "flowchart TD",
        "    %% Styles",
        "    classDef model fill:#e3f2fd,stroke:#1565c0",
        "    classDef column fill:#f3e5f5,stroke:#6a1b9a",
        "    classDef ancestorModel fill:#e1f5fe,stroke:#01579b",
        "    classDef ancestorColumn fill:#e8f5e9,stroke:#1b5e20",
        "    classDef descendantModel fill:#f3e5f5,stroke:#4a148c",
        "    classDef descendantColumn fill:#fce4ec,stroke:#880e4f",
        "    classDef centralModel fill:#fff9c4,stroke:#f57f17,stroke-width:3px",
        "    classDef centralColumn fill:#ffecb3,stroke:#ff6f00,stroke-width:2px",
        "    %% Link styles",
        "    linkStyle default stroke:#666,stroke-width:2px",
        "    %% Nodes and relationships"
    ]
    visited_models = set()
    visited_columns = set()
    model_relationships = set()  # Track model relationships to avoid duplicates
    model_column_connections = set()  # Track model-column connections to avoid duplicates
    link_count = 0  # Counter for link styles
    
    def get_model_name(full_node):
        """Extract model name from full node path."""
        return full_node.split('.')[-1]
    
    def add_model_and_column(node, col, direction=None):
        """Add model and column nodes with appropriate styling."""
        nonlocal link_count
        model_id = f"model_{node}".replace('.', '_')
        column_id = f"{node}_{col}".replace('.', '_')
        model_label = get_model_name(node)
        
        # Add model node if not visited
        if model_id not in visited_models:
            mermaid_lines.append(f"    {model_id}[{model_label}]")
            visited_models.add(model_id)
            # Apply style based on direction
            if direction == 'up':
                mermaid_lines.append(f"    class {model_id} ancestorModel")
            elif direction == 'down':
                mermaid_lines.append(f"    class {model_id} descendantModel")
            else:
                mermaid_lines.append(f"    class {model_id} model")
        
        # Add column node if not visited
        if column_id not in visited_columns:
            mermaid_lines.append(f"    {column_id}{{'{col}'}}") # Use curly braces for column nodes
            visited_columns.add(column_id)
            # Apply style based on direction
            if direction == 'up':
                mermaid_lines.append(f"    class {column_id} ancestorColumn")
            elif direction == 'down':
                mermaid_lines.append(f"    class {column_id} descendantColumn")
            else:
                mermaid_lines.append(f"    class {column_id} column")
        
        # Only connect model to column with a dotted line if not already connected
        model_column_connection = (model_id, column_id)
        if model_column_connection not in model_column_connections:
            mermaid_lines.append(f"    {model_id} -.- {column_id}")
            # Style the connection line to be thinner and lighter
            mermaid_lines.append(f"    linkStyle {link_count} stroke:#999,stroke-width:1px,stroke-dasharray:3")
            link_count += 1
            model_column_connections.add(model_column_connection)
        
        return model_id, column_id
    
    def process_structure(structure, direction='up', parent_ids=None):
        """Process the structured lineage data recursively."""
        nonlocal link_count
        current_level_ids = []
        
        for node, columns in structure.items():
            for col, details in columns.items():
                # Add source model and column nodes
                source_model_id, source_column_id = add_model_and_column(node, col, direction)
                current_level_ids.extend([source_model_id, source_column_id])
                
                # Connect to parent level if exists
                if parent_ids:
                    for parent_model_id, parent_column_id in zip(parent_ids[::2], parent_ids[1::2]):
                        # Add relationship between models with solid arrow if not already added
                        model_rel = (source_model_id, parent_model_id) if direction == 'down' else (parent_model_id, source_model_id)
                        
                        # Skip self-references (models pointing to themselves)
                        if source_model_id != parent_model_id and model_rel not in model_relationships:
                            if direction == 'up':
                                mermaid_lines.append(f"    {source_model_id} --> {parent_model_id}")
                            else:
                                mermaid_lines.append(f"    {parent_model_id} --> {source_model_id}")
                            # Style the model relationship
                            mermaid_lines.append(f"    linkStyle {link_count} stroke:#333,stroke-width:2px")
                            link_count += 1
                            model_relationships.add(model_rel)
                        
                        # Add relationship between columns with dashed arrow
                        # Skip self-references (columns in the same model pointing to themselves)
                        if not (source_model_id == parent_model_id and source_column_id == parent_column_id):
                            if direction == 'up':
                                mermaid_lines.append(f"    {source_column_id} -.-> {parent_column_id}")
                            else:
                                mermaid_lines.append(f"    {parent_column_id} -.-> {source_column_id}")
                            # Style the column relationship
                            mermaid_lines.append(f"    linkStyle {link_count} stroke:#666,stroke-width:1.5px")
                            link_count += 1
                
                # Process nested relationships
                if '+' in details:
                    nested_ids = []
                    for nested_node, nested_cols in details['+'].items():
                        for nested_col in nested_cols:
                            # Add target model and column nodes
                            target_model_id, target_column_id = add_model_and_column(nested_node, nested_col, direction)
                            nested_ids.extend([target_model_id, target_column_id])
                            
                            # Add relationship between models with solid arrow if not already added
                            model_rel = (target_model_id, source_model_id) if direction == 'up' else (source_model_id, target_model_id)
                            
                            # Skip self-references (models pointing to themselves)
                            if target_model_id != source_model_id and model_rel not in model_relationships:
                                if direction == 'up':
                                    mermaid_lines.append(f"    {target_model_id} --> {source_model_id}")
                                else:
                                    mermaid_lines.append(f"    {source_model_id} --> {target_model_id}")
                                # Style the model relationship
                                mermaid_lines.append(f"    linkStyle {link_count} stroke:#333,stroke-width:2px")
                                link_count += 1
                                model_relationships.add(model_rel)
                            
                            # Add relationship between columns with dashed arrow
                            # Skip self-references (columns in the same model pointing to themselves)
                            if not (target_model_id == source_model_id and target_column_id == source_column_id):
                                if direction == 'up':
                                    mermaid_lines.append(f"    {target_column_id} -.-> {source_column_id}")
                                else:
                                    mermaid_lines.append(f"    {source_column_id} -.-> {target_column_id}")
                                # Style the column relationship
                                mermaid_lines.append(f"    linkStyle {link_count} stroke:#666,stroke-width:1.5px")
                                link_count += 1
                            
                            # Process nested structure recursively
                            if nested_cols[nested_col].get('+'):
                                process_structure({nested_node: {nested_col: nested_cols[nested_col]}}, direction, [target_model_id, target_column_id])
                    
                    if nested_ids:
                        current_level_ids.extend(nested_ids)
        
        return current_level_ids
    
    # Add the central model and column nodes first
    central_model_id = f"model_{model_node}".replace('.', '_')
    central_column_id = f"{model_node}_{column}".replace('.', '_')
    model_label = get_model_name(model_node)
    
    mermaid_lines.append(f"    {central_model_id}[{model_label}]")
    mermaid_lines.append(f"    {central_column_id}{{'{column}'}}")
    mermaid_lines.append(f"    class {central_model_id} centralModel")
    mermaid_lines.append(f"    class {central_column_id} centralColumn")
    visited_models.add(central_model_id)
    visited_columns.add(central_column_id)
    
    # Connect central model and column
    mermaid_lines.append(f"    {central_model_id} -.- {central_column_id}")
    mermaid_lines.append(f"    linkStyle {link_count} stroke:#999,stroke-width:1px,stroke-dasharray:3")
    link_count += 1
    
    # Process ancestors (upstream) relationships
    if ancestors_structured:
        process_structure(ancestors_structured, 'up', [central_model_id, central_column_id])
    
    # Process descendants (downstream) relationships
    if descendants_structured:
        process_structure(descendants_structured, 'down', [central_model_id, central_column_id])
    
    return "\n".join(mermaid_lines) 