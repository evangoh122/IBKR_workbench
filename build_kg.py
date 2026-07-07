import ast
import os
import json
from pathlib import Path

# Files to analyze
FILES_TO_ANALYZE = [
    "main.py", "query.py", "rag_engine.py", "db/database.py", 
    "dashboard/app.py", "config/tickers.py", "config/update_tickers.py",
    "etl/chat_engine.py", "etl/embed_tickers.py", "etl/extract_cot.py", 
    "etl/extract_edgar.py", "etl/extract_options.py", "etl/extract_polygon.py", 
    "etl/extract_stocks.py", "etl/ibkr_client.py", "etl/polygon_client.py", 
    "etl/slippage.py", "etl/utils.py"
]

def analyze_file(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=filepath)
    except Exception as e:
        return None
    
    classes = []
    functions = []
    imports = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module if node.module else ""
            for alias in node.names:
                imports.append(f"{module}.{alias.name}" if module else alias.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)
        elif isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            functions.append(node.name)
            
    return {
        "module": filepath.replace(".py", "").replace("\\", "/").replace("/", "."),
        "filepath": filepath,
        "classes": classes,
        "functions": functions,
        "imports": imports
    }

def main():
    knowledge_graph = []
    
    for file in FILES_TO_ANALYZE:
        if os.path.exists(file):
            data = analyze_file(file)
            if data:
                knowledge_graph.append(data)
                
    # Generate Mermaid Diagram
    mermaid = ["```mermaid", "flowchart TD"]
    
    for item in knowledge_graph:
        mod_name = item["module"]
        mod_id = mod_name.replace(".", "_")
        
        mermaid.append(f"    subgraph {mod_id}[\"{mod_name}\"]")
        
        for cls in item["classes"]:
            mermaid.append(f"        {mod_id}_{cls}([{cls}])")
            
        for func in item["functions"]:
            if not func.startswith("__"):
                mermaid.append(f"        {mod_id}_{func}({func})")
                
        mermaid.append("    end")
        
    for item in knowledge_graph:
        mod_name = item["module"]
        mod_id = mod_name.replace(".", "_")
        
        for imp in item["imports"]:
            for other in knowledge_graph:
                if imp == other["module"] or imp.startswith(other["module"] + "."):
                    other_id = other["module"].replace(".", "_")
                    mermaid.append(f"    {mod_id} --> {other_id}")
                    break
    
    mermaid.append("```")
    
    markdown_output = "# Codebase Knowledge Graph\n\n"
    markdown_output += "\n".join(mermaid) + "\n\n"
    markdown_output += "## Modules and Contents\n\n"
    
    for item in knowledge_graph:
        markdown_output += f"### {item['module']}\n"
        markdown_output += f"- **File**: `{item['filepath']}`\n"
        if item["classes"]:
            markdown_output += f"- **Classes**: {', '.join(item['classes'])}\n"
        if item["functions"]:
            funcs = [f for f in item["functions"] if not f.startswith("__")]
            markdown_output += f"- **Functions**: {', '.join(funcs)}\n"
        if item["imports"]:
            local_imports = [imp for imp in item["imports"] if any(imp.startswith(other["module"]) for other in knowledge_graph)]
            if local_imports:
                markdown_output += f"- **Internal Dependencies**: {', '.join(local_imports)}\n"
        markdown_output += "\n"
        
    artifact_path = os.getenv("KG_OUTPUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "knowledge_graph.md"))
    with open(artifact_path, "w", encoding="utf-8") as f:
        f.write(markdown_output)
    print(f"Generated {artifact_path}")

if __name__ == "__main__":
    main()
