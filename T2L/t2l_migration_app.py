import streamlit as st
import pandas as pd
import zipfile
from io import BytesIO
import xml.etree.ElementTree as ET
import re
import os
import json
from datetime import datetime
import traceback
import time

# Third-party imports
import google.generativeai as genai
from dotenv import load_dotenv

# Looker SDK imports
try:
    from looker_sdk import init40
    LOOKER_SDK_AVAILABLE = True
except ImportError:
    LOOKER_SDK_AVAILABLE = False
    st.warning("Looker SDK not available. Install with: pip install looker-sdk")

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# Page configuration
st.set_page_config(
    page_title="Tableau → Looker Migration Kit",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        padding: 1rem;
        background: linear-gradient(90deg, #f0f8ff, #e6f3ff);
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #1f77b4;
    }
    
    .success-box {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .warning-box {
        background: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .error-box {
        background: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .tab-content {
        padding: 2rem 1rem;
    }
    
    .deployment-log {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 1rem;
        font-family: monospace;
        font-size: 0.9rem;
        max-height: 400px;
        overflow-y: auto;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'parsed_data' not in st.session_state:
    st.session_state.parsed_data = None
if 'assessment_df' not in st.session_state:
    st.session_state.assessment_df = None
if 'translation_results' not in st.session_state:
    st.session_state.translation_results = None
if 'generated_lookml' not in st.session_state:
    st.session_state.generated_lookml = None
if 'deployment_logs' not in st.session_state:
    st.session_state.deployment_logs = []

# Helper Functions
def log_deployment_step(message, step_type="info"):
    """Add a deployment step to the logs"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    icon = {"info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "❌"}.get(step_type, "📝")
    log_entry = f"[{timestamp}] {icon} {message}"
    st.session_state.deployment_logs.append(log_entry)
    return log_entry

def display_deployment_logs():
    """Display deployment logs in a styled container"""
    if st.session_state.deployment_logs:
        st.markdown("### 📋 Deployment Logs")
        logs_text = "\n".join(st.session_state.deployment_logs[-20:])  # Show last 20 logs
        st.markdown(f'<div class="deployment-log">{logs_text}</div>', unsafe_allow_html=True)

def extract_twb_from_twbx(uploaded_file):
    """Extract .twb file from .twbx archive"""
    try:
        with zipfile.ZipFile(uploaded_file) as z:
            for name in z.namelist():
                if name.endswith(".twb"):
                    return z.read(name)
    except Exception as e:
        st.error(f"Error extracting TWBX: {e}")
    return None

def sanitize_identifier(name: str) -> str:
    """Make a string safe for LookML identifiers"""
    if not name:
        return "field"
    name = name.strip()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"__+", "_", name)
    name = name.strip("_").lower()
    if re.match(r"^\d", name):
        name = "_" + name
    return name or "field"

def detect_calc_complexity(formula: str) -> str:
    """Determine calculation complexity based on formula content"""
    if not formula:
        return "unknown"
    
    f = formula.upper()
    lod_keywords = ["FIXED", "INCLUDE", "EXCLUDE"]
    tablecalc_keywords = ["WINDOW", "INDEX(", "LOOKUP(", "RUNNING_", "PREVIOUS_VALUE", "RANK(", "TOTAL(", "FIRST(", "LAST("]
    custom_sql = ["RAWSQL", "RAW_SQL"]
    
    if any(k in f for k in lod_keywords):
        return "complex"
    if any(k in f for k in tablecalc_keywords):
        return "complex"
    if any(k in f for k in custom_sql):
        return "complex"
    
    simple_patterns = ["SUM(", "AVG(", "MIN(", "MAX(", "+", "-", "*", "/", "IF ", "CASE ", "DATEPART(", "DATEDIFF("]
    if any(p in f for p in simple_patterns):
        return "medium"
    
    return "medium"

def parse_tableau_xml(xml_bytes):
    """Parse Tableau XML and extract metadata"""
    parsed = {
        "datasources": [],
        "calculations": [],
        "worksheets": [],
        "parameters": [],
        "filters": [],
        "actions": []
    }
    
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        st.error(f"Unable to parse XML: {e}")
        return parsed

    # Parse datasources and columns
    for ds in root.findall(".//datasource"):
        ds_name = ds.get("name") or ds.get("caption") or "datasource"
        ds_dict = {"name": ds_name, "columns": [], "custom_sql": False}
        
        # Detect custom SQL
        for rel in ds.findall(".//relation"):
            if rel.get("table") and "custom" in (rel.get("table") or "").lower():
                ds_dict["custom_sql"] = True
                
        for col in ds.findall(".//column"):
            col_name = col.get("name") or col.get("caption") or col.get("field")
            datatype = col.get("datatype") or col.get("type") or ""
            
            # Find calculation formula
            calc_elem = None
            for c in col.findall(".//calculation"):
                calc_elem = c
                break
                
            formula = None
            if calc_elem is not None:
                formula = calc_elem.get("formula") or (calc_elem.text or "")
            else:
                formula = col.get("formula") or col.get("calculation") or None
                
            is_calc = bool(formula)
            ds_dict["columns"].append({
                "name": col_name or "unknown",
                "datatype": datatype,
                "is_calculation": is_calc,
                "formula": formula
            })
            
            if is_calc:
                parsed["calculations"].append({
                    "name": col_name or "unnamed_calc",
                    "datasource": ds_name,
                    "formula": formula,
                    "complexity": detect_calc_complexity(formula)
                })
                
        parsed["datasources"].append(ds_dict)

    # Parse worksheets
    for ws in root.findall(".//worksheet"):
        try:
            xml_str = ET.tostring(ws, encoding="unicode")
        except Exception:
            xml_str = ""
        parsed["worksheets"].append({
            "name": ws.get("name") or ws.get("caption") or "worksheet",
            "xml": xml_str
        })

    # Parse parameters
    for p in root.findall(".//parameter"):
        parsed["parameters"].append({
            "name": p.get("name") or p.get("caption"),
            "datatype": p.get("datatype") or ""
        })

    return parsed

def classify_worksheet(ws_record, parsed):
    """Classify worksheet complexity for migration assessment"""
    name = ws_record["name"]
    xml = ws_record["xml"] or ""
    
    # Find referenced calculations
    calc_names = [c["name"] for c in parsed["calculations"] if c.get("name")]
    referenced_calcs = [cn for cn in calc_names if cn and cn in xml]
    complex_calcs = [c for c in parsed["calculations"] if c["name"] in referenced_calcs and c["complexity"] == "complex"]
    medium_calcs = [c for c in parsed["calculations"] if c["name"] in referenced_calcs and c["complexity"] == "medium"]
    
    # Find parameter references
    param_names = [p["name"] for p in parsed["parameters"]]
    referenced_params = [pn for pn in param_names if pn and pn in xml]
    
    # Detect filters and actions
    has_basic_filter = "filter" in xml.lower() and not any(complex_filter in xml.lower() for complex_filter in ["advanced", "context", "condition"])
    has_complex_filter = any(complex_filter in xml.lower() for complex_filter in ["advanced", "context", "condition"])
    
    worksheet_xml = xml.lower()
    complex_action_indicators = ["url-action", "parameter-action", "go-to-sheet", "go-to-dashboard", "export-action", "tabbed-navigation", "run-command"]
    simple_action_indicators = ["filter-action", "highlight-action", "select"]
    
    has_complex_actions = any(indicator in worksheet_xml for indicator in complex_action_indicators)
    has_simple_actions = any(indicator in worksheet_xml for indicator in simple_action_indicators) and not has_complex_actions
    
    # Custom SQL detection
    ds_names = [ds["name"] for ds in parsed["datasources"]]
    referenced_ds = [d for d in ds_names if d and d in xml]
    ds_custom_sql = [ds for ds in parsed["datasources"] if ds["name"] in referenced_ds and ds.get("custom_sql")]
    
    # Classification logic
    if complex_calcs or ds_custom_sql or has_complex_actions or has_complex_filter:
        classification = "complex"
        reason_parts = []
        if complex_calcs:
            reason_parts.append("complex calculations: " + ", ".join(set([c["name"] for c in complex_calcs])))
        if has_complex_actions:
            reason_parts.append("complex dashboard actions")
        if ds_custom_sql:
            reason_parts.append("custom SQL datasource")
        if has_complex_filter:
            reason_parts.append("complex filters")
        reason = "; ".join(reason_parts) or "complex features detected"
        possible = False
        
    elif medium_calcs or referenced_params or has_basic_filter or has_simple_actions:
        classification = "medium"
        reason_parts = []
        if medium_calcs:
            medium_calc_names = [c["name"] for c in medium_calcs]
            reason_parts.append("basic calculations: " + ", ".join(set(medium_calc_names)))
        if referenced_params:
            reason_parts.append("parameters: " + ", ".join(set(referenced_params)))
        if has_basic_filter:
            reason_parts.append("basic filters")
        if has_simple_actions:
            reason_parts.append("simple actions (filter/highlight)")
        reason = "; ".join(reason_parts) or "medium complexity features"
        possible = True
        
    else:
        classification = "simple"
        reason = "basic fields and standard visualizations only"
        possible = True

    return {
        "worksheet": name,
        "classification": classification,
        "reason": reason,
        "possible_auto_migration": possible,
        "referenced_calculations": referenced_calcs,
        "referenced_datasources": referenced_ds
    }

def generate_assessment_df(parsed):
    """Generate assessment DataFrame for all worksheets"""
    rows = []
    for ws in parsed["worksheets"]:
        c = classify_worksheet(ws, parsed)
        rows.append({
            "worksheet": c["worksheet"],
            "classification": c["classification"],
            "possible_auto_migration": "Yes" if c["possible_auto_migration"] else "No",
            "reason": c["reason"],
            "referenced_calculations": "; ".join(c["referenced_calculations"]),
            "referenced_datasources": "; ".join(c["referenced_datasources"])
        })
    return pd.DataFrame(rows)

def call_gemini(prompt: str, model_name="gemini-2.0-flash-exp") -> str:
    """Call Gemini API for translation suggestions"""
    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(prompt)
        return response.text if hasattr(response, 'text') else str(response)
    except Exception as e:
        return f"# Gemini error: {e}"

def get_looker_type(datatype):
    """Convert Tableau data type to Looker type"""
    dt = (datatype or "").lower()
    if dt in ("real", "float", "double", "decimal", "number"):
        return "number"
    elif dt in ("integer", "int"):
        return "number"
    elif "date" in dt:
        return "date"
    elif dt in ("datetime", "timestamp"):
        return "date_time"
    elif dt in ("bool", "boolean"):
        return "yesno"
    else:
        return "string"

def generate_view_lookml(datasource, include_calcs_as_dimensions=True):
    """Generate LookML view for a datasource"""
    view_name = sanitize_identifier(datasource["name"])
    
    table_name = datasource["name"]
    if table_name.startswith("federated."):
        table_name = "your_table_name"
    
    content_lines = [
        f"view: {view_name} {{",
        f"  sql_table_name: {table_name} ;;",
        f"  # Generated from Tableau datasource: {datasource['name']}",
        ""
    ]
    
    # Group columns by type
    dimension_cols = []
    measure_cols = []
    calc_cols = []
    
    for col in datasource["columns"]:
        col_name = col.get("name") or "field"
        if col_name in ["unknown", ""]:
            continue
            
        if col.get("is_calculation"):
            calc_cols.append(col)
        else:
            dt = (col.get("datatype") or "").lower()
            if dt in ("real", "float", "double", "decimal", "number", "integer", "int"):
                measure_cols.append(col)
            else:
                dimension_cols.append(col)
    
    # Add dimensions
    if dimension_cols:
        content_lines.append("  # Dimensions")
    
    for col in dimension_cols:
        col_name = col.get("name")
        dimension_name = sanitize_identifier(col_name)
        looker_type = get_looker_type(col.get("datatype", ""))
        
        content_lines.append(f"  dimension: {dimension_name} {{")
        content_lines.append(f"    type: {looker_type}")
        content_lines.append(f"    sql: ${{TABLE}}.{col_name} ;;")
        content_lines.append("  }")
        content_lines.append("")
    
    # Add measures
    if measure_cols:
        content_lines.append("  # Measures")
    
    for col in measure_cols:
        col_name = col.get("name")
        dimension_name = sanitize_identifier(col_name)
        
        content_lines.append(f"  dimension: {dimension_name} {{")
        content_lines.append(f"    type: number")
        content_lines.append(f"    sql: ${{TABLE}}.{col_name} ;;")
        content_lines.append("  }")
        content_lines.append("")
        
        for measure_type in ["sum", "avg", "max", "min", "count"]:
            if measure_type == "count":
                content_lines.append(f"  measure: count_{dimension_name} {{")
                content_lines.append(f"    type: count")
                content_lines.append(f"    filters: [")
                content_lines.append(f"      {dimension_name}: \"-NULL\"")
                content_lines.append(f"    ]")
            else:
                content_lines.append(f"  measure: {measure_type}_{dimension_name} {{")
                content_lines.append(f"    type: {measure_type}")
                content_lines.append(f"    sql: ${{{dimension_name}}} ;;")
            content_lines.append("  }")
            content_lines.append("")
    
    # Add calculated fields
    if calc_cols:
        content_lines.append("  # Calculated Fields - REVIEW REQUIRED")
        
    for col in calc_cols:
        col_name = col.get("name")
        dimension_name = sanitize_identifier(col_name)
        formula = col.get("formula", "")
        
        content_lines.append(f"  # dimension: {dimension_name} {{")
        content_lines.append(f"  #   type: string")
        content_lines.append(f"  #   sql: -- TODO: Convert Tableau formula to SQL")
        content_lines.append(f"  #   # Original Tableau formula: {formula}")
        content_lines.append(f"  # }}")
        content_lines.append("")
    
    content_lines.append("}")
    return view_name, "\n".join(content_lines)

def generate_model_lookml(model_name, explores, connection_name=None):
    """Generate LookML model file"""
    if not connection_name:
        connection_name = os.getenv("LOOKER_CONNECTION_NAME", "your_connection_name")
    
    lines = [
        f"connection: \"{connection_name}\"",
        "",
        f"# Model: {model_name}",
        f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "# Include all views",
        "include: \"/views/*.view.lkml\"",
        "",
        "# Datagroup for caching",
        "datagroup: default_datagroup {",
        "  sql_trigger: SELECT MAX(id) FROM etl_log ;;",
        "  max_cache_age: \"1 hour\"",
        "}",
        "",
        "persist_with: default_datagroup",
        ""
    ]
    
    for v in explores:
        lines.append(f"explore: {v} {{")
        lines.append(f"  group_label: \"Migrated from Tableau\"")
        lines.append(f"  description: \"Data from {v} datasource\"")
        lines.append("}")
        lines.append("")
    
    return "\n".join(lines)

def generate_dashboard_lookml(dashboard_name, dashboard_elements):
    """Generate LookML dashboard file"""
    dashboard_name_clean = sanitize_identifier(dashboard_name)
    
    content_lines = [
        f"dashboard: {dashboard_name_clean} {{",
        f"  title: \"{dashboard_name}\"",
        f"  layout: newspaper",
        f"  preferred_viewer: dashboards-next",
        f"  # Generated from Tableau dashboard: {dashboard_name}",
        f"  # Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "  filters: [",
        "    {",
        "      name: date_filter",
        "      title: \"Date Range\"",
        "      type: field_filter",
        "      default_value: \"7 days\"",
        "      allow_multiple_values: true",
        "      required: false",
        "    }",
        "  ]",
        ""
    ]
    
    # Add elements
    for i, element in enumerate(dashboard_elements):
        element_name = sanitize_identifier(element.get("name", f"element_{i}"))
        explore_name = sanitize_identifier(element.get("explore", ""))
        
        content_lines.append(f"  element: {element_name} {{")
        content_lines.append(f"    title: \"{element.get('title', element.get('name', 'Untitled'))}\"")
        content_lines.append(f"    query: {explore_name} {{")
        content_lines.append(f"      # TODO: Add dimensions and measures from original Tableau worksheet")
        content_lines.append(f"      # Original worksheet: {element.get('worksheet', 'Unknown')}")
        
        # Add sample dimensions and measures
        if element.get("dimensions"):
            content_lines.append(f"      dimensions: [{', '.join(element['dimensions'])}]")
        if element.get("measures"):
            content_lines.append(f"      measures: [{', '.join(element['measures'])}]")
            
        content_lines.append("    }")
        content_lines.append(f"    type: looker_line")
        content_lines.append(f"    row: {i * 6}")
        content_lines.append(f"    col: 0")
        content_lines.append(f"    width: 12")
        content_lines.append(f"    height: 6")
        content_lines.append("  }")
        content_lines.append("")
    
    content_lines.append("}")
    return "\n".join(content_lines)

def extract_dashboard_elements(parsed_data):
    """Extract dashboard elements from parsed Tableau data"""
    dashboards = []
    
    # Look for dashboard information in worksheets
    for ws in parsed_data["worksheets"]:
        ws_name = ws["name"]
        
        # Create a basic dashboard element for each worksheet
        element = {
            "name": f"element_{sanitize_identifier(ws_name)}",
            "title": ws_name,
            "worksheet": ws_name,
            "explore": sanitize_identifier(ws_name),
            "dimensions": [],
            "measures": []
        }
        
        # Try to infer dimensions and measures from calculations
        for calc in parsed_data["calculations"]:
            if calc["complexity"] in ["simple", "medium"]:
                if any(keyword in calc["formula"].upper() for keyword in ["SUM", "COUNT", "AVG", "MAX", "MIN"]):
                    element["measures"].append(sanitize_identifier(calc["name"]))
                else:
                    element["dimensions"].append(sanitize_identifier(calc["name"]))
        
        dashboards.append({
            "name": f"dashboard_{sanitize_identifier(ws_name)}",
            "title": f"Dashboard for {ws_name}",
            "elements": [element]
        })
    
    return dashboards
    """Package complete LookML project"""
    files = {}
    
    possible_ws = assessment_df[assessment_df["possible_auto_migration"] == "Yes"]
    referenced_ds_names = set()
    
    for _, row in possible_ws.iterrows():
        ds_str = row["referenced_datasources"]
        if not ds_str:
            continue
        for ds in ds_str.split(";"):
            referenced_ds_names.add(ds.strip())
    
    if not referenced_ds_names:
        referenced_ds_names = set([ds["name"] for ds in parsed["datasources"]])

    view_names = []
    for ds in parsed["datasources"]:
        if ds["name"] in referenced_ds_names:
            vname, view_lkml = generate_view_lookml(ds)
            files[f"views/{vname}.view.lkml"] = view_lkml
            view_names.append(vname)
    
    model_name = "migrated_model"
    model_lkml = generate_model_lookml(model_name, view_names)
    files[f"models/{model_name}.model.lkml"] = model_lkml
    
    summary = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "views": view_names,
        "model": model_name,
        "datasources_included": list(referenced_ds_names),
        "note": "This is a starter LookML project - review, adjust connection name, and validate in Looker."
    }
    files["migration_summary.json"] = json.dumps(summary, indent=2)
    
    return files

def zip_files_dict(files_dict):
    """Create ZIP file from files dictionary"""
    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files_dict.items():
            zf.writestr(filename, content)
    mem.seek(0)
    return mem.read()

def deploy_lookml_to_looker():
    """Deploy LookML to Looker instance"""
    if not LOOKER_SDK_AVAILABLE:
        log_deployment_step("Looker SDK not available. Please install: pip install looker-sdk", "error")
        return False
    
    try:
        log_deployment_step("Initializing Looker SDK connection...", "info")
        
        # Initialize SDK
        sdk = init40("looker.ini")
        
        # Get project configuration from environment
        project_name = os.getenv("LOOKER_PROJECT_NAME")
        branch_name = os.getenv("LOOKER_BRANCH_NAME", "dev-migration")
        
        if not project_name:
            log_deployment_step("LOOKER_PROJECT_NAME not found in environment", "error")
            log_deployment_step("Please set LOOKER_PROJECT_NAME in your .env file", "error")
            return False
        
        log_deployment_step(f"Target project: {project_name}", "info")
        log_deployment_step(f"Target branch: {branch_name}", "info")
        
        # Validate project exists
        try:
            project = sdk.project(project_name)
            log_deployment_step(f"✅ Project found: {project.name}", "success")
            
            # Get project details
            if hasattr(project, 'git_remote_url') and project.git_remote_url:
                log_deployment_step(f"Git repository: {project.git_remote_url}", "info")
            
        except Exception as e:
            log_deployment_step(f"❌ Project '{project_name}' not found: {e}", "error")
            log_deployment_step("Available projects:", "info")
            try:
                projects = sdk.all_projects()
                for proj in projects[:5]:  # Show first 5 projects
                    log_deployment_step(f"  - {proj.name}", "info")
            except:
                pass
            return False
        
        # Validate branch if specified
        if branch_name != "main" and branch_name != "master":
            try:
                # Try to get branch info
                branches = sdk.all_git_branches(project_id=project_name)
                branch_names = [b.name for b in branches if b.name]
                
                if branch_name not in branch_names:
                    log_deployment_step(f"⚠️ Branch '{branch_name}' not found. Available branches: {', '.join(branch_names[:5])}", "warning")
                    log_deployment_step(f"Will attempt to create branch '{branch_name}'", "info")
                else:
                    log_deployment_step(f"✅ Branch '{branch_name}' found", "success")
                    
            except Exception as e:
                log_deployment_step(f"Could not validate branch: {e}", "warning")
        
        # Run project validation before deployment
        log_deployment_step("Running project validation...", "info")
        try:
            validation = sdk.validate_project(project_id=project_name)
            
            if validation.errors and len(validation.errors) > 0:
                log_deployment_step(f"⚠️ Validation found {len(validation.errors)} error(s)", "warning")
                for error in validation.errors[:3]:  # Show first 3 errors
                    log_deployment_step(f"  - {error.message}", "error")
                
                # Ask user if they want to proceed
                log_deployment_step("Proceeding with deployment despite validation errors...", "warning")
            else:
                log_deployment_step("✅ Project validation passed", "success")
                
        except Exception as e:
            log_deployment_step(f"Validation check failed: {e}", "warning")
        
        # Deploy to production
        log_deployment_step(f"Starting deployment to production from branch '{branch_name}'...", "info")
        
        try:
            # Use deploy_ref_to_production for branch deployment
            result = sdk.deploy_ref_to_production(project_id=project_name, branch=branch_name)
            
            if result:
                log_deployment_step("🎉 Deployment successful!", "success")
                log_deployment_step(f"Deployment result: {result}", "info")
            else:
                log_deployment_step("✅ Deployment completed (no result returned)", "success")
                
            # Post-deployment validation
            log_deployment_step("Running post-deployment validation...", "info")
            try:
                post_validation = sdk.validate_project(project_id=project_name)
                if post_validation.errors and len(post_validation.errors) > 0:
                    log_deployment_step(f"⚠️ Post-deployment validation found {len(post_validation.errors)} error(s)", "warning")
                else:
                    log_deployment_step("✅ Post-deployment validation passed", "success")
            except Exception as e:
                log_deployment_step(f"Post-deployment validation failed: {e}", "warning")
            
            return True
            
        except Exception as deploy_error:
            log_deployment_step(f"❌ Deployment failed: {deploy_error}", "error")
            
            # Provide detailed error information
            error_str = str(deploy_error)
            if "permission" in error_str.lower():
                log_deployment_step("💡 Check that your API user has deployment permissions", "info")
            elif "not found" in error_str.lower():
                log_deployment_step("💡 Verify project name and branch name are correct", "info")
            elif "validation" in error_str.lower():
                log_deployment_step("💡 Fix validation errors before deploying", "info")
            
            return False
            
    except Exception as e:
        log_deployment_step(f"❌ Connection error: {e}", "error")
        
        # Provide connection troubleshooting
        if "authentication" in str(e).lower() or "unauthorized" in str(e).lower():
            log_deployment_step("💡 Check your looker.ini credentials", "info")
        elif "connection" in str(e).lower() or "network" in str(e).lower():
            log_deployment_step("💡 Check network connectivity to Looker instance", "info")
        
        log_deployment_step(f"Full error: {traceback.format_exc()}", "error")
        return False

# Main App Layout
st.markdown('<div class="main-header">🔄 Tableau → Looker Migration Kit</div>', unsafe_allow_html=True)

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Environment check
    st.subheader("Environment Status")
    gemini_key = os.getenv("GEMINI_API_KEY")
    looker_project = os.getenv("LOOKER_PROJECT_NAME")
    
    st.write("🤖 Gemini API:", "✅ Configured" if gemini_key else "❌ Missing")
    st.write("🔗 Looker Project:", "✅ Configured" if looker_project else "❌ Missing")
    st.write("📦 Looker SDK:", "✅ Available" if LOOKER_SDK_AVAILABLE else "❌ Missing")
    
    if st.button("🔄 Reload Configuration"):
        load_dotenv()
        st.rerun()
    
    st.markdown("---")
    
    # Quick stats
    if st.session_state.parsed_data:
        st.subheader("📊 Quick Stats")
        st.metric("Datasources", len(st.session_state.parsed_data["datasources"]))
        st.metric("Worksheets", len(st.session_state.parsed_data["worksheets"]))
        st.metric("Calculations", len(st.session_state.parsed_data["calculations"]))
    
    st.markdown("---")
    
    # Help section
    with st.expander("ℹ️ Help & Documentation"):
        st.markdown("""
        **Migration Process:**
        1. Upload Tableau workbook
        2. Review assessment results
        3. Generate LookML files
        4. Deploy to Looker
        
        **Requirements:**
        - Tableau .twb or .twbx file
        - Looker instance with API access
        - Database connection configured in Looker
        
        **Configuration Files:**
        - `.env`: Environment variables
        - `looker.ini`: Looker API credentials
        """)

# Main content tabs
tab1, tab2, tab3, tab4 = st.tabs(["📁 Upload & Parse", "📋 Assessment", "🔧 Generate LookML", "🚀 Deploy"])

# Tab 1: Upload and Parse
with tab1:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    st.header("📁 Upload Tableau Workbook")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### File Upload")
        uploaded_file = st.file_uploader(
            "Choose a Tableau workbook file",
            type=["twb", "twbx"],
            help="Upload either a .twb or .twbx file from Tableau Desktop"
        )
        
        if uploaded_file:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            # Parse the file
            with st.spinner("🔍 Parsing workbook..."):
                xml_bytes = None
                
                if uploaded_file.name.lower().endswith(".twbx"):
                    xml_bytes = extract_twb_from_twbx(uploaded_file)
                    if xml_bytes is None:
                        st.error("❌ Could not find .twb inside .twbx file")
                else:
                    try:
                        xml_bytes = uploaded_file.read()
                    except Exception as e:
                        st.error(f"❌ Failed to read file: {e}")
                
                if xml_bytes:
                    parsed_data = parse_tableau_xml(xml_bytes)
                    st.session_state.parsed_data = parsed_data
                    
                    # Generate assessment
                    assessment_df = generate_assessment_df(parsed_data)
                    st.session_state.assessment_df = assessment_df
                    
                    st.success("✅ Workbook parsed successfully!")
    
    with col2:
        st.markdown("### Alternative Options")
        
        with st.expander("🌐 Connect to Tableau Server"):
            st.info("Feature coming soon! Will support direct connection to Tableau Server/Cloud")
            server_url = st.text_input("Server URL", placeholder="https://your-server.com")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            site_id = st.text_input("Site ID (optional)")
            
            if st.button("Connect to Server", disabled=True):
                st.warning("This feature is not yet implemented")
        
        with st.expander("📝 Upload from File"):
            st.info("You can also drag and drop files directly into the upload area above")
    
    # Display parsed results
    if st.session_state.parsed_data:
        st.markdown("---")
        st.markdown("### 📊 Workbook Analysis")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Datasources", len(st.session_state.parsed_data["datasources"]))
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Worksheets", len(st.session_state.parsed_data["worksheets"]))
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Calculations", len(st.session_state.parsed_data["calculations"]))
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Parameters", len(st.session_state.parsed_data["parameters"]))
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Datasource details
        st.markdown("#### 🗄️ Datasource Details")
        ds_rows = []
        for ds in st.session_state.parsed_data["datasources"]:
            ds_rows.append({
                "Name": ds["name"],
                "Columns": len(ds["columns"]),
                "Custom SQL": "Yes" if ds.get("custom_sql") else "No",
                "Calculated Fields": sum(1 for c in ds["columns"] if c.get("is_calculation"))
            })
        
        if ds_rows:
            st.dataframe(pd.DataFrame(ds_rows), use_container_width=True)
        
        # Calculations summary
        if st.session_state.parsed_data["calculations"]:
            st.markdown("#### 🧮 Calculated Fields")
            calc_df = pd.DataFrame(st.session_state.parsed_data["calculations"])
            
            # Add complexity breakdown
            complexity_counts = calc_df["complexity"].value_counts()
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Simple", complexity_counts.get("simple", 0))
            with col2:
                st.metric("Medium", complexity_counts.get("medium", 0))
            with col3:
                st.metric("Complex", complexity_counts.get("complex", 0))
            
            st.dataframe(calc_df, use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

# Tab 2: Assessment
with tab2:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    st.header("📋 Migration Assessment")
    
    if not st.session_state.parsed_data:
        st.warning("⚠️ Please upload and parse a Tableau workbook first in the 'Upload & Parse' tab")
    else:
        # Assessment overview
        st.markdown("### 🎯 Assessment Overview")
        
        if st.session_state.assessment_df is not None:
            assessment_df = st.session_state.assessment_df.copy()
            
            # Summary metrics
            total_worksheets = len(assessment_df)
            initial_counts = assessment_df["classification"].value_counts().to_dict()
            possible_count = (assessment_df["possible_auto_migration"] == "Yes").sum()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Simple", initial_counts.get('simple', 0), 
                         delta=f"{initial_counts.get('simple', 0)/total_worksheets*100:.1f}%")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Medium", initial_counts.get('medium', 0),
                         delta=f"{initial_counts.get('medium', 0)/total_worksheets*100:.1f}%")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col3:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Complex", initial_counts.get('complex', 0),
                         delta=f"{initial_counts.get('complex', 0)/total_worksheets*100:.1f}%")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col4:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                migration_percentage = (possible_count / total_worksheets * 100) if total_worksheets > 0 else 0
                st.metric("Auto-Migratable", possible_count,
                         delta=f"{migration_percentage:.1f}%")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Classification guidelines
            with st.expander("ℹ️ Classification Guidelines"):
                st.markdown("""
                **Simple** (Auto-migration ready):
                - Basic charts with standard fields
                - Simple aggregations (SUM, COUNT, AVG)
                - Basic filters and sorting
                
                **Medium** (Auto-migration with review):
                - Basic calculated fields (arithmetic, IF statements)
                - Parameters and simple filters
                - Basic date functions and highlight actions
                
                **Complex** (Manual migration required):
                - LOD expressions (FIXED, INCLUDE, EXCLUDE)
                - Table calculations (WINDOW functions, RANK, etc.)
                - Custom SQL and complex actions
                - Extensions, stories, advanced mapping
                """)
            
            # Editable assessment table
            st.markdown("### 📝 Review and Edit Classifications")
            st.info("💡 Review the automated classifications below and adjust as needed for your specific migration requirements")
            
            column_config = {
                "worksheet": st.column_config.TextColumn("Worksheet Name", width="medium", disabled=True),
                "classification": st.column_config.SelectboxColumn(
                    "Classification",
                    options=["simple", "medium", "complex"],
                    help="Choose the complexity level",
                    width="small"
                ),
                "possible_auto_migration": st.column_config.SelectboxColumn(
                    "Auto Migration?",
                    options=["Yes", "No"],
                    help="Can this be automatically migrated?",
                    width="small"
                ),
                "reason": st.column_config.TextColumn("Reason", width="large"),
                "referenced_calculations": st.column_config.TextColumn("Calculations", width="medium"),
                "referenced_datasources": st.column_config.TextColumn("Datasources", width="medium")
            }
            
            edited_df = st.data_editor(
                assessment_df,
                column_config=column_config,
                use_container_width=True,
                height=400,
                key="assessment_editor"
            )
            
            # Update session state
            st.session_state.assessment_df = edited_df
            
            # Bulk actions
            st.markdown("### 🔄 Bulk Actions")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📝 Mark All Medium as Simple", help="Override all medium complexity worksheets to simple"):
                    for idx in edited_df.index:
                        if edited_df.at[idx, 'classification'] == 'medium':
                            edited_df.at[idx, 'classification'] = 'simple'
                            edited_df.at[idx, 'possible_auto_migration'] = 'Yes'
                    st.session_state.assessment_df = edited_df
                    st.success("✅ Updated all medium worksheets to simple")
                    st.rerun()
            
            with col2:
                if st.button("🔄 Reset to Original", help="Reset all classifications to original assessment"):
                    original_df = generate_assessment_df(st.session_state.parsed_data)
                    st.session_state.assessment_df = original_df
                    st.success("✅ Reset to original assessment")
                    st.rerun()
            
            with col3:
                selected_worksheets = st.multiselect(
                    "Select worksheets to mark as Simple:",
                    options=edited_df['worksheet'].tolist(),
                    key="bulk_select"
                )
                if st.button("✨ Mark Selected as Simple") and selected_worksheets:
                    for worksheet in selected_worksheets:
                        idx = edited_df[edited_df['worksheet'] == worksheet].index[0]
                        edited_df.at[idx, 'classification'] = 'simple'
                        edited_df.at[idx, 'possible_auto_migration'] = 'Yes'
                    st.session_state.assessment_df = edited_df
                    st.success(f"✅ Updated {len(selected_worksheets)} worksheets to simple")
                    st.rerun()
            
            # Migration readiness analysis
            st.markdown("### 📊 Migration Readiness Analysis")
            
            updated_counts = edited_df["classification"].value_counts().to_dict()
            updated_possible = (edited_df["possible_auto_migration"] == "Yes").sum()
            updated_percentage = (updated_possible / total_worksheets * 100) if total_worksheets > 0 else 0
            
            if updated_percentage >= 70:
                st.markdown('<div class="success-box">', unsafe_allow_html=True)
                st.markdown(f"**🎉 High Migration Readiness ({updated_percentage:.1f}%)**")
                st.markdown("Excellent! Most worksheets can be auto-migrated. Proceed with confidence.")
                st.markdown('</div>', unsafe_allow_html=True)
            elif updated_percentage >= 40:
                st.markdown('<div class="warning-box">', unsafe_allow_html=True)
                st.markdown(f"**⚡ Medium Migration Readiness ({updated_percentage:.1f}%)**")
                st.markdown("Good progress. Consider reviewing complex worksheets for simplification opportunities.")
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="error-box">', unsafe_allow_html=True)
                st.markdown(f"**🔍 Low Migration Readiness ({updated_percentage:.1f}%)**")
                st.markdown("Many worksheets need manual attention. Review classifications carefully.")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Detailed breakdown
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### ✅ Ready for Auto-Migration")
                ready_worksheets = edited_df[edited_df["possible_auto_migration"] == "Yes"]
                if not ready_worksheets.empty:
                    for _, row in ready_worksheets.head(10).iterrows():
                        st.write(f"• **{row['worksheet']}** ({row['classification']})")
                    if len(ready_worksheets) > 10:
                        st.write(f"• ... and {len(ready_worksheets) - 10} more")
                else:
                    st.write("*No worksheets ready for auto-migration*")
            
            with col2:
                st.markdown("#### ⚠️ Needs Manual Review")
                manual_worksheets = edited_df[edited_df["possible_auto_migration"] == "No"]
                if not manual_worksheets.empty:
                    for _, row in manual_worksheets.head(10).iterrows():
                        reason_short = row['reason'][:50] + "..." if len(row['reason']) > 50 else row['reason']
                        st.write(f"• **{row['worksheet']}**: {reason_short}")
                    if len(manual_worksheets) > 10:
                        st.write(f"• ... and {len(manual_worksheets) - 10} more")
                else:
                    st.write("*All worksheets ready for auto-migration!* 🎉")
            
            # Export assessment
            st.markdown("### 📥 Export Assessment")
            col1, col2 = st.columns(2)
            
            with col1:
                csv_data = edited_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📄 Download CSV Report",
                    data=csv_data,
                    file_name=f"tableau_assessment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            
            with col2:
                report_data = {
                    "assessment_summary": {
                        "total_worksheets": total_worksheets,
                        "ready_for_migration": int(updated_possible),
                        "migration_percentage": updated_percentage,
                        "breakdown": updated_counts,
                        "generated_at": datetime.now().isoformat()
                    },
                    "worksheet_details": edited_df.to_dict(orient="records")
                }
                json_data = json.dumps(report_data, indent=2).encode('utf-8')
                st.download_button(
                    "📊 Download JSON Report",
                    data=json_data,
                    file_name=f"migration_assessment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
    
    st.markdown('</div>', unsafe_allow_html=True)

# Tab 3: Generate LookML
with tab3:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    st.header("🔧 Generate LookML")
    
    if not st.session_state.parsed_data or st.session_state.assessment_df is None:
        st.warning("⚠️ Please complete the upload and assessment steps first")
    else:
        # AI Translation Section
        st.markdown("### 🤖 AI-Powered Formula Translation")
        
        gemini_key = os.getenv("GEMINI_API_KEY")
        if gemini_key:
            # Get medium complexity calculations
            medium_calcs = [c for c in st.session_state.parsed_data["calculations"] if c["complexity"] == "medium"]
            
            if medium_calcs:
                st.info(f"🔍 Found {len(medium_calcs)} medium complexity calculations that can benefit from AI translation")
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("**Available Calculations for Translation:**")
                    for calc in medium_calcs[:5]:
                        st.write(f"• {calc['name']}: `{calc['formula'][:60]}...`")
                    if len(medium_calcs) > 5:
                        st.write(f"• ... and {len(medium_calcs) - 5} more")
                
                with col2:
                    if st.button("🚀 Generate AI Translations", type="primary"):
                        with st.spinner("🤖 Calling Gemini Pro for translations..."):
                            translations = []
                            batch_size = 10
                            total_batches = (len(medium_calcs) + batch_size - 1) // batch_size
                            
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            for i in range(0, len(medium_calcs), batch_size):
                                batch = medium_calcs[i:i+batch_size]
                                batch_num = (i // batch_size) + 1
                                
                                status_text.text(f"Processing batch {batch_num}/{total_batches} ({len(batch)} calculations)")
                                progress_bar.progress(batch_num / total_batches)
                                
                                # Create batch prompt
                                prompt = (
                                    "Convert these Tableau calculated fields to LookML-compatible SQL expressions. "
                                    "For each calculation, respond with:\n"
                                    "CALCULATION_NAME: [exact_name]\n"
                                    "SQL_TRANSLATION: [sql_expression]\n"
                                    "---\n\n"
                                    "Guidelines:\n"
                                    "- Use standard SQL functions\n"
                                    "- Replace IF() with CASE WHEN\n"
                                    "- Use ${parameter_name} for parameters\n"
                                    "- Keep expressions simple and readable\n\n"
                                    "Calculations to convert:\n\n"
                                )
                                
                                for j, calc in enumerate(batch, 1):
                                    prompt += f"#{j}. CALCULATION_NAME: {calc['name']}\n"
                                    prompt += f"TABLEAU_FORMULA: {calc['formula']}\n\n"
                                
                                # Get translation
                                response = call_gemini(prompt)
                                
                                # Parse response (simplified)
                                sections = response.split('---')
                                for section in sections:
                                    if 'CALCULATION_NAME:' in section and 'SQL_TRANSLATION:' in section:
                                        lines = section.strip().split('\n')
                                        calc_name = None
                                        sql_translation = None
                                        
                                        for line in lines:
                                            if line.startswith('CALCULATION_NAME:'):
                                                calc_name = line.replace('CALCULATION_NAME:', '').strip().strip('[]')
                                            elif line.startswith('SQL_TRANSLATION:'):
                                                sql_translation = line.replace('SQL_TRANSLATION:', '').strip()
                                        
                                        if calc_name and sql_translation:
                                            # Find original formula
                                            original_calc = next((c for c in batch if c['name'] == calc_name), None)
                                            if original_calc:
                                                translations.append({
                                                    "name": calc_name,
                                                    "original_formula": original_calc['formula'],
                                                    "sql_translation": sql_translation,
                                                    "datasource": original_calc['datasource']
                                                })
                                
                                time.sleep(1)  # Rate limiting
                            
                            progress_bar.progress(1.0)
                            status_text.text("✅ Translation complete!")
                            
                            st.session_state.translation_results = translations
                            
                            if translations:
                                st.success(f"✅ Generated {len(translations)} translations")
                            else:
                                st.warning("⚠️ No translations were generated. Check the Gemini API response.")
                
                # Display translation results
                if st.session_state.translation_results:
                    st.markdown("### 📝 Translation Results")
                    
                    trans_df = pd.DataFrame(st.session_state.translation_results)
                    
                    # Allow editing of translations
                    edited_translations = st.data_editor(
                        trans_df,
                        column_config={
                            "name": st.column_config.TextColumn("Field Name", disabled=True),
                            "original_formula": st.column_config.TextColumn("Original Tableau Formula", disabled=True),
                            "sql_translation": st.column_config.TextColumn("SQL Translation"),
                            "datasource": st.column_config.TextColumn("Datasource", disabled=True)
                        },
                        use_container_width=True,
                        height=300
                    )
                    
                    # Export translations
                    trans_csv = edited_translations.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Download Translation Results",
                        data=trans_csv,
                        file_name=f"tableau_translations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("ℹ️ No medium complexity calculations found for AI translation")
        else:
            st.warning("⚠️ Gemini API key not configured. Set GEMINI_API_KEY in your .env file to enable AI translations.")
        
        st.markdown("---")
        
        # LookML Generation Section
        st.markdown("### 🏗️ LookML Project Generation")
        
        assessment_df = st.session_state.assessment_df
        possible_worksheets = assessment_df[assessment_df["possible_auto_migration"] == "Yes"]
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("**Generation Summary:**")
            st.write(f"• {len(possible_worksheets)} worksheets ready for auto-migration")
            st.write(f"• {len(st.session_state.parsed_data['datasources'])} datasources to convert")
            st.write(f"• {len([c for c in st.session_state.parsed_data['calculations'] if c['complexity'] != 'complex'])} calculations to include")
            
            # Generation options
            include_comments = st.checkbox("Include detailed comments in LookML", value=True)
            include_measures = st.checkbox("Generate common measures automatically", value=True)
            include_dashboards = st.checkbox("Generate dashboard LookML files", value=True)
            connection_name = st.text_input("Connection name", 
                                          value=os.getenv("LOOKER_CONNECTION_NAME", "your_connection_name"), 
                                          help="Name of the database connection in Looker")
            model_name = st.text_input("Model name",
                                     value=os.getenv("LOOKER_MODEL_NAME", "migrated_tableau_model"),
                                     help="Name for the generated LookML model")
        
        with col2:
            st.markdown("**Generated Files:**")
            st.write("• Model file (.model.lkml)")
            st.write("• View files (.view.lkml)")
            if include_dashboards:
                st.write("• Dashboard files (.dashboard.lkml)")
            st.write("• Project manifest (manifest.lkml)")
            st.write("• Migration summary (JSON)")
            st.write("• Configuration guide")
        
        # Generate LookML button
        if st.button("🚀 Generate LookML Project", type="primary"):
            with st.spinner("🔧 Generating LookML files..."):
                try:
                    # Set environment variables for generation
                    if connection_name and connection_name != "your_connection_name":
                        os.environ["LOOKER_CONNECTION_NAME"] = connection_name
                    if model_name:
                        os.environ["LOOKER_MODEL_NAME"] = model_name
                    
                    # Generate the LookML project
                    files = package_looker_project(st.session_state.parsed_data, assessment_df)
                    
                    # Update all model files with correct connection name
                    for filename, content in files.items():
                        if filename.endswith('.model.lkml'):
                            files[filename] = content.replace('your_connection_name', connection_name)
                        elif filename.endswith('.dashboard.lkml') and not include_dashboards:
                            # Remove dashboard files if not requested
                            del files[filename]
                    
                    # Add configuration guide
                    config_guide = f"""
# Looker Migration Configuration Guide

## Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Pre-deployment Checklist:
1. ✅ Update connection name in model files (currently: '{connection_name}')
2. ✅ Verify table names match your database schema
3. ✅ Review and uncomment calculated field definitions
4. ✅ Test explores in Looker development mode
5. ✅ Validate data accuracy before production deployment

### Files Generated:
"""
                    for filename in sorted(files.keys()):
                        config_guide += f"- {filename}\n"
                    
                    config_guide += f"""
### Next Steps:
1. Download and extract the ZIP file
2. Upload files to your Looker project using the IDE
3. Commit changes and validate in development mode
4. Deploy to production using the Deployment tab

### Migration Statistics:
- Total worksheets: {len(assessment_df)}
- Auto-migrated: {len(possible_worksheets)}
- Manual review needed: {len(assessment_df) - len(possible_worksheets)}
- Datasources included: {len(st.session_state.parsed_data['datasources'])}
"""
                    
                    files["MIGRATION_GUIDE.md"] = config_guide
                    
                    # Store in session state
                    st.session_state.generated_lookml = files
                    
                    st.success("✅ LookML project generated successfully!")
                    
                except Exception as e:
                    st.error(f"❌ Error generating LookML: {e}")
                    st.error(traceback.format_exc())
        
        # Display and download generated files
        if st.session_state.generated_lookml:
            st.markdown("### 📦 Generated LookML Project")
            
            # Preview files
            with st.expander("🔍 Preview Generated Files"):
                for filename, content in st.session_state.generated_lookml.items():
                    st.markdown(f"**{filename}:**")
                    if filename.endswith(('.lkml', '.md')):
                        st.code(content[:500] + "..." if len(content) > 500 else content, 
                               language="yaml" if filename.endswith('.lkml') else "markdown")
                    else:
                        st.json(content[:500] + "..." if len(str(content)) > 500 else content)
                    st.markdown("---")
            
            # Download options
            col1, col2 = st.columns(2)
            
            with col1:
                # ZIP download
                zip_data = zip_files_dict(st.session_state.generated_lookml)
                st.download_button(
                    "📦 Download Complete LookML Project (ZIP)",
                    data=zip_data,
                    file_name=f"looker_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                    mime="application/zip",
                    type="primary"
                )
            
            with col2:
                # Individual file downloads
                selected_file = st.selectbox(
                    "Or download individual file:",
                    options=list(st.session_state.generated_lookml.keys())
                )
                
                if selected_file:
                    file_content = st.session_state.generated_lookml[selected_file]
                    if isinstance(file_content, dict):
                        file_content = json.dumps(file_content, indent=2)
                    
                    st.download_button(
                        f"📄 Download {selected_file}",
                        data=file_content.encode('utf-8'),
                        file_name=selected_file,
                        mime="text/plain"
                    )
    
    st.markdown('</div>', unsafe_allow_html=True)

# Tab 4: Deploy
with tab4:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    st.header("🚀 Deploy to Looker")
    
    if not st.session_state.generated_lookml:
        st.warning("⚠️ Please generate LookML files first in the 'Generate LookML' tab")
    else:
        # Configuration check
        st.markdown("### ⚙️ Deployment Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Environment Status:**")
            
            # Check configuration
            looker_project = os.getenv("LOOKER_PROJECT_NAME")
            looker_branch = os.getenv("LOOKER_BRANCH_NAME", "dev-migration")
            looker_connection = os.getenv("LOOKER_CONNECTION_NAME", "your_connection_name")
            
            config_status = []
            config_status.append(("Looker SDK", "✅ Available" if LOOKER_SDK_AVAILABLE else "❌ Missing"))
            config_status.append(("looker.ini", "✅ Found" if os.path.exists("looker.ini") else "❌ Missing"))
            config_status.append(("Project Name", f"✅ {looker_project}" if looker_project else "❌ Not set"))
            config_status.append(("Branch Name", f"✅ {looker_branch}" if looker_branch else "❌ Not set"))
            config_status.append(("Connection Name", f"✅ {looker_connection}" if looker_connection != "your_connection_name" else "⚠️ Default"))
            
            for item, status in config_status:
                st.write(f"• **{item}**: {status}")
        
        with col2:
            st.markdown("**Deployment Options:**")
            
            # Allow override of environment settings
            override_project = st.text_input("Override Project Name", value=looker_project or "", 
                                           help="Leave empty to use LOOKER_PROJECT_NAME from .env")
            override_branch = st.text_input("Override Branch Name", value=looker_branch or "dev-migration",
                                          help="Leave empty to use LOOKER_BRANCH_NAME from .env")
            override_connection = st.text_input("Override Connection Name", value=looker_connection,
                                              help="Connection name to use in generated LookML files")
            
            # Update environment if overrides provided
            if override_project:
                os.environ["LOOKER_PROJECT_NAME"] = override_project
            if override_branch:
                os.environ["LOOKER_BRANCH_NAME"] = override_branch
            if override_connection and override_connection != "your_connection_name":
                os.environ["LOOKER_CONNECTION_NAME"] = override_connection
            
            # Deployment mode
            deployment_mode = st.radio(
                "Deployment Mode:",
                options=["Development", "Production"],
                help="Development: Deploy to dev branch. Production: Deploy to production branch"
            )
        
        # Pre-deployment checks
        st.markdown("### 🔍 Pre-Deployment Validation")
        
        if st.button("🔍 Run Pre-Deployment Checks"):
            with st.spinner("Running validation checks..."):
                checks_passed = 0
                total_checks = 5
                
                # Check 1: Looker connection
                try:
                    if LOOKER_SDK_AVAILABLE:
                        sdk = init40("looker.ini")
                        st.success("✅ Looker API connection successful")
                        checks_passed += 1
                    else:
                        st.error("❌ Looker SDK not available")
                except Exception as e:
                    st.error(f"❌ Looker connection failed: {e}")
                
                # Check 2: Project exists
                try:
                    project_name = override_project or looker_project
                    if project_name and LOOKER_SDK_AVAILABLE:
                        sdk = init40("looker.ini")
                        project = sdk.project(project_name)
                        st.success(f"✅ Project '{project_name}' found")
                        checks_passed += 1
                    else:
                        st.error("❌ Project name not configured")
                except Exception as e:
                    st.error(f"❌ Project validation failed: {e}")
                
                # Check 3: LookML syntax validation (basic)
                lookml_files = [f for f in st.session_state.generated_lookml.keys() if f.endswith('.lkml')]
                if lookml_files:
                    st.success(f"✅ {len(lookml_files)} LookML files ready for deployment")
                    checks_passed += 1
                else:
                    st.error("❌ No LookML files found")
                
                # Check 4: Required fields present
                model_files = [f for f in st.session_state.generated_lookml.keys() if 'model' in f]
                view_files = [f for f in st.session_state.generated_lookml.keys() if 'view' in f]
                
                if model_files and view_files:
                    st.success(f"✅ Project structure valid ({len(model_files)} models, {len(view_files)} views)")
                    checks_passed += 1
                else:
                    st.error("❌ Invalid project structure")
                
                # Check 5: Environment configuration
                if all([looker_project, LOOKER_SDK_AVAILABLE, os.path.exists("looker.ini")]):
                    st.success("✅ Environment properly configured")
                    checks_passed += 1
                else:
                    st.error("❌ Environment configuration incomplete")
                
                # Summary
                if checks_passed == total_checks:
                    st.markdown('<div class="success-box">', unsafe_allow_html=True)
                    st.markdown("**🎉 All pre-deployment checks passed! Ready to deploy.**")
                    st.markdown('</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="warning-box">', unsafe_allow_html=True)
                    st.markdown(f"**⚠️ {checks_passed}/{total_checks} checks passed. Review issues before deployment.**")
                    st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Deployment section
        st.markdown("### 🚀 Deploy LookML")
        
        # Deployment warning
        if deployment_mode == "Production":
            st.warning("⚠️ **Production Deployment**: This will deploy changes to your production Looker environment. Ensure all validations have passed.")
        else:
            st.info("ℹ️ **Development Deployment**: This will deploy to your development branch for testing.")
        
        # Deployment options
        col1, col2 = st.columns(2)
        
        with col1:
            # Manual file upload instructions
            with st.expander("📝 Manual Deployment Instructions"):
                st.markdown("""
                **Alternative: Manual Upload to Looker IDE**
                
                1. Download the generated LookML ZIP file
                2. Extract files to your local machine  
                3. Open Looker IDE in your browser
                4. Navigate to your project
                5. Create/update files using the IDE
                6. Commit changes in Looker
                7. Deploy to production when ready
                
                This method gives you full control over the deployment process.
                """)
        
        with col2:
            # Automated deployment
            st.markdown("**Automated Deployment:**")
            
            # Clear logs button
            if st.button("🗑️ Clear Deployment Logs"):
                st.session_state.deployment_logs = []
                st.rerun()
            
            # Deploy button
            deploy_button_disabled = not (LOOKER_SDK_AVAILABLE and (override_project or looker_project))
            
            if st.button("🚀 Deploy to Looker", 
                        disabled=deploy_button_disabled,
                        type="primary",
                        help="Deploy generated LookML to your Looker instance"):
                
                # Clear previous logs
                st.session_state.deployment_logs = []
                
                with st.spinner("🚀 Deploying to Looker..."):
                    deployment_success = deploy_lookml_to_looker()
                    
                    if deployment_success:
                        st.success("🎉 Deployment completed successfully!")
                        
                        # Post-deployment recommendations
                        st.markdown("### ✅ Post-Deployment Steps")
                        st.markdown("""
                        **Recommended next actions:**
                        1. 🔍 Test explores in Looker development mode
                        2. 📊 Validate data accuracy against original Tableau reports  
                        3. 👥 Share with stakeholders for user acceptance testing
                        4. 📝 Document any manual adjustments needed
                        5. 🎯 Train users on new Looker dashboards
                        """)
                    else:
                        st.error("❌ Deployment failed. Check logs for details.")
                        
                        # Troubleshooting tips
                        st.markdown("### 🔧 Troubleshooting")
                        st.markdown("""
                        **Common issues and solutions:**
                        - **Connection failed**: Check looker.ini credentials
                        - **Project not found**: Verify LOOKER_PROJECT_NAME
                        - **Permission denied**: Ensure API user has deploy permissions
                        - **Validation errors**: Review LookML syntax in generated files
                        """)
        
        # Display deployment logs
        display_deployment_logs()
        
        # Deployment history and monitoring
        st.markdown("### 📊 Deployment Monitoring")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Deployment Statistics:**")
            if st.session_state.generated_lookml:
                lookml_files = [f for f in st.session_state.generated_lookml.keys() if f.endswith('.lkml')]
                model_files = [f for f in lookml_files if 'model' in f]
                view_files = [f for f in lookml_files if 'view' in f]
                
                st.metric("Total Files", len(st.session_state.generated_lookml))
                st.metric("LookML Files", len(lookml_files))
                st.metric("Models", len(model_files))
                st.metric("Views", len(view_files))
        
        with col2:
            st.markdown("**Health Checks:**")
            
            if st.button("🔍 Run Post-Deployment Validation"):
                if LOOKER_SDK_AVAILABLE:
                    try:
                        sdk = init40("looker.ini")
                        project_name = override_project or looker_project
                        
                        # Validate project
                        log_deployment_step("Running post-deployment validation...", "info")
                        
                        validation = sdk.validate_project(project_id=project_name)
                        
                        if validation.errors:
                            log_deployment_step(f"Validation found {len(validation.errors)} errors", "error")
                            for error in validation.errors[:5]:  # Show first 5 errors
                                log_deployment_step(f"Error: {error.message}", "error")
                        else:
                            log_deployment_step("✅ Project validation passed!", "success")
                        
                        # Check explores
                        explores = sdk.all_lookml_models(project_id=project_name)
                        log_deployment_step(f"Found {len(explores)} explores in project", "info")
                        
                    except Exception as e:
                        log_deployment_step(f"Validation failed: {e}", "error")
                else:
                    st.warning("Looker SDK not available for validation")
        
        # Advanced deployment options
        with st.expander("🔧 Advanced Deployment Options"):
            st.markdown("#### Git Integration")
            st.info("For advanced Git workflows, consider using Looker's Git integration features:")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                **Git Branch Management:**
                - Create feature branches for major changes
                - Use pull requests for code review
                - Tag releases for version control
                """)
            
            with col2:
                st.markdown("""
                **Rollback Strategy:**
                - Keep backup of current production state
                - Test rollback procedures in development
                - Document rollback steps for emergencies
                """)
            
            # Git operations (if available)
            if st.button("🔄 Create Backup Branch", help="Create a backup of current state before deployment"):
                st.info("Feature coming soon: Automated backup branch creation")
            
            if st.button("📝 Generate Deployment Report", help="Create detailed deployment documentation"):
                # Generate comprehensive deployment report
                report_data = {
                    "deployment_summary": {
                        "timestamp": datetime.now().isoformat(),
                        "project_name": override_project or looker_project,
                        "branch_name": override_branch or looker_branch,
                        "deployment_mode": deployment_mode,
                        "files_deployed": len(st.session_state.generated_lookml) if st.session_state.generated_lookml else 0
                    },
                    "migration_stats": {
                        "total_worksheets": len(st.session_state.assessment_df) if st.session_state.assessment_df is not None else 0,
                        "migrated_worksheets": len(st.session_state.assessment_df[st.session_state.assessment_df["possible_auto_migration"] == "Yes"]) if st.session_state.assessment_df is not None else 0,
                        "datasources": len(st.session_state.parsed_data["datasources"]) if st.session_state.parsed_data else 0,
                        "calculations": len(st.session_state.parsed_data["calculations"]) if st.session_state.parsed_data else 0
                    },
                    "files_generated": list(st.session_state.generated_lookml.keys()) if st.session_state.generated_lookml else [],
                    "deployment_logs": st.session_state.deployment_logs,
                    "recommendations": [
                        "Test all explores in development mode",
                        "Validate data accuracy against Tableau reports",
                        "Update connection names to match your environment",
                        "Review and uncomment calculated field definitions",
                        "Set up monitoring for query performance"
                    ]
                }
                
                report_json = json.dumps(report_data, indent=2)
                
                st.download_button(
                    "📊 Download Deployment Report",
                    data=report_json.encode('utf-8'),
                    file_name=f"deployment_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        # Success metrics and ROI
        if st.session_state.assessment_df is not None:
            st.markdown("### 📈 Migration Success Metrics")
            
            total_worksheets = len(st.session_state.assessment_df)
            migrated_worksheets = len(st.session_state.assessment_df[st.session_state.assessment_df["possible_auto_migration"] == "Yes"])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                migration_rate = (migrated_worksheets / total_worksheets * 100) if total_worksheets > 0 else 0
                st.metric("Migration Success Rate", f"{migration_rate:.1f}%")
            
            with col2:
                estimated_hours_saved = migrated_worksheets * 3  # Assume 3 hours per worksheet
                st.metric("Estimated Hours Saved", f"{estimated_hours_saved}h")
            
            with col3:
                manual_effort_remaining = (total_worksheets - migrated_worksheets) * 8  # 8 hours for complex worksheets
                st.metric("Manual Effort Remaining", f"{manual_effort_remaining}h")
    
    st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 20px;">
    <p>🔄 <strong>Tableau → Looker Migration Kit</strong></p>
    <p>Streamline your analytics platform migration with automated assessment, LookML generation, and deployment tools.</p>
    <p><em>For support and documentation, visit your internal wiki or contact the data engineering team.</em></p>
</div>
""", unsafe_allow_html=True)

# Debug information (only show in development)
if os.getenv("DEBUG", "false").lower() == "true":
    with st.expander("🐛 Debug Information"):
        st.markdown("**Session State:**")
        st.json({
            "parsed_data_available": st.session_state.parsed_data is not None,
            "assessment_df_available": st.session_state.assessment_df is not None,
            "translation_results_available": st.session_state.translation_results is not None,
            "generated_lookml_available": st.session_state.generated_lookml is not None,
            "deployment_logs_count": len(st.session_state.deployment_logs)
        })
        
        st.markdown("**Environment Variables:**")
        env_vars = {
            "GEMINI_API_KEY": "Set" if os.getenv("GEMINI_API_KEY") else "Not set",
            "LOOKER_PROJECT_NAME": os.getenv("LOOKER_PROJECT_NAME", "Not set"),
            "LOOKER_BRANCH_NAME": os.getenv("LOOKER_BRANCH_NAME", "Not set"),
            "DEBUG": os.getenv("DEBUG", "false")
        }
        st.json(env_vars)
                
                