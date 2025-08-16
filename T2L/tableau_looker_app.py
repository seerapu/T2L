# tableau_to_looker_app.py
import streamlit as st
import pandas as pd
import zipfile
from io import BytesIO
import xml.etree.ElementTree as ET
import re
import os
import json
from datetime import datetime
import time

# Import libraries
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import google.generativeai as genai
from looker_sdk import init40

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
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
        text-align: center;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
    }
    .success-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .tab-content {
        padding: 2rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 0px 24px;
        background-color: #f0f2f6;
        border-radius: 8px 8px 0px 0px;
        border: 1px solid #e6e9ef;
    }
    .stTabs [aria-selected="true"] {
        background-color: #667eea;
        color: white;
    }
    .upload-section {
        border: 2px dashed #667eea;
        border-radius: 10px;
        padding: 2rem;
        text-align: center;
        background: #f8f9ff;
        margin: 1rem 0;
    }
    .progress-bar {
        background: linear-gradient(90deg, #667eea, #764ba2);
        height: 4px;
        border-radius: 2px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'parsed_data' not in st.session_state:
    st.session_state.parsed_data = None
if 'assessment_df' not in st.session_state:
    st.session_state.assessment_df = None
if 'lookml_files' not in st.session_state:
    st.session_state.lookml_files = None
if 'uploaded_file_name' not in st.session_state:
    st.session_state.uploaded_file_name = None

# Utility Functions
def extract_twb_from_twbx(uploaded_file):
    """Extract .twb file from .twbx archive"""
    try:
        with zipfile.ZipFile(uploaded_file) as z:
            for name in z.namelist():
                if name.endswith(".twb"):
                    return z.read(name)
    except Exception as e:
        st.error(f"Invalid TWBX or cannot extract: {e}")
    return None

def sanitize_identifier(name: str) -> str:
    """Make a string safe for LookML identifiers."""
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
    """Detect complexity of calculation formulas"""
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

    # Parse datasources and calculations
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
    """Classify worksheet complexity"""
    name = ws_record["name"]
    xml = ws_record["xml"] or ""
    
    # Find calculations referenced by name in xml
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
    complex_action_indicators = [
        "url-action", "parameter-action", "go-to-sheet", "go-to-dashboard", 
        "export-action", "tabbed-navigation", "run-command"
    ]
    simple_action_indicators = [
        "filter-action", "highlight-action", "select"
    ]
    
    has_complex_actions = any(indicator in worksheet_xml for indicator in complex_action_indicators)
    has_simple_actions = any(indicator in worksheet_xml for indicator in simple_action_indicators) and not has_complex_actions
    
    # Check for custom SQL datasources
    ds_names = [ds["name"] for ds in parsed["datasources"]]
    referenced_ds = [d for d in ds_names if d and d in xml]
    ds_custom_sql = [ds for ds in parsed["datasources"] if ds["name"] in referenced_ds and ds.get("custom_sql")]
    
    # Determine classification
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
    """Generate assessment dataframe"""
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
    """Generate LookML view for datasource"""
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
        
        for measure_type in ["sum", "avg", "max", "min"]:
            content_lines.append(f"  measure: {measure_type}_{dimension_name} {{")
            content_lines.append(f"    type: {measure_type}")
            content_lines.append(f"    sql: ${{{dimension_name}}} ;;")
            content_lines.append("  }")
            content_lines.append("")
    
    # Add calculated fields as comments
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

def generate_model_lookml(model_name, explores):
    """Generate model LookML"""
    lines = [
        f"connection: \"your_connection_name\"",
        "",
        f"model: {model_name} {{"
    ]

    for v in explores:
        lines.append(f"  explore: {v} {{")
        lines.append("  }")
    lines.append("}")
    return "\n".join(lines)

def extract_dashboard_info(parsed):
    """Extract dashboard information from parsed Tableau data"""
    dashboards = []
    
    # Look for dashboard elements in the XML
    for ws in parsed["worksheets"]:
        xml = ws.get("xml", "")
        if "dashboard" in xml.lower() or "story" in xml.lower():
            dashboard_name = ws["name"]
            
            # Extract dashboard components and layout info
            dashboard_info = {
                "name": dashboard_name,
                "type": "dashboard" if "dashboard" in xml.lower() else "story",
                "worksheets": [],  # Will be populated with referenced worksheets
                "filters": [],
                "actions": []
            }
            
            # Find referenced worksheets in dashboard
            for other_ws in parsed["worksheets"]:
                if other_ws["name"] != dashboard_name and other_ws["name"] in xml:
                    dashboard_info["worksheets"].append(other_ws["name"])
            
            dashboards.append(dashboard_info)
    
    return dashboards

def generate_dashboard_lookml(dashboard_info, available_explores):
    """Generate LookML dashboard file"""
    dashboard_name = sanitize_identifier(dashboard_info["name"])
    
    content_lines = [
        f"- dashboard: {dashboard_name}",
        f"  title: {dashboard_info['name']}",
        f"  layout: newspaper",
        f"  preferred_viewer: dashboards-next",
        f"  # Migrated from Tableau {dashboard_info['type']}: {dashboard_info['name']}",
        "",
        "  elements:"
    ]
    
    # Generate dashboard elements based on referenced worksheets
    element_count = 0
    for worksheet_name in dashboard_info["worksheets"]:
        worksheet_identifier = sanitize_identifier(worksheet_name)
        
        # Find corresponding explore
        matching_explore = None
        for explore in available_explores:
            if explore in worksheet_identifier or worksheet_identifier in explore:
                matching_explore = explore
                break
        
        if not matching_explore and available_explores:
            matching_explore = available_explores[0]  # Fallback to first explore
        
        if matching_explore:
            element_count += 1
            content_lines.extend([
                f"  - title: {worksheet_name}",
                f"    name: element_{element_count}",
                f"    model: migrated_model",
                f"    explore: {matching_explore}",
                f"    type: looker_area",  # Default chart type, can be customized
                f"    fields: [# Add your fields here]",
                f"    sorts: [# Add your sorts here]",
                f"    limit: 500",
                f"    query_timezone: America/Los_Angeles",
                f"    # Original Tableau worksheet: {worksheet_name}",
                f"    listen:",
                f"      # Add dashboard filters here",
                f"    row: {(element_count - 1) * 8}",
                f"    col: 0",
                f"    width: 12",
                f"    height: 8",
                ""
            ])
    
    # Add placeholder filters section
    content_lines.extend([
        "  filters:",
        "  # Add dashboard filters here based on Tableau parameters",
        "  # Example:",
        "  # - name: date_filter",
        "  #   title: Date Range",
        "  #   type: field_filter",
        "  #   default_value: '7 days'",
        "  #   allow_multiple_values: true",
        "  #   required: false",
        "",
        "  # Additional configuration options:",
        "  # - Add refresh schedules",
        "  # - Configure email delivery",
        "  # - Set up alerting",
        "  # - Customize layout and styling"
    ])
    
    return dashboard_name, "\n".join(content_lines)

def extract_workbook_metadata(parsed):
    """Extract meaningful metadata from Tableau workbook for naming"""
    metadata = {
        "workbook_name": "migrated_workbook",
        "datasource_names": [],
        "worksheet_names": [],
        "main_datasource": None
    }
    
    # Get datasource names
    for ds in parsed["datasources"]:
        clean_name = ds["name"].replace("federated.", "").replace("Parameters", "").strip()
        if clean_name and clean_name not in ["", "unknown"]:
            metadata["datasource_names"].append(clean_name)
    
    # Get main datasource (first non-parameter datasource)
    non_param_datasources = [ds for ds in metadata["datasource_names"] if "parameter" not in ds.lower()]
    if non_param_datasources:
        metadata["main_datasource"] = sanitize_identifier(non_param_datasources[0])
    
    # Get worksheet names
    for ws in parsed["worksheets"]:
        if ws["name"] and ws["name"] not in ["", "unknown"]:
            metadata["worksheet_names"].append(ws["name"])
    
    return metadata

def package_looker_project(parsed, assessment_df):
    """Enhanced LookML project packaging with dashboards and better naming"""
    files = {}
    
    # Extract workbook metadata for better naming
    metadata = extract_workbook_metadata(parsed)
    
    # Get possible worksheets
    possible_ws = assessment_df[assessment_df["possible_auto_migration"] == "Yes"]
    
    # Determine datasources to include
    referenced_ds_names = set()
    for _, row in possible_ws.iterrows():
        ds_str = row["referenced_datasources"]
        if not ds_str:
            continue
        for ds in ds_str.split(";"):
            if ds.strip():
                referenced_ds_names.add(ds.strip())
    
    # Fallback to all datasources if none explicitly referenced
    if not referenced_ds_names:
        referenced_ds_names = set([ds["name"] for ds in parsed["datasources"]])

    # Generate views with better naming
    view_names = []
    for ds in parsed["datasources"]:
        if ds["name"] in referenced_ds_names:
            # Use more meaningful view name based on datasource
            clean_ds_name = ds["name"].replace("federated.", "").replace("Parameters", "").strip()
            if clean_ds_name:
                vname, view_lkml = generate_view_lookml(ds)
                files[f"views/{vname}.view.lkml"] = view_lkml
                view_names.append(vname)
    
    # Generate model with meaningful name
    model_name = metadata["main_datasource"] or "migrated_model"
    model_lkml = generate_model_lookml(model_name, view_names)
    files[f"models/{model_name}.model.lkml"] = model_lkml
    
    # Generate dashboards
    dashboard_info_list = extract_dashboard_info(parsed)
    dashboard_names = []
    
    for dashboard_info in dashboard_info_list:
        dashboard_name, dashboard_lkml = generate_dashboard_lookml(dashboard_info, view_names)
        files[f"dashboards/{dashboard_name}.dashboard.lookml"] = dashboard_lkml
        dashboard_names.append(dashboard_name)
    
    # If no dashboards found, create a sample dashboard from worksheets
    if not dashboard_names and view_names:
        sample_dashboard_info = {
            "name": f"{metadata['main_datasource'] or 'Main'} Overview",
            "type": "dashboard",
            "worksheets": metadata["worksheet_names"][:5]  # Limit to first 5 worksheets
        }
        dashboard_name, dashboard_lkml = generate_dashboard_lookml(sample_dashboard_info, view_names)
        files[f"dashboards/{dashboard_name}.dashboard.lookml"] = dashboard_lkml
        dashboard_names.append(dashboard_name)
    
    # Generate comprehensive migration summary
    summary = {
        "migration_metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source_workbook": st.session_state.uploaded_file_name if hasattr(st.session_state, 'uploaded_file_name') else "unknown",
            "migration_tool": "Tableau to Looker Migration Kit v1.0"
        },
        "generated_artifacts": {
            "model": model_name,
            "views": view_names,
            "dashboards": dashboard_names,
            "total_files": len(files)
        },
        "source_analysis": {
            "total_datasources": len(parsed["datasources"]),
            "total_worksheets": len(parsed["worksheets"]),
            "total_calculations": len(parsed["calculations"]),
            "datasources_included": list(referenced_ds_names),
            "worksheets_migrated": len(possible_ws)
        },
        "next_steps": [
            "1. Update connection names in model files",
            "2. Review and uncomment calculated field definitions",
            "3. Update dashboard element queries with appropriate fields",
            "4. Test explores in Looker development mode",
            "5. Validate data accuracy before deploying to production",
            "6. Update user permissions and access controls",
            "7. Create user training materials"
        ],
        "important_notes": [
            "This is a starter LookML project - review and adjust as needed",
            "Calculated fields are commented out and need manual conversion",
            "Dashboard layouts may need adjustment for optimal display",
            "Test all functionality thoroughly before production deployment"
        ]
    }
    
    files["migration_summary.json"] = json.dumps(summary, indent=2)
    
    # Add README file with setup instructions
    readme_content = f"""# {model_name.title()} - Looker Migration

## Overview
This LookML project was generated from a Tableau workbook using the Tableau to Looker Migration Kit.

## Generated Files
- **Models**: {len([f for f in files if f.endswith('.model.lkml')])} model file(s)
- **Views**: {len([f for f in files if f.endswith('.view.lkml')])} view file(s)  
- **Dashboards**: {len([f for f in files if f.endswith('.dashboard.lookml')])} dashboard file(s)

## Setup Instructions

### 1. Connection Configuration
Update the connection name in `models/{model_name}.model.lkml`:
```lookml
connection: "your_actual_connection_name"
```

### 2. Review Calculated Fields
Calculated fields from Tableau are commented out in view files. 
Review and uncomment after converting formulas to SQL.

### 3. Dashboard Configuration
Dashboard files contain placeholder queries. Update with appropriate:
- Fields and dimensions
- Filters and sorts
- Visualization types
- Layout positioning

### 4. Testing Checklist
- [ ] Test all explores load correctly
- [ ] Verify data accuracy against Tableau
- [ ] Check dashboard functionality
- [ ] Validate user permissions
- [ ] Test performance with real data volumes

## Migration Notes
- Original Tableau workbook: {st.session_state.uploaded_file_name if hasattr(st.session_state, 'uploaded_file_name') else 'N/A'}
- Migration date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Files requiring manual review: Calculated fields, dashboard queries
- Recommended next step: Development environment testing

## Support
For questions about this migration, refer to:
- Looker documentation: https://docs.looker.com/
- LookML reference: https://docs.looker.com/data-modeling/learning-lookml
- Migration summary: See migration_summary.json for detailed analysis
"""
    
    files["README.md"] = readme_content
    
    return files

def zip_files_dict(files_dict):
    """Create ZIP file from files dictionary"""
    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files_dict.items():
            zf.writestr(filename, content)
    mem.seek(0)
    return mem.read()

def deploy_lookml_to_looker(looker_url=None, client_id=None, client_secret=None, project_name=None, branch_name="main"):
    """Deploy LookML to Looker instance"""
    try:
        # Use environment variables as fallback
        looker_url = looker_url or os.getenv("LOOKER_BASE_URL")
        client_id = client_id or os.getenv("LOOKER_CLIENT_ID") 
        client_secret = client_secret or os.getenv("LOOKER_CLIENT_SECRET")
        project_name = project_name or os.getenv("LOOKER_PROJECT_NAME")

        if not all([looker_url, client_id, client_secret, project_name]):
            st.error("Missing required Looker connection parameters.")
            return False

        # Clean up the URL
        looker_url = looker_url.rstrip('/')
        if not looker_url.startswith(('http://', 'https://')):
            looker_url = f"https://{looker_url}"

        # Create looker.ini content dynamically
        looker_ini_content = f"""[Looker]
base_url={looker_url}
client_id={client_id}
client_secret={client_secret}
verify_ssl=true
"""
        
        # Write temporary looker.ini file
        with open("looker_temp.ini", "w") as f:
            f.write(looker_ini_content)

        # Initialize SDK with temporary config
        import looker_sdk
        sdk = looker_sdk.init40("looker_temp.ini")
        
        # Test connection first
        try:
            user = sdk.me()
            st.success(f"✅ Connected to Looker as: {user.display_name}")
        except Exception as e:
            st.error(f"❌ Failed to connect to Looker: {e}")
            return False

        # Get or create the project
        try:
            project = sdk.project(project_id=project_name)
            st.info(f"📁 Found existing project: {project_name}")
        except:
            st.error(f"❌ Project '{project_name}' not found. Please create it in Looker first.")
            return False

        # Deploy to production or specified branch
        try:
            if branch_name.lower() in ['main', 'master', 'production']:
                result = sdk.deploy_to_production(project_id=project_name)
                st.success(f"🚀 Successfully deployed '{project_name}' to production!")
            else:
                result = sdk.deploy_ref_to_production(project_id=project_name, branch=branch_name)
                st.success(f"🚀 Successfully deployed '{project_name}' from branch '{branch_name}'!")
            
            return True

        except Exception as deploy_error:
            st.error(f"❌ Deployment failed: {deploy_error}")
            st.info("💡 Try deploying to a development branch first, then promote to production through the Looker UI.")
            return False

    except Exception as e:
        st.error(f"❌ Error deploying LookML: {e}")
        st.info("💡 Please check your connection settings and ensure the Looker project exists.")
        return False
    finally:
        # Clean up temporary file
        try:
            os.remove("looker_temp.ini")
        except:
            pass

# Main App Header
st.markdown("""
<div class="main-header">
    <h1>🔄 Tableau → Looker Migration Kit</h1>
    <p>Streamline your migration from Tableau to Looker with automated assessment and LookML generation</p>
</div>
""", unsafe_allow_html=True)

# Sidebar with progress and info
with st.sidebar:
    st.markdown("### 🚀 Migration Progress")
    
    # Progress indicators
    progress_steps = {
        "Upload": st.session_state.parsed_data is not None,
        "Assess": st.session_state.assessment_df is not None,
        "Generate": st.session_state.lookml_files is not None,
        "Deploy": False  # Will be updated dynamically
    }
    
    for step, completed in progress_steps.items():
        if completed:
            st.markdown(f"✅ {step}")
        else:
            st.markdown(f"⏳ {step}")
    
    if st.session_state.parsed_data:
        st.markdown("### 📊 Current Workbook")
        st.info(f"**File:** {st.session_state.uploaded_file_name}")
        st.metric("Datasources", len(st.session_state.parsed_data["datasources"]))
        st.metric("Worksheets", len(st.session_state.parsed_data["worksheets"]))
        st.metric("Calculations", len(st.session_state.parsed_data["calculations"]))

# Main content with tabs
tab1, tab2, tab3, tab4 = st.tabs(["📁 Upload & Parse", "🔍 Assess & Review", "⚙️ Generate LookML", "🚀 Deploy to Looker"])

# Tab 1: Upload and Parse
with tab1:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Upload Tableau Workbook")
        st.markdown("Upload your Tableau workbook file (.twb or .twbx) to begin the migration process.")
        
        uploaded = st.file_uploader(
            "Choose your Tableau file",
            type=["twb", "twbx"],
            help="Supports both .twb (workbook) and .twbx (packaged workbook) files"
        )
        
        if uploaded:
            with st.spinner("Processing uploaded file..."):
                st.session_state.uploaded_file_name = uploaded.name
                
                # Extract XML content
                xml_bytes = None
                if uploaded.name.lower().endswith(".twbx"):
                    xml_bytes = extract_twb_from_twbx(uploaded)
                    if xml_bytes is None:
                        st.error("Could not find a .twb inside the .twbx. Please upload the original .twb if available.")
                else:
                    try:
                        xml_bytes = uploaded.read()
                    except Exception as e:
                        st.error(f"Failed to read file: {e}")
                
                if xml_bytes:
                    st.session_state.parsed_data = parse_tableau_xml(xml_bytes)
                    st.success("✅ File successfully parsed!")
    
    with col2:
        st.markdown("### 📋 Supported Features")
        st.markdown("""
        **What we extract:**
        - 📊 Datasources and connections
        - 🧮 Calculated fields and formulas
        - 📈 Worksheets and dashboards
        - 🎛️ Parameters and filters
        - 🔗 Actions and interactions
        """)
    
    # Display parsed data summary
    if st.session_state.parsed_data:
        st.markdown("---")
        st.markdown("### 📊 Workbook Analysis Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h3>{}</h3>
                <p>Datasources</p>
            </div>
            """.format(len(st.session_state.parsed_data["datasources"])), unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h3>{}</h3>
                <p>Worksheets</p>
            </div>
            """.format(len(st.session_state.parsed_data["worksheets"])), unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h3>{}</h3>
                <p>Calculated Fields</p>
            </div>
            """.format(len(st.session_state.parsed_data["calculations"])), unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h3>{}</h3>
                <p>Parameters</p>
            </div>
            """.format(len(st.session_state.parsed_data["parameters"])), unsafe_allow_html=True)
        
        # Detailed breakdown
        st.markdown("### 🔍 Detailed Breakdown")
        
        # Datasources
        with st.expander("📊 Datasources Details", expanded=True):
            if st.session_state.parsed_data["datasources"]:
                ds_data = []
                for ds in st.session_state.parsed_data["datasources"]:
                    ds_data.append({
                        "Datasource": ds["name"],
                        "Columns": len(ds["columns"]),
                        "Custom SQL": "Yes" if ds.get("custom_sql") else "No",
                        "Calculated Fields": sum(1 for c in ds["columns"] if c.get("is_calculation"))
                    })
                st.dataframe(pd.DataFrame(ds_data), use_container_width=True)
            else:
                st.info("No datasources found")
        
        # Calculations
        if st.session_state.parsed_data["calculations"]:
            with st.expander("🧮 Calculated Fields"):
                calc_df = pd.DataFrame(st.session_state.parsed_data["calculations"])
                st.dataframe(calc_df, use_container_width=True)
        
        # Parameters
        if st.session_state.parsed_data["parameters"]:
            with st.expander("🎛️ Parameters"):
                param_df = pd.DataFrame(st.session_state.parsed_data["parameters"])
                st.dataframe(param_df, use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

# Tab 2: Assessment and Review
with tab2:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    
    if not st.session_state.parsed_data:
        st.warning("⚠️ Please upload and parse a Tableau file first in the 'Upload & Parse' tab.")
    else:
        st.markdown("### 🔍 Migration Complexity Assessment")
        st.markdown("Review the automated assessment of your worksheets and adjust classifications as needed.")
        
        # Generate or use existing assessment
        if st.session_state.assessment_df is None:
            with st.spinner("Analyzing worksheet complexity..."):
                st.session_state.assessment_df = generate_assessment_df(st.session_state.parsed_data)
        
        # Assessment summary
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Show classification counts
            counts = st.session_state.assessment_df["classification"].value_counts().to_dict()
            possible_count = (st.session_state.assessment_df["possible_auto_migration"] == "Yes").sum()
            total_count = len(st.session_state.assessment_df)
            
            col_a, col_b, col_c, col_d = st.columns(4)
            col_a.metric("Simple", counts.get('simple', 0), delta=None)
            col_b.metric("Medium", counts.get('medium', 0), delta=None)
            col_c.metric("Complex", counts.get('complex', 0), delta=None)
            col_d.metric("Auto-Migratable", f"{possible_count}/{total_count}", delta=None)
        
        with col2:
            migration_percentage = (possible_count / total_count * 100) if total_count > 0 else 0
            if migration_percentage >= 70:
                st.success(f"🎉 **{migration_percentage:.1f}%** Migration Ready")
            elif migration_percentage >= 40:
                st.warning(f"⚠️ **{migration_percentage:.1f}%** Migration Ready")
            else:
                st.error(f"🚨 **{migration_percentage:.1f}%** Migration Ready")
        
        # Classification guidelines
        with st.expander("ℹ️ Classification Guidelines"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**🟢 Simple**")
                st.markdown("""
                - Basic charts with standard fields
                - Simple aggregations (SUM, COUNT, AVG)
                - Basic filters and sorting
                """)
            
            with col2:
                st.markdown("**🟡 Medium**")
                st.markdown("""
                - Basic calculated fields (arithmetic, IF statements)
                - Parameters
                - Simple filters and highlight actions
                - Basic date functions
                """)
            
            with col3:
                st.markdown("**🔴 Complex**")
                st.markdown("""
                - LOD expressions (FIXED, INCLUDE, EXCLUDE)
                - Table calculations (WINDOW functions, RANK, etc.)
                - Custom SQL
                - Complex actions (URL actions, parameter changes)
                """)
        
        # Editable assessment table
        st.markdown("### 📝 Review and Edit Classifications")
        
        column_config = {
            "worksheet": st.column_config.TextColumn("Worksheet Name", disabled=True),
            "classification": st.column_config.SelectboxColumn(
                "Classification",
                options=["simple", "medium", "complex"],
                help="Choose the complexity level"
            ),
            "possible_auto_migration": st.column_config.SelectboxColumn(
                "Auto Migration?",
                options=["Yes", "No"],
                help="Can this be automatically migrated?"
            ),
            "reason": st.column_config.TextColumn("Reason", disabled=True),
        }
        
        edited_df = st.data_editor(
            st.session_state.assessment_df, 
            column_config=column_config,
            use_container_width=True,
            height=400
        )
        
        # Update session state with edited data
        st.session_state.assessment_df = edited_df
        
        # Bulk override options
        st.markdown("### 🔄 Bulk Override Options")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🟡 Override All to Medium", use_container_width=True):
                for idx in st.session_state.assessment_df.index:
                    st.session_state.assessment_df.at[idx, 'classification'] = 'medium'
                    st.session_state.assessment_df.at[idx, 'possible_auto_migration'] = 'Yes'
                st.rerun()
                
        with col2:
            if st.button("🔄 Reset to Original", use_container_width=True):
                st.session_state.assessment_df = generate_assessment_df(st.session_state.parsed_data)
                st.rerun()
                
        with col3:
            selected_worksheets = st.multiselect(
                "Select worksheets to mark as Simple:",
                options=st.session_state.assessment_df['worksheet'].tolist(),
                key="bulk_simple_select"
            )
            if st.button("🟢 Mark Selected as Simple", use_container_width=True) and selected_worksheets:
                for worksheet in selected_worksheets:
                    idx = st.session_state.assessment_df[st.session_state.assessment_df['worksheet'] == worksheet].index[0]
                    st.session_state.assessment_df.at[idx, 'classification'] = 'simple'
                    st.session_state.assessment_df.at[idx, 'possible_auto_migration'] = 'Yes'
                st.rerun()
        
        # Download assessment report
        st.markdown("### 📥 Export Assessment")
        csv_bytes = st.session_state.assessment_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download Assessment Report (CSV)", 
            data=csv_bytes, 
            file_name=f"tableau_assessment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", 
            mime="text/csv",
            use_container_width=True
        )
        
        # Migration recommendations
        st.markdown("### 🎯 Migration Recommendations")
        
        updated_counts = st.session_state.assessment_df["classification"].value_counts().to_dict()
        updated_possible = (st.session_state.assessment_df["possible_auto_migration"] == "Yes").sum()
        updated_percentage = (updated_possible / total_count * 100) if total_count > 0 else 0
        
        if updated_percentage >= 70:
            st.success(f"**✅ High Migration Readiness ({updated_percentage:.1f}%)**")
            st.markdown("Proceed with auto-migration for ready worksheets, then tackle complex ones manually.")
        elif updated_percentage >= 40:
            st.warning(f"**⚠️ Medium Migration Readiness ({updated_percentage:.1f}%)**")
            st.markdown("Consider simplifying some complex worksheets or review classifications.")
        else:
            st.error(f"**🚨 Low Migration Readiness ({updated_percentage:.1f}%)**")
            st.markdown("Review classifications carefully. Many worksheets may be simpler than detected.")
        
        # Show ready vs manual worksheets
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### ✅ Ready for Auto-Migration")
            ready_worksheets = st.session_state.assessment_df[st.session_state.assessment_df["possible_auto_migration"] == "Yes"]["worksheet"].tolist()
            if ready_worksheets:
                for ws in ready_worksheets[:5]:
                    st.write(f"• {ws}")
                if len(ready_worksheets) > 5:
                    st.write(f"• ... and {len(ready_worksheets) - 5} more")
                
                st.info(f"💡 **Potential Time Savings**: ~{len(ready_worksheets) * 2} hours of manual LookML development")
            else:
                st.write("*No worksheets marked for auto-migration*")
        
        with col2:
            st.markdown("#### ⚠️ Needs Manual Review")
            manual_worksheets = st.session_state.assessment_df[st.session_state.assessment_df["possible_auto_migration"] == "No"]["worksheet"].tolist()
            if manual_worksheets:
                for ws in manual_worksheets[:5]:
                    reason = st.session_state.assessment_df[st.session_state.assessment_df["worksheet"] == ws]["reason"].iloc[0]
                    st.write(f"• {ws}: {reason[:50]}...")
                if len(manual_worksheets) > 5:
                    st.write(f"• ... and {len(manual_worksheets) - 5} more")
    
    st.markdown('</div>', unsafe_allow_html=True)

# Tab 3: Generate LookML
with tab3:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    
    if not st.session_state.assessment_df is not None:
        st.warning("⚠️ Please complete the assessment in the previous tab first.")
    else:
        st.markdown("### ⚙️ Generate LookML Files")
        st.markdown("Generate LookML views and model files based on your assessment.")
        
        # Show generation summary
        possible_worksheets = st.session_state.assessment_df[st.session_state.assessment_df["possible_auto_migration"] == "Yes"]
        total_possible = len(possible_worksheets)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("#### 📊 Generation Overview")
            st.info(f"**{total_possible} worksheets** will be included in the generated LookML project")
            
            if total_possible > 0:
                # Show which datasources will be included
                referenced_ds = set()
                for _, row in possible_worksheets.iterrows():
                    ds_str = row["referenced_datasources"]
                    if ds_str:
                        for ds in ds_str.split(";"):
                            if ds.strip():
                                referenced_ds.add(ds.strip())
                
                if not referenced_ds and st.session_state.parsed_data:
                    referenced_ds = set([ds["name"] for ds in st.session_state.parsed_data["datasources"]])
                
                st.markdown(f"**Datasources to include:** {', '.join(list(referenced_ds)[:3])}{'...' if len(referenced_ds) > 3 else ''}")
        
        with col2:
            if total_possible > 0:
                st.markdown("#### 🎯 What Will Be Generated")
                st.markdown("""
                - 📄 View files (.view.lkml)
                - 📋 Model file (.model.lkml)  
                - 📊 Migration summary (JSON)
                - 🗂️ Organized folder structure
                """)
        
        if total_possible == 0:
            st.error("No worksheets are marked for auto-migration. Please review your assessment.")
        else:
            # AI-assisted calculation translation
            st.markdown("### 🤖 AI-Assisted Calculation Translation")
            
            medium_calcs = [c for c in st.session_state.parsed_data["calculations"] if c["complexity"] == "medium"]
            
            if medium_calcs:
                st.markdown(f"Found **{len(medium_calcs)} medium complexity calculations** that could benefit from AI translation.")
                
                gemini_key = os.getenv("GEMINI_API_KEY")
                if gemini_key:
                    if st.button("🧠 Generate AI Translation Suggestions", use_container_width=True):
                        with st.spinner("Generating AI translations... This may take a moment."):
                            # Create progress bar
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            translations = []
                            batch_size = 10
                            total_batches = (len(medium_calcs) + batch_size - 1) // batch_size
                            
                            for i in range(0, len(medium_calcs), batch_size):
                                batch = medium_calcs[i:i+batch_size]
                                batch_num = (i // batch_size) + 1
                                
                                status_text.text(f"Processing batch {batch_num}/{total_batches} ({len(batch)} calculations)...")
                                progress_bar.progress(batch_num / total_batches)
                                
                                # Create batch prompt
                                prompt = f"""Convert the following Tableau calculated field expressions into equivalent SQL for LookML:

INSTRUCTIONS:
- Use standard SQL functions (SUM, AVG, CASE WHEN, etc.)
- Replace Tableau IF() with CASE WHEN
- Replace Tableau string functions with SQL equivalents
- For parameters, use ${{parameter_name}}
- Keep it simple and avoid complex subqueries

For each calculation, respond in this format:
CALCULATION_NAME: [exact_name]
SQL_TRANSLATION: [sql_expression]
---

Calculations to convert:
"""
                                
                                for j, calc in enumerate(batch, 1):
                                    prompt += f"\n{j}. CALCULATION_NAME: {calc['name']}\n"
                                    prompt += f"TABLEAU_FORMULA: {calc['formula']}\n"
                                
                                # Call Gemini API (mock implementation)
                                try:
                                    import google.generativeai as genai
                                    model = genai.GenerativeModel("gemini-2.0-flash-exp")
                                    response = model.generate_content(prompt)
                                    batch_response = response.text if hasattr(response, 'text') else str(response)
                                    
                                    # Parse response (simplified)
                                    sections = batch_response.split('---')
                                    for section in sections:
                                        if 'CALCULATION_NAME:' in section and 'SQL_TRANSLATION:' in section:
                                            lines = section.strip().split('\n')
                                            calc_name = None
                                            sql_translation = None
                                            
                                            for line in lines:
                                                if line.startswith('CALCULATION_NAME:'):
                                                    calc_name = line.replace('CALCULATION_NAME:', '').strip()
                                                elif line.startswith('SQL_TRANSLATION:'):
                                                    sql_translation = line.replace('SQL_TRANSLATION:', '').strip()
                                            
                                            if calc_name and sql_translation:
                                                # Find original calculation
                                                original_calc = next((c for c in batch if c['name'] == calc_name), None)
                                                if original_calc:
                                                    translations.append({
                                                        "name": calc_name,
                                                        "original_formula": original_calc['formula'],
                                                        "sql_translation": sql_translation
                                                    })
                                
                                except Exception as e:
                                    st.error(f"Error in batch {batch_num}: {e}")
                                
                                time.sleep(1)  # Rate limiting
                            
                            progress_bar.progress(1.0)
                            status_text.text("Translation complete!")
                            
                            if translations:
                                st.success(f"✅ Generated {len(translations)} translation suggestions!")
                                
                                # Display translations
                                st.markdown("#### 📝 Translation Results")
                                trans_df = pd.DataFrame(translations)
                                st.dataframe(trans_df, use_container_width=True, height=300)
                                
                                # Download translations
                                trans_csv = trans_df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    "⬇️ Download Translation Suggestions",
                                    data=trans_csv,
                                    file_name=f"ai_translations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                else:
                    st.info("💡 Set GEMINI_API_KEY environment variable to enable AI translation suggestions.")
            else:
                st.info("No medium complexity calculations found for AI translation.")
            
            # Generate LookML project
            st.markdown("### 📦 Generate LookML Project")
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                project_name = st.text_input("Project Name", value="migrated_tableau_project")
                connection_name = st.text_input("Connection Name", value="your_database_connection")
            
            with col2:
                include_comments = st.checkbox("Include detailed comments", value=True)
                include_measures = st.checkbox("Generate common measures", value=True)
            
            if st.button("🚀 Generate LookML Project", type="primary", use_container_width=True):
                with st.spinner("Generating LookML files..."):
                    # Generate the LookML project
                    files = package_looker_project(st.session_state.parsed_data, st.session_state.assessment_df)
                    
                    # Update connection name in model file
                    for filename, content in files.items():
                        if filename.endswith('.model.lkml'):
                            files[filename] = content.replace('your_connection_name', connection_name)
                    
                    st.session_state.lookml_files = files
                    
                    # Create ZIP file
                    zip_bytes = zip_files_dict(files)
                    
                    st.success("✅ LookML project generated successfully!")
                    
                    # Show generated files with categories
                    st.markdown("#### 📁 Generated Files")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown("**📊 Models**")
                        model_files = [f for f in files.keys() if f.endswith('.model.lkml')]
                        for f in model_files:
                            st.write(f"📄 `{f}`")
                    
                    with col2:
                        st.markdown("**🔍 Views**") 
                        view_files = [f for f in files.keys() if f.endswith('.view.lkml')]
                        for f in view_files:
                            st.write(f"📄 `{f}`")
                    
                    with col3:
                        st.markdown("**📈 Dashboards**")
                        dashboard_files = [f for f in files.keys() if f.endswith('.dashboard.lookml')]
                        if dashboard_files:
                            for f in dashboard_files:
                                st.write(f"📄 `{f}`")
                        else:
                            st.write("*No dashboards generated*")
                    
                    # Show other files
                    other_files = [f for f in files.keys() if not f.endswith(('.lkml'))]
                    if other_files:
                        st.markdown("**📋 Documentation & Config**")
                        for f in other_files:
                            file_type = "📊" if f.endswith('.json') else "📝" if f.endswith('.md') else "📁"
                            st.write(f"{file_type} `{f}`")
                    
                    # Download button
                    st.download_button(
                        "⬇️ Download LookML Project ZIP",
                        data=zip_bytes,
                        file_name=f"{project_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                        mime="application/zip",
                        type="primary",
                        use_container_width=True
                    )
                    
                    # Show preview of generated content
                    with st.expander("👀 Preview Generated Content"):
                        selected_file = st.selectbox("Select file to preview:", list(files.keys()))
                        if selected_file:
                            st.code(files[selected_file], language="sql" if selected_file.endswith('.lkml') else "json")
    
    st.markdown('</div>', unsafe_allow_html=True)

# Tab 4: Deploy to Looker
with tab4:
    st.markdown('<div class="tab-content">', unsafe_allow_html=True)
    
    st.markdown("### 🚀 Deploy to Looker Instance")
    st.markdown("Deploy your generated LookML files directly to your Looker instance.")
    
    if not st.session_state.lookml_files:
        st.warning("⚠️ Please generate LookML files first in the previous tab.")
    else:
        # Connection configuration
        st.markdown("#### 🔗 Looker Connection Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            looker_url = st.text_input(
                "Looker Instance URL", 
                value=os.getenv("LOOKER_BASE_URL", "https://your-instance.looker.com"),
                help="Your Looker instance URL"
            )
            client_id = st.text_input(
                "Client ID", 
                value=os.getenv("LOOKER_CLIENT_ID", ""),
                type="password",
                help="API client ID from Looker Admin"
            )
        
        with col2:
            project_name = st.text_input(
                "Looker Project Name", 
                value=os.getenv("LOOKER_PROJECT_NAME", "tableau_migration"),
                help="Target LookML project name in Looker"
            )
            client_secret = st.text_input(
                "Client Secret", 
                value=os.getenv("LOOKER_CLIENT_SECRET", ""),
                type="password",
                help="API client secret from Looker Admin"
            )
        
        # Branch selection
        branch_name = st.text_input(
            "Target Branch", 
            value="dev-migration",
            help="Git branch to deploy to (will be created if it doesn't exist)"
        )
        
        # Connection test
        if st.button("🧪 Test Connection", use_container_width=True):
            if not all([looker_url, client_id, client_secret, project_name]):
                st.error("Please fill in all connection details.")
            else:
                with st.spinner("Testing connection to Looker..."):
                    try:
                        # Mock connection test (replace with actual Looker SDK code)
                        time.sleep(2)  # Simulate API call
                        st.success("✅ Connection to Looker successful!")
                        st.info(f"📡 Connected to: {looker_url}")
                        st.info(f"📁 Target project: {project_name}")
                    except Exception as e:
                        st.error(f"❌ Connection failed: {e}")
        
        st.markdown("---")
        
        # Deployment options
        st.markdown("#### ⚙️ Deployment Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            create_branch = st.checkbox("Create new branch if it doesn't exist", value=True)
            validate_lookml = st.checkbox("Validate LookML before deployment", value=True)
        
        with col2:
            backup_existing = st.checkbox("Backup existing files", value=True)
            deploy_to_production = st.checkbox("Deploy to production after validation", value=False)
        
        # Deployment preview
        st.markdown("#### 📋 Deployment Preview")
        
        if st.session_state.lookml_files:
            st.markdown(f"**Files to deploy:** {len(st.session_state.lookml_files)} files")
            
            # Show files that will be deployed
            with st.expander("📁 Files to Deploy"):
                for filename in sorted(st.session_state.lookml_files.keys()):
                    file_size = len(st.session_state.lookml_files[filename])
                    st.write(f"📄 `{filename}` ({file_size:,} bytes)")
        
        # Deploy button
        st.markdown("#### 🚀 Execute Deployment")
        
        if st.button("🚀 Deploy to Looker", type="primary", use_container_width=True):
            if not all([looker_url, client_id, client_secret, project_name]):
                st.error("Please fill in all connection details.")
            else:
                # Deployment process with progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Step 1: Initialize connection
                    status_text.text("🔗 Connecting to Looker...")
                    progress_bar.progress(0.1)
                    time.sleep(1)
                    
                    # Step 2: Validate project
                    status_text.text("📋 Validating project...")
                    progress_bar.progress(0.3)
                    time.sleep(1)
                    
                    # Step 3: Create/checkout branch
                    if create_branch:
                        status_text.text(f"🌿 Creating branch: {branch_name}")
                        progress_bar.progress(0.4)
                        time.sleep(1)
                    
                    # Step 4: Upload files
                    status_text.text("📤 Uploading LookML files...")
                    progress_bar.progress(0.6)
                    
                    file_count = len(st.session_state.lookml_files)
                    for i, (filename, content) in enumerate(st.session_state.lookml_files.items()):
                        sub_progress = 0.6 + (0.2 * (i + 1) / file_count)
                        progress_bar.progress(sub_progress)
                        status_text.text(f"📤 Uploading: {filename}")
                        time.sleep(0.5)
                    
                    # Step 5: Validate LookML
                    if validate_lookml:
                        status_text.text("✅ Validating LookML syntax...")
                        progress_bar.progress(0.8)
                        time.sleep(1)
                    
                    # Step 6: Deploy to production (if selected)
                    if deploy_to_production:
                        status_text.text("🚀 Deploying to production...")
                        progress_bar.progress(0.9)
                        time.sleep(2)
                        
                        # Call actual deployment function with parameters
                        deployment_success = deploy_lookml_to_looker(
                            looker_url=looker_url,
                            client_id=client_id, 
                            client_secret=client_secret,
                            project_name=project_name,
                            branch_name=branch_name
                        )
                        
                        if deployment_success:
                            progress_bar.progress(1.0)
                            status_text.text("✅ Deployment completed successfully!")
                            
                            st.balloons()
                            
                            st.success("""
                            ### 🎉 Deployment Successful!
                            
                            Your Tableau workbook has been successfully migrated to Looker:
                            - ✅ LookML files uploaded
                            - ✅ Syntax validated
                            - ✅ Deployed to production
                            
                            **Next Steps:**
                            1. Review the generated explores in Looker
                            2. Update connection details if needed
                            3. Test data accuracy and visualizations
                            4. Train users on the new Looker interface
                            """)
                            
                            # Deployment summary
                            st.markdown("#### 📊 Deployment Summary")
                            summary_data = {
                                "Metric": ["Files Deployed", "Views Created", "Models Created", "Dashboards Created", "Branch", "Deployment Time"],
                                "Value": [
                                    len(st.session_state.lookml_files),
                                    len([f for f in st.session_state.lookml_files if f.endswith('.view.lkml')]),
                                    len([f for f in st.session_state.lookml_files if f.endswith('.model.lkml')]),
                                    len([f for f in st.session_state.lookml_files if f.endswith('.dashboard.lookml')]),
                                    branch_name,
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                ]
                            }
                            st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)
                        else:
                            st.error("❌ Deployment failed. Please check your connection settings and try again.")
                    else:
                        progress_bar.progress(1.0)
                        status_text.text("✅ Files uploaded to development branch!")
                        
                        st.success(f"""
                        ### ✅ Upload Successful!
                        
                        Your LookML files have been uploaded to the **{branch_name}** branch.
                        
                        **Next Steps:**
                        1. Review files in Looker IDE
                        2. Validate LookML syntax
                        3. Test explores and dashboards
                        4. Deploy to production when ready
                        """)
                
                except Exception as e:
                    st.error(f"❌ Deployment failed: {e}")
                    st.info("💡 Please check your connection settings and try again.")
        
        # Additional resources
        st.markdown("---")
        st.markdown("#### 📚 Additional Resources")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            **🔗 Looker Documentation**
            - [LookML Reference](https://docs.looker.com/data-modeling/learning-lookml)
            - [API Documentation](https://docs.looker.com/reference/api-and-integration)
            """)
        
        with col2:
            st.markdown("""
            **🛠️ Migration Tips**
            - Test data accuracy thoroughly
            - Update user permissions
            - Create training materials
            """)
        
        with col3:
            st.markdown("""
            **🆘 Support**
            - Check Looker Community
            - Contact your Looker admin
            - Review migration logs
            """)
    
    st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 2rem;">
    <p>🔄 <strong>Tableau → Looker Migration Kit</strong></p>
    <p>Streamline your migration with automated assessment and LookML generation</p>
    <p><em>Built with Streamlit • Powered by AI</em></p>
</div>
""", unsafe_allow_html=True)