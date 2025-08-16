# app.py
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

# External libraries
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    st.warning("Google Generative AI not available. Install with: pip install google-generativeai")

try:
    from looker_sdk import init40
    LOOKER_SDK_AVAILABLE = True
except ImportError:
    LOOKER_SDK_AVAILABLE = False
    st.warning("Looker SDK not available. Install with: pip install looker-sdk")

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Gemini API if available
if GEMINI_AVAILABLE:
    try:
        api_key = os.getenv("GEMINI_API_KEY")
        if api_key:
            genai.configure(api_key=api_key)
    except Exception as e:
        st.warning(f"Could not configure Gemini API: {e}")

st.set_page_config(page_title="Tableau → Looker Migrator", layout="wide")
st.title("🔁 Tableau → Looker Migration Kit (Phase 1: Assessment → Phase 2: LookML)")


# Function to deploy LookML to Looker
def deploy_lookml_to_looker():
    """
    Deploys the LookML project to Looker using the API.
    """
    if not LOOKER_SDK_AVAILABLE:
        st.error("Looker SDK is not available. Please install it with: pip install looker-sdk")
        return False
        
    try:
        # Initialize the Looker SDK using environment variables
        sdk = init40("looker.ini")

        # Get the Looker project name from the environment file
        project_name = os.getenv("LOOKER_PROJECT_NAME")

        if not project_name:
            st.error("LOOKER_PROJECT_NAME not found in environment file.")
            return False

        st.info(f"Attempting to deploy project: {project_name}")

        # Try different deployment methods
        try:
            sdk.deploy_ref_to_production(project_id=project_name, branch="dev-sudharshan-seerapu-gbfx")
            st.success(f"Deployment of project '{project_name}' successful!")
            return True
        except Exception as e1:
            try:
                sdk.deploy_to_production(project_id=project_name)
                st.success(f"Deployment of project '{project_name}' successful!")
                return True
            except Exception as e2:
                st.error(f"Deployment failed with both methods:")
                st.error(f"Method 1: {e1}")
                st.error(f"Method 2: {e2}")
                return False

    except Exception as e:
        st.error(f"Error deploying LookML: {e}")
        return False

def call_gemini(prompt: str, model_name="gemini-2.0-flash-exp") -> str:
    """Call Gemini Pro API with error handling"""
    if not GEMINI_AVAILABLE:
        return "# Gemini API not available"
        
    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(prompt)
        return response.text if hasattr(response, 'text') else str(response)
    except Exception as e:
        return f"# Gemini error: {e}"

def parse_batch_response(response_text: str, original_calcs: list) -> list:
    """
    Parse the structured batch response from Gemini into individual translations.
    Expected format:
    CALCULATION_NAME: [name]
    SQL_TRANSLATION: [sql]
    ---
    """
    translations = []
    
    # Split response by --- separator
    sections = response_text.split('---')
    
    for section in sections:
        section = section.strip()
        if not section:
            continue
            
        lines = section.split('\n')
        calc_name = None
        sql_translation = None
        
        for line in lines:
            line = line.strip()
            if line.startswith('CALCULATION_NAME:'):
                calc_name_raw = line.replace('CALCULATION_NAME:', '').strip()
                # Remove brackets if present: [Base Salary] -> Base Salary
                calc_name = calc_name_raw.strip('[]')
            elif line.startswith('SQL_TRANSLATION:'):
                sql_translation = line.replace('SQL_TRANSLATION:', '').strip()
        
        # Find the matching original calculation - try multiple matching strategies
        original_formula = None
        matched_calc = None
        
        if calc_name:
            # Strategy 1: Exact match
            for calc in original_calcs:
                if calc.get('name') == calc_name:
                    matched_calc = calc
                    break
            
            # Strategy 2: Case-insensitive match
            if not matched_calc:
                for calc in original_calcs:
                    if calc.get('name', '').lower() == calc_name.lower():
                        matched_calc = calc
                        break
            
            # Strategy 3: Partial match (if calc_name is contained in original name)
            if not matched_calc:
                for calc in original_calcs:
                    calc_name_lower = calc_name.lower()
                    orig_name_lower = calc.get('name', '').lower()
                    if calc_name_lower in orig_name_lower or orig_name_lower in calc_name_lower:
                        matched_calc = calc
                        break
        
        if matched_calc:
            original_formula = matched_calc.get('formula', '')
            final_calc_name = matched_calc.get('name', '')
        elif calc_name:
            # Use the parsed name even if we couldn't match exactly
            final_calc_name = calc_name
            original_formula = f"Formula not found for: {calc_name}"
        else:
            continue
        
        if final_calc_name and sql_translation:
            translations.append({
                "name": final_calc_name,
                "formula": original_formula,
                "translation": sql_translation
            })
    
    # If we got fewer translations than expected, try alternative parsing
    if len(translations) < len(original_calcs) * 0.5:  # If we got less than 50% of expected translations
        return parse_batch_response_alternative(response_text, original_calcs)
    
    # If we still don't have translations for all calcs, add the missing ones
    translated_names = {t["name"] for t in translations}
    for calc in original_calcs:
        calc_name = calc.get("name", "")
        if calc_name and calc_name not in translated_names:
            translations.append({
                "name": calc_name,
                "formula": calc.get("formula", ""), 
                "translation": f"# Translation not found in response for: {calc_name}"
            })
    
    return translations

def parse_batch_response_alternative(response_text: str, original_calcs: list) -> list:
    """
    Alternative parsing method that's more flexible with the response format
    """
    translations = []
    
    # Try to extract name-value pairs more flexibly
    pattern = r'([A-Z\s_]+):\s*\[?([^\]]+)\]?\s*SQL_TRANSLATION:\s*([^\n]+)'
    matches = re.findall(pattern, response_text, re.IGNORECASE | re.MULTILINE)
    
    for match in matches:
        field_type = match[0].strip()
        calc_name = match[1].strip()
        sql_translation = match[2].strip()
        
        # Find matching original calculation
        matched_calc = None
        for calc in original_calcs:
            calc_name_orig = calc.get('name', '')
            if (calc_name_orig.lower() == calc_name.lower() or 
                calc_name.lower() in calc_name_orig.lower() or 
                calc_name_orig.lower() in calc_name.lower()):
                matched_calc = calc
                break
        
        if matched_calc:
            translations.append({
                "name": matched_calc.get("name", ""),
                "formula": matched_calc.get("formula", ""),
                "translation": sql_translation
            })
    
    # If still no good results, create error entries for all calcs
    if len(translations) == 0:
        for calc in original_calcs:
            translations.append({
                "name": calc.get("name", ""),
                "formula": calc.get("formula", ""), 
                "translation": f"# Parsing error - Raw response section: {response_text[:200]}..."
            })
    
    return translations

# -------------------------
# Helper utilities
# -------------------------
def extract_twb_from_twbx(uploaded_file):
    """
    Return bytes of the .twb inside a .twbx UploadedFile
    """
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
    name = str(name).strip()
    name = re.sub(r"[^\w]+", "_", name)  # non-alphanumeric -> underscore
    name = re.sub(r"__+", "_", name)
    name = name.strip("_").lower()
    if re.match(r"^\d", name):
        name = "_" + name
    return name or "field"

def detect_calc_complexity(formula: str) -> str:
    if not formula:
        return "unknown"
    f = str(formula).upper()
    lod_keywords = ["FIXED", "INCLUDE", "EXCLUDE"]
    tablecalc_keywords = ["WINDOW", "INDEX(", "LOOKUP(", "RUNNING_", "PREVIOUS_VALUE", "RANK(", "TOTAL(", "FIRST(", "LAST("]
    custom_sql = ["RAWSQL", "RAW_SQL"]
    # Complex if any LOD or table-calc keywords present
    if any(k in f for k in lod_keywords):
        return "complex"
    if any(k in f for k in tablecalc_keywords):
        return "complex"
    if any(k in f for k in custom_sql):
        return "complex"
    # If it looks like simple aggregation or arithmetic -> medium
    simple_patterns = ["SUM(", "AVG(", "MIN(", "MAX(", "+", "-", "*", "/", "IF ", "CASE ", "DATEPART(", "DATEDIFF("]
    if any(p in f for p in simple_patterns):
        return "medium"
    # Otherwise default to medium (still needs review)
    return "medium"

def parse_tableau_xml(xml_bytes):
    """
    Parse TWB xml bytes. Return dictionary with datasources, calculations, worksheets, parameters, filters, actions.
    Heuristics-based parsing to detect calc formulas and where used.
    """
    parsed = {
        "datasources": [],
        "calculations": [],  # global list of calculated fields found (name, formula, complexity, datasource)
        "worksheets": [],    # list of {name, xml_str}
        "parameters": [],
        "filters": [],
        "actions": []
    }
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        st.error(f"Unable to parse XML: {e}")
        return parsed

    # --- Datasources and columns (calculated fields) ---
    for ds in root.findall(".//datasource"):
        ds_name = ds.get("name") or ds.get("caption") or "datasource"
        ds_dict = {"name": ds_name, "columns": [], "custom_sql": False}
        # detect custom SQL by searching for "relation" nodes with custom-sql or 'inline' text
        for rel in ds.findall(".//relation"):
            table_attr = rel.get("table")
            if table_attr and "custom" in table_attr.lower():
                ds_dict["custom_sql"] = True
        for col in ds.findall(".//column"):
            col_name = col.get("name") or col.get("caption") or col.get("field") or "unknown"
            datatype = col.get("datatype") or col.get("type") or ""
            # Search for calculation element inside column
            calc_elem = None
            for c in col.findall(".//calculation"):
                calc_elem = c
                break
            # Some TWBs may put formula in attribute 'formula'
            formula = None
            if calc_elem is not None:
                formula = calc_elem.get("formula") or calc_elem.text or ""
            else:
                formula = col.get("formula") or col.get("calculation")
            is_calc = bool(formula)
            ds_dict["columns"].append({
                "name": col_name,
                "datatype": datatype,
                "is_calculation": is_calc,
                "formula": formula or ""
            })
            if is_calc and formula:
                parsed["calculations"].append({
                    "name": col_name,
                    "datasource": ds_name,
                    "formula": formula,
                    "complexity": detect_calc_complexity(formula)
                })
        parsed["datasources"].append(ds_dict)

    # --- Worksheets & Dashboards ---
    # Save XML string for each worksheet to do simple text searches later
    for ws in root.findall(".//worksheet"):
        try:
            xml_str = ET.tostring(ws, encoding="unicode")
        except Exception:
            xml_str = ""
        parsed["worksheets"].append({
            "name": ws.get("name") or ws.get("caption") or "worksheet",
            "xml": xml_str
        })

    # --- Parameters ---
    for p in root.findall(".//parameter"):
        parsed["parameters"].append({
            "name": p.get("name") or p.get("caption") or "parameter",
            "datatype": p.get("datatype") or ""
        })

    # --- Filters (heuristic) ---
    # Look for tags named filter, filtercolumn, filtering
    filter_elems = []
    for f in root.iter():
        if f.tag.lower().endswith("filter") or "filter" in f.tag.lower():
            filter_elems.append(f)
    parsed["filters"] = [{"tag": f.tag, "attrib": dict(f.attrib)} for f in filter_elems]

    # --- Actions (heuristic) ---
    # Dashboard actions, navigation, highlights etc.
    actions = []
    for a in root.findall(".//action") + root.findall(".//dashboard-action") + root.findall(".//highlight-action"):
        actions.append({"tag": a.tag, "attrib": dict(a.attrib)})
    parsed["actions"] = actions

    return parsed

def classify_worksheet(ws_record, parsed):
    """
    Returns classification 'simple'|'medium'|'complex' and reason text.
    More migration-friendly heuristics:
    - Complex: LOD expressions, table calculations, custom SQL, complex dashboard actions
    - Medium: Basic calculations, parameters, simple filters, basic actions
    - Simple: Basic fields only
    """
    name = ws_record.get("name", "")
    xml = ws_record.get("xml", "")
    
    # Find calculations referenced by name in xml
    calc_names = [c.get("name", "") for c in parsed["calculations"] if c.get("name")]
    referenced_calcs = [cn for cn in calc_names if cn and cn in xml]
    complex_calcs = [c for c in parsed["calculations"] if c.get("name") in referenced_calcs and c.get("complexity") == "complex"]
    medium_calcs = [c for c in parsed["calculations"] if c.get("name") in referenced_calcs and c.get("complexity") == "medium"]
    
    # Find if xml has parameter refs
    param_names = [p.get("name", "") for p in parsed["parameters"] if p.get("name")]
    referenced_params = [pn for pn in param_names if pn and pn in xml]
    
    # More nuanced filter detection
    xml_lower = xml.lower()
    has_basic_filter = "filter" in xml_lower and not any(complex_filter in xml_lower for complex_filter in ["advanced", "context", "condition"])
    has_complex_filter = any(complex_filter in xml_lower for complex_filter in ["advanced", "context", "condition"])
    
    # More nuanced action detection - only check for truly complex actions
    complex_action_indicators = [
        "url-action", "parameter-action", "go-to-sheet", "go-to-dashboard", 
        "export-action", "tabbed-navigation", "run-command"
    ]
    simple_action_indicators = [
        "filter-action", "highlight-action", "select"
    ]
    
    has_complex_actions = any(indicator in xml_lower for indicator in complex_action_indicators)
    has_simple_actions = any(indicator in xml_lower for indicator in simple_action_indicators) and not has_complex_actions
    
    # Custom SQL detection: check datasources used in xml
    ds_names = [ds.get("name", "") for ds in parsed["datasources"] if ds.get("name")]
    referenced_ds = [d for d in ds_names if d and d in xml]
    ds_custom_sql = [ds for ds in parsed["datasources"] if ds.get("name") in referenced_ds and ds.get("custom_sql")]
    
    # Check for other complex features
    complex_features = []
    if "story" in xml_lower:
        complex_features.append("story points")
    if "extension" in xml_lower:
        complex_features.append("extensions")
    if "map" in xml_lower and "latitude" in xml_lower and "longitude" in xml_lower:
        # Only mark maps as complex if they use custom lat/lng calculations
        if any("generate" in calc.lower() for calc in referenced_calcs):
            complex_features.append("complex mapping")
    
    # Determine classification with more migration-friendly logic
    if complex_calcs or ds_custom_sql or has_complex_actions or complex_features or has_complex_filter:
        classification = "complex"
        reason_parts = []
        if complex_calcs:
            reason_parts.append("complex calculations: " + ", ".join(set([c.get("name", "") for c in complex_calcs])))
        if has_complex_actions:
            reason_parts.append("complex dashboard actions")
        if ds_custom_sql:
            reason_parts.append("custom SQL datasource")
        if complex_features:
            reason_parts.append("complex features: " + ", ".join(complex_features))
        if has_complex_filter:
            reason_parts.append("complex filters")
        reason = "; ".join(reason_parts) or "complex features detected"
        possible = False
        
    elif medium_calcs or referenced_params or has_basic_filter or has_simple_actions:
        classification = "medium"
        reason_parts = []
        if medium_calcs:
            medium_calc_names = [c.get("name", "") for c in medium_calcs]
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
    """
    Build a dataframe with assessment for each worksheet
    """
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
    df = pd.DataFrame(rows)
    return df

# -------------------------
# LookML generation
# -------------------------
def generate_view_lookml(datasource, include_calcs_as_dimensions=True):
    """
    Create a LookML view for a given datasource dictionary.
    More comprehensive LookML generation with better type mapping and measures.
    """
    view_name = sanitize_identifier(datasource.get("name", "datasource"))
    
    # Clean up the table name - remove federated prefixes
    table_name = datasource.get("name", "your_table")
    if table_name.startswith("federated."):
        table_name = "your_table_name"  # Placeholder for user to update
    
    content_lines = [
        f"view: {view_name} {{",
        f"  sql_table_name: {table_name} ;;",
        f"  # Generated from Tableau datasource: {datasource.get('name', 'unknown')}",
        ""
    ]
    
    # Group columns by type for better organization
    dimension_cols = []
    measure_cols = []
    calc_cols = []
    
    for col in datasource.get("columns", []):
        col_name = col.get("name", "field")
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
    
    # Add regular dimensions first
    if dimension_cols:
        content_lines.append("  # Dimensions")
    
    for col in dimension_cols:
        col_name = col.get("name", "field")
        dimension_name = sanitize_identifier(col_name)
        looker_type = get_looker_type(col.get("datatype", ""))
        
        content_lines.append(f"  dimension: {dimension_name} {{")
        content_lines.append(f"    type: {looker_type}")
        content_lines.append(f"    sql: ${{TABLE}}.{col_name} ;;")
        
        # Add description if it's a calculated field
        formula = col.get("formula", "")
        if col.get("is_calculation") and formula:
            content_lines.append(f"    # Original Tableau formula: {formula[:100]}...")
            
        content_lines.append("  }")
        content_lines.append("")
    
    # Add measures
    if measure_cols:
        content_lines.append("  # Measures")
    
    for col in measure_cols:
        col_name = col.get("name", "field")
        dimension_name = sanitize_identifier(col_name)
        
        # Create dimension first
        content_lines.append(f"  dimension: {dimension_name} {{")
        content_lines.append(f"    type: number")
        content_lines.append(f"    sql: ${{TABLE}}.{col_name} ;;")
        content_lines.append("  }")
        content_lines.append("")
        
        # Create common measures
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
    
    # Add calculated fields with comments for manual review
    if calc_cols:
        content_lines.append("  # Calculated Fields - REVIEW REQUIRED")
        content_lines.append("  # These were calculated fields in Tableau and need manual conversion")
        
    for col in calc_cols:
        col_name = col.get("name", "field")
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

def get_looker_type(datatype):
    """Convert Tableau data type to appropriate Looker type"""
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

def generate_model_lookml(model_name, explores):
    """
    model_name: string
    explores: list of view names to expose as explores; for simplicity each explore references single view
    """
    lines = [f"connection: \"your_connection_name\"", "", f"model: {model_name} {{"]

    for v in explores:
        lines.append(f"  explore: {v} {{")
        lines.append("  }")
    lines.append("}")
    return "\n".join(lines)

def package_looker_project(parsed, assessment_df):
    """
    Generate a dictionary of filename -> content for LookML files for all datasources/worksheets marked possible.
    Rules:
      - For each datasource used by at least one possible worksheet -> create a view file
      - Create a model file that exposes explores for each possible worksheet (one explore per datasource/view)
    """
    files = {}
    # Determine possible worksheets
    possible_ws = assessment_df[assessment_df["possible_auto_migration"] == "Yes"]
    # Find referenced datasources from those worksheets
    referenced_ds_names = set()
    for _, row in possible_ws.iterrows():
        ds_str = row.get("referenced_datasources", "")
        if not ds_str:
            continue
        for ds in str(ds_str).split(";"):
            ds_name = ds.strip()
            if ds_name:
                referenced_ds_names.add(ds_name)
    # If none referenced explicitly, fallback to all datasources
    if not referenced_ds_names:
        referenced_ds_names = set([ds.get("name", "") for ds in parsed["datasources"] if ds.get("name")])

    view_names = []
    for ds in parsed["datasources"]:
        ds_name = ds.get("name", "")
        if ds_name in referenced_ds_names:
            vname, view_lkml = generate_view_lookml(ds)
            files[f"views/{vname}.view.lkml"] = view_lkml
            view_names.append(vname)
    # Build a model
    model_name = "migrated_model"
    model_lkml = generate_model_lookml(model_name, view_names)
    files[f"models/{model_name}.model.lkml"] = model_lkml
    # Add migration manifest / report
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
    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files_dict.items():
            # ensure parent folders in ZIP
            zf.writestr(filename, content)
    mem.seek(0)
    return mem.read()

# -------------------------
# UI: Upload, Assessment, Report, Generation
# -------------------------
st.markdown("## Step A — Upload Tableau workbook (.twb or .twbx)")
uploaded = st.file_uploader("Upload one .twb or .twbx file (for multiple uploads run multiple times)", type=["twb", "twbx"])

if uploaded:
    st.info(f"Uploaded: {uploaded.name}")
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
        st.success("Parsing Tableau workbook...")
        parsed = parse_tableau_xml(xml_bytes)
        # Basic summary
        st.markdown("### Workbook Summary")
        c1, c2, c3 = st.columns(3)
        c1.metric("Datasources", len(parsed["datasources"]))
        c2.metric("Worksheets", len(parsed["worksheets"]))
        c3.metric("Calculated Fields Found", len(parsed["calculations"]))
        # Show found datasources and count of columns
        st.markdown("#### Datasource details (top-level)")
        ds_rows = []
        for ds in parsed["datasources"]:
            ds_rows.append({
                "datasource": ds.get("name", "unknown"),
                "columns_count": len(ds.get("columns", [])),
                "custom_sql": "Yes" if ds.get("custom_sql") else "No",
                "calculated_fields": sum(1 for c in ds.get("columns", []) if c.get("is_calculation"))
            })
        ds_df = pd.DataFrame(ds_rows)
        st.dataframe(ds_df, height=200)

        # Show calculations (name, datasource, complexity)
        if parsed["calculations"]:
            st.markdown("#### Calculated fields found")
            calc_df = pd.DataFrame(parsed["calculations"])
            st.dataframe(calc_df, height=240)
        else:
            st.markdown("_No calculated fields found in datasources._")

        # Show parameters
        if parsed["parameters"]:
            st.markdown("#### Parameters found")
            st.dataframe(pd.DataFrame(parsed["parameters"]))
        else:
            st.markdown("_No parameters detected (or none present in workbook)._")

        # Build assessment dataframe
        st.markdown("## Step B — Assessment (Simple / Medium / Complex)")
        assessment_df = generate_assessment_df(parsed)
        
        # Show initial assessment summary
        initial_counts = assessment_df["classification"].value_counts().to_dict()
        st.write("**Initial Assessment Summary:**", initial_counts)
        
        # Allow reviewer to edit classification if they want
        st.markdown("### 📝 Review and Edit Classifications")
        st.write("The tool has made initial classifications based on detected features. You can override these classifications below:")
        
        # Add helpful guidance
        with st.expander("ℹ️ Classification Guidelines"):
            st.markdown("""
            **Simple** (Auto-migration possible):
            - Basic charts with standard fields
            - Simple aggregations (SUM, COUNT, AVG)
            - Basic filters and sorting
            
            **Medium** (Auto-migration possible with review):
            - Basic calculated fields (arithmetic, IF statements)
            - Parameters
            - Simple filters and highlight actions
            - Basic date functions
            
            **Complex** (Manual migration recommended):
            - LOD expressions (FIXED, INCLUDE, EXCLUDE)
            - Table calculations (WINDOW functions, RANK, etc.)
            - Custom SQL
            - Complex actions (URL actions, parameter changes)
            - Extensions, stories, advanced mapping
            """)
        
        # Create editable dataframe with better column configuration
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
        
        edited = st.data_editor(
            assessment_df, 
            column_config=column_config,
            use_container_width=True,
            height=400,
            key="assessment_editor"
        )
        
        # Add bulk override options
        st.markdown("### 🔄 Bulk Override Options")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Override All to Medium"):
                for idx in edited.index:
                    edited.at[idx, 'classification'] = 'medium'
                    edited.at[idx, 'possible_auto_migration'] = 'Yes'
                st.rerun()
                
        with col2:
            if st.button("Reset to Original Assessment"):
                edited = assessment_df.copy()
                st.rerun()
                
        with col3:
            selected_worksheets = st.multiselect(
                "Select worksheets to mark as Simple:",
                options=edited['worksheet'].tolist(),
                key="bulk_select_worksheets"
            )
            if st.button("Mark Selected as Simple") and selected_worksheets:
                for worksheet in selected_worksheets:
                    idx = edited[edited['worksheet'] == worksheet].index[0]
                    edited.at[idx, 'classification'] = 'simple'
                    edited.at[idx, 'possible_auto_migration'] = 'Yes'
                st.rerun()
        
        st.markdown("### 📊 Updated Assessment")
        updated_counts = edited["classification"].value_counts().to_dict()
        possible_count = (edited["possible_auto_migration"] == "Yes").sum()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Simple", updated_counts.get('simple', 0))
        col2.metric("Medium", updated_counts.get('medium', 0))
        col3.metric("Complex", updated_counts.get('complex', 0))
        col4.metric("Auto-Migratable", possible_count)
        
        st.dataframe(edited, use_container_width=True, height=300)

        # Provide download for assessment CSV
        csv_bytes = edited.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download Assessment Report (CSV)", 
            data=csv_bytes, 
            file_name=f"tableau_assessment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", 
            mime="text/csv"
        )

        st.markdown("---")
        # Show counts summary
        counts = edited["classification"].value_counts().to_dict()
        st.write("Classification counts:", counts)

        # Use Gemini Pro to propose translations of medium calc formulas
        st.markdown("### Optional: Use Gemini Pro to propose translations for medium calculated fields")
        gemini_key = os.getenv("GEMINI_API_KEY")
        if not gemini_key and hasattr(st, 'secrets') and "GEMINI_API_KEY" in st.secrets:
            gemini_key = st.secrets["GEMINI_API_KEY"]
        
        if gemini_key and GEMINI_AVAILABLE:
            if st.button("Propose translations for medium calcs (Gemini Pro)"):
                # collect medium calc formulas
                medium_calcs = [c for c in parsed["calculations"] if c.get("complexity") == "medium"]
                
                if not medium_calcs:
                    st.info("No medium complexity calculations found to translate.")
                else:
                    translations = []
                    # Process calculations in batches of 20 to avoid rate limits
                    batch_size = 20
                    total_batches = (len(medium_calcs) + batch_size - 1) // batch_size
                    
                    with st.spinner(f"Calling Gemini Pro ({total_batches} batch{'es' if total_batches > 1 else ''} for {len(medium_calcs)} calculations)..."):
                        for i in range(0, len(medium_calcs), batch_size):
                            batch = medium_calcs[i:i+batch_size]
                            batch_num = (i // batch_size) + 1
                            
                            # Create a single prompt for the entire batch
                            prompt = (
                                "You are an expert at converting Tableau calculated-field expressions into equivalent SQL / LookML "
                                "for a SQL warehouse. Convert the following Tableau calculations into LookML-compatible SQL snippets (Snowflake/ANSI SQL).\n\n"
                                "IMPORTANT: For each calculation, provide the response in this EXACT format (use the exact calculation name provided):\n"
                                "CALCULATION_NAME: [exact_name_from_input]\n"
                                "SQL_TRANSLATION: [your_sql_expression]\n"
                                "---\n\n"
                                "Guidelines for SQL conversion:\n"
                                "- Use standard SQL functions (SUM, AVG, CASE WHEN, etc.)\n"
                                "- Replace Tableau IF() with CASE WHEN\n"
                                "- Replace Tableau string functions with SQL equivalents\n"
                                "- For parameters, use ${parameter_name}\n"
                                "- Keep it simple and avoid complex subqueries when possible\n\n"
                                "Here are the calculations to convert:\n\n"
                            )
                            
                            # Add all calculations in this batch to the prompt
                            for j, calc in enumerate(batch, 1):
                                calc_name = calc.get('name', f'calc_{j}')
                                calc_formula = calc.get('formula', '')
                                prompt += f"#{j}. CALCULATION_NAME: {calc_name}\n"
                                prompt += f"TABLEAU_FORMULA: {calc_formula}\n\n"
                            
                            prompt += (
                                "\nPlease convert each calculation and follow the exact format specified above. "
                                "Use the exact calculation names I provided in your response."
                            )
                            
                            # Call Gemini with the batch
                            progress_text = f"Processing batch {batch_num}/{total_batches} ({len(batch)} calculations)..."
                            st.write(progress_text)
                            batch_response = call_gemini(prompt)
                            
                            # Parse the batch response
                            if "Gemini error" in batch_response:
                                # If there's an error, add individual error entries for this batch
                                for calc in batch:
                                    translations.append({
                                        "name": calc.get("name", "unknown"), 
                                        "formula": calc.get("formula", ""), 
                                        "translation": batch_response
                                    })
                            else:
                                # Parse the structured response
                                parsed_translations = parse_batch_response(batch_response, batch)
                                translations.extend(parsed_translations)
                                
                                # Show parsing success info
                                parsed_count = len([t for t in parsed_translations if not t.get("translation", "").startswith("#")])
                                st.success(f"✅ Batch {batch_num}: Successfully parsed {parsed_count}/{len(batch)} translations")
                            
                            # Add a small delay between batches to be respectful of rate limits
                            if i + batch_size < len(medium_calcs):
                                time.sleep(2)
                    
                    # Show summary of translation results
                    successful_translations = len([t for t in translations if not t.get("translation", "").startswith("#")])
                    st.info(f"📊 Translation Summary: {successful_translations}/{len(translations)} successful translations")
                    
                    # Show any problematic translations for debugging
                    problematic = [t for t in translations if t.get("translation", "").startswith("#")]
                    if problematic and len(problematic) < 5:  # Only show if few errors
                        with st.expander(f"⚠️ Debug Info - {len(problematic)} translations need review"):
                            for prob in problematic:
                                st.write(f"**{prob.get('name', 'unknown')}**: {prob.get('translation', '')[:100]}...")
                    
                    trans_df = pd.DataFrame(translations)
                    st.markdown("#### Proposed translations (review before using in production)")
                    st.dataframe(trans_df, height=300)
                    # allow download
                    trans_csv = trans_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Download Gemini translation suggestions", 
                        data=trans_csv, 
                        file_name=f"gemini_translations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
        else:
            if not GEMINI_AVAILABLE:
                st.info("Gemini API not available. Install with: pip install google-generativeai")
            else:
                st.info("To enable Gemini Pro translation, set environment variable GEMINI_API_KEY or add it to Streamlit secrets.")

        # Phase 2: Generate LookML for items that are possible
        st.markdown("## Step C — Generate LookML for worksheets marked Possible")
        if st.button("Generate LookML project (for items marked Possible)"):
            # Use the edited (possibly user-modified) assessment table
            df_for_generation = edited.copy()
            files = package_looker_project(parsed, df_for_generation)
            zip_bytes = zip_files_dict(files)
            st.success("LookML starter project generated. Download and review the files in Looker.")
            st.download_button(
                "⬇️ Download LookML Project ZIP", 
                data=zip_bytes, 
                file_name=f"looker_migration_starter_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip", 
                mime="application/zip"
            )
            # Also provide a migration report
            mig_report = {
                "summary": {
                    "datasources": len(parsed["datasources"]),
                    "worksheets_total": len(parsed["worksheets"]),
                    "worksheets_possible": int((df_for_generation["possible_auto_migration"] == "Yes").sum()),
                    "worksheets_not_possible": int((df_for_generation["possible_auto_migration"] == "No").sum()),
                    "generated_at": datetime.utcnow().isoformat() + "Z"
                },
                "assessments": df_for_generation.to_dict(orient="records")
            }
            report_json = json.dumps(mig_report, indent=2).encode("utf-8")
            st.download_button(
                "⬇️ Download Migration Report (JSON)", 
                data=report_json, 
                file_name=f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 
                mime="application/json"
            )

        st.markdown("---")
        st.markdown("### 📋 Migration Guidance & Next Steps")
        
        # Show actionable migration statistics
        total_worksheets = len(edited)
        possible_worksheets = (edited["possible_auto_migration"] == "Yes").sum()
        migration_percentage = (possible_worksheets / total_worksheets * 100) if total_worksheets > 0 else 0
        
        st.info(f"**Migration Readiness: {migration_percentage:.1f}%** ({possible_worksheets}/{total_worksheets} worksheets can be auto-migrated)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### ✅ Ready for Auto-Migration")
            ready_worksheets = edited[edited["possible_auto_migration"] == "Yes"]["worksheet"].tolist()
            if ready_worksheets:
                for ws in ready_worksheets[:5]:  # Show first 5
                    st.write(f"• {ws}")
                if len(ready_worksheets) > 5:
                    st.write(f"• ... and {len(ready_worksheets) - 5} more")
            else:
                st.write("*No worksheets marked for auto-migration*")
                st.write("💡 Consider overriding some classifications to 'medium' or 'simple'")
        
        with col2:
            st.markdown("#### ⚠️ Needs Manual Review")
            manual_worksheets = edited[edited["possible_auto_migration"] == "No"]["worksheet"].tolist()
            if manual_worksheets:
                for ws in manual_worksheets[:5]:  # Show first 5
                    try:
                        reason = edited[edited["worksheet"] == ws]["reason"].iloc[0]
                        st.write(f"• {ws}: {reason[:50]}...")
                    except (IndexError, KeyError):
                        st.write(f"• {ws}: Complex worksheet")
                if len(manual_worksheets) > 5:
                    st.write(f"• ... and {len(manual_worksheets) - 5} more")
        
        st.markdown("#### 🎯 Migration Strategy Recommendations")
        
        if migration_percentage >= 70:
            st.success("**High Migration Readiness** - Proceed with auto-migration for ready worksheets, then tackle complex ones manually.")
        elif migration_percentage >= 40:
            st.warning("**Medium Migration Readiness** - Consider simplifying some complex worksheets or review classifications.")
        else:
            st.error("**Low Migration Readiness** - Review classifications carefully. Many worksheets may be simpler than detected.")
        
        # Provide specific guidance based on detected patterns
        complex_reasons = edited[edited["classification"] == "complex"]["reason"].tolist()
        
        if any("dashboard actions" in str(reason).lower() for reason in complex_reasons):
            st.markdown("##### 🔧 Dashboard Actions Guidance")
            st.write("Many worksheets were marked complex due to dashboard actions. Consider:")
            st.write("• Simple filter/highlight actions can often be replicated in Looker")
            st.write("• URL actions may need custom implementation") 
            st.write("• Parameter actions can be handled with Looker filters")
        
        if any("complex calculations" in str(reason).lower() for reason in complex_reasons):
            st.markdown("##### 🧮 Complex Calculations Guidance")
            st.write("Complex calculations detected. Consider:")
            st.write("• LOD expressions may need to be rewritten as subqueries")
            st.write("• Table calculations can often be converted to window functions")
            st.write("• Use the Gemini translation feature above for suggestions")
        
        st.markdown("#### 🚀 Looker Prerequisites")
        st.markdown("""
        **Before using generated LookML:**
        1. **Database Connection**: Ensure your data is in a SQL warehouse (Snowflake, BigQuery, Redshift, etc.)
        2. **Looker Instance**: Have a running Looker instance with developer access
        3. **LookML Project**: Create or access a LookML project where you can add the generated files
        4. **Data Validation**: Verify that table/column names match your actual database schema
        
        **After generating LookML:**
        1. Update connection names in the model file
        2. Update table names to match your database schema
        3. Review and uncomment calculated field definitions
        4. Test explores in Looker development mode
        5. Validate data accuracy before deploying to production
        """)
        
        # Show potential time savings
        if possible_worksheets > 0:
            estimated_hours_saved = possible_worksheets * 2  # Assume 2 hours per worksheet
            st.success(f"**Potential Time Savings**: ~{estimated_hours_saved} hours of manual LookML development")
        
        st.markdown("---")

        # Add deployment section
        st.markdown("## Step D — Deploy to Looker (Optional)")
        st.write("Deploy the generated LookML files directly to your Looker instance.")
        
        if not LOOKER_SDK_AVAILABLE:
            st.warning("Looker SDK not available. Install with: pip install looker-sdk")
        elif not os.path.exists("looker.ini"):
            st.warning("looker.ini configuration file not found. Create this file with your Looker API credentials.")
            with st.expander("📋 How to create looker.ini"):
                st.code("""
[Looker]
base_url = "https://your-instance.cloud.looker.com/"
client_id = "your_client_id"
client_secret = "your_client_secret"
                """)
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                project_name = st.text_input(
                    "Looker Project Name", 
                    value=os.getenv("LOOKER_PROJECT_NAME", ""),
                    help="Name of your LookML project in Looker"
                )
            
            with col2:
                if st.button("🚀 Deploy to Looker", type="primary", disabled=not project_name):
                    if project_name:
                        # Set the project name in environment for the deployment function
                        os.environ["LOOKER_PROJECT_NAME"] = project_name
                        success = deploy_lookml_to_looker()
                        if success:
                            st.success("🎉 Successfully deployed to Looker!")
                            st.balloons()
                        else:
                            st.error("❌ Deployment failed. Check your Looker configuration and permissions.")
                    else:
                        st.error("Please enter a project name")

        # Final recommendations
        st.markdown("### 🎯 Next Steps")
        st.markdown("""
        1. **Download** the generated LookML project ZIP file
        2. **Extract** and review the files before deployment
        3. **Update** connection names and table references
        4. **Test** in Looker development mode
        5. **Validate** data accuracy with sample queries
        6. **Deploy** to production when ready
        """)
        
        # Footer
        st.markdown("---")
        st.markdown("""
        <div style="text-align: center; color: #666; font-size: 0.9em;">
            <p>🔄 Tableau → Looker Migration Kit</p>
            <p>Automated assessment, LookML generation, and deployment platform</p>
        </div>
        """, unsafe_allow_html=True)