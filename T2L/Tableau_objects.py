# model/Tableau_objects.py

# TODO - parsing calculated columns - severity: 1
# TODO - optimizing structure - severity: 3
# TODO - proper logging - severity: 3
# TODO - handling Tableau extracts - severity: 4
# TODO - handling right joins in Relation - severity: 4
# TODO - handling Relation with type union - severity: 5

# ? is it possible to have multiple panes in a worksheet? -> YES, when multiple measures in an axis
# IDEA - create a custom SQL for objects if it is related to multiple tables (relation type union, join)

from __future__ import annotations
from itertools import chain
import os

from utils.process_util import load_tableau_extract_to_dict
from utils.main_logger import logger
from .LookML_objects import ViewBaseField, LookMLView, LookMLExplore, LookMLModel, ViewDerivedField, DashboardElement, \
    LookMLProject, Dashboard
from .LookML_enums import ViewBaseTypeEnum, JoinTypeEnum, LookMLTimeframesEnum, TimeDatatypeEnum, \
    LookMLDashboardElementTypeEnum, LookMLFieldStructEnum, LookMLMeasureTypeEnum, JoinRelationshipEnum
from collections import OrderedDict
from html import unescape
import re


def without_square_brackets(p_text: str) -> str:
    """
    Remove square brackets (if exists) from the string (e.g. '[word]' -> 'word')
    :param p_text: text to be processed
    :return: string without squared brackets
    """
    if p_text and p_text.startswith('[') and p_text.endswith(']'):
        return p_text[1:-1]
    return p_text


def iter_tag(p_tag_value: dict | list | None):
    """
    Helper function to iterate over a tag's value which might be a single dictionary,
    a list of dictionaries, or None.
    """
    if p_tag_value is None:
        return
    if isinstance(p_tag_value, dict):
        yield p_tag_value
    elif isinstance(p_tag_value, list):
        for act_val in p_tag_value:
            yield act_val
    else:
        # Log unexpected types if necessary, but don't raise an error to keep it robust
        logger.debug(f"iter_tag received unexpected type: {type(p_tag_value)}. Yielding nothing.")


class Workbook:
    """
    Represents a Tableau Workbook, which is the top-level container for datasources
    and worksheets. It orchestrates the parsing of the Tableau extract and the
    generation of the LookML project.
    """
    _file_path: str
    tableau_workbook_name: str = ''
    parameter_table: ParameterTable | None = None
    datasources: dict[str, Datasource] = {}
    worksheets: dict[str, Worksheet] = {}
    _lookml_project: LookMLProject | None = None
    _raw_extract: dict
    _deployment_folder: str

    def __init__(self, p_deployment_folder: str = 'lookml_files'):
        """
        Initializes a Workbook instance with an optional deployment folder.
        :param p_deployment_folder: The target directory for generated LookML files.
        """
        self._deployment_folder = p_deployment_folder

    @property
    def file_full_path(self):
        return self._file_path

    @file_full_path.setter
    def file_full_path(self, p_file_path: str):
        """
        Sets the full path to the Tableau workbook file and triggers parsing.
        """
        self._file_path = p_file_path
        logger.info(f'Loading extract from {p_file_path}.')
        self.tableau_workbook_name, self.raw_extract = load_tableau_extract_to_dict(p_file_path=p_file_path)

    @property
    def file_name(self):
        return os.path.basename(self._file_path)

    @property
    def file_name_wo_ext(self):
        return os.path.splitext(self.file_name)[0]

    @property
    def lookml_project(self) -> LookMLProject:
        """
        Returns or creates the associated LookMLProject object.
        Ensures the deployment folder is set on the project.
        """
        if self._lookml_project:
            return self._lookml_project
        self._lookml_project = LookMLProject()
        self._lookml_project.name_orig = self.tableau_workbook_name or self.file_name_wo_ext
        self._lookml_project.deployment_folder = self._deployment_folder
        return self._lookml_project

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary extracted from the Tableau workbook XML
        and triggers the extraction of datasources and worksheets.
        """
        self._raw_extract = p_raw_extract
        if not self.raw_extract:
            logger.warning("Raw extract is empty for workbook.")
            return
        self.extract_datasources()
        self.extract_worksheets()

    def extract_datasources(self):
        """Extracts datasource objects from the raw Tableau extract."""
        # Ensure .get() provides a default empty dictionary if keys are missing
        ds_part = self._raw_extract.get('workbook', {}).get('datasources', {}).get('datasource', None)
        self.datasources = {}
        for ds_item in iter_tag(ds_part):
            if ds_item.get('@name', '') == 'Parameters':
                self.parameter_table = ParameterTable()
                self.parameter_table.parent_object = self
                self.parameter_table.raw_extract = ds_item
                continue
            new_datasource = Datasource()
            new_datasource.parent_object = self
            new_datasource.raw_extract = ds_item
            if new_datasource.name:
                self.datasources[new_datasource.name] = new_datasource
            else:
                logger.warning(f"Datasource found without a name: {ds_item}")

    def extract_worksheets(self):
        """Extracts worksheet objects from the raw Tableau extract."""
        # Ensure .get() provides a default empty dictionary if keys are missing
        ws_part = self._raw_extract.get('workbook', {}).get('worksheets', {}).get('worksheet', None)
        self.worksheets = {}
        for ws_item in iter_tag(ws_part):
            new_worksheet = Worksheet()
            new_worksheet.parent_object = self
            new_worksheet.raw_extract = ws_item
            if new_worksheet.name:
                self.worksheets[new_worksheet.name] = new_worksheet
            else:
                logger.warning(f"Worksheet found without a name: {ws_item}")


class Datasource:
    """
    Represents a Tableau Datasource, containing connection details,
    the object graph (logical tables and relationships), and metadata columns.
    """
    name: str = ''
    caption: str = ''
    connection: Connection | None = None
    parent_object: Workbook
    object_graph: ObjectGraph | None = None
    _lookml_model: LookMLModel | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @property
    def lookml_model(self) -> LookMLModel:
        """
        Returns or creates the associated LookMLModel object.
        """
        if self._lookml_model is None:
            self._lookml_model = LookMLModel()
            self._lookml_model.name_orig = self.caption or self.name or "unknown_model"
            self.parent_object.lookml_project.add_lookml_model(self._lookml_model)
        return self._lookml_model

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the datasource and triggers its parsing.
        """
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name', '')
        self.caption = p_raw_extract.get('@caption', '')
        logger.info(f'Parsing {self}.')
        if not p_raw_extract:
            logger.warning("Raw extract is empty for datasource.")
            return
        self.extract_connection()
        self.extract_object_graph()
        # Ensure lookml_model is set up before related objects try to access it
        _ = self.lookml_model


    def extract_connection(self):
        """Extracts connection details for the datasource."""
        if conn_data := self.raw_extract.get('connection'):
            self.connection = Connection()
            self.connection.parent_object = self
            self.connection.raw_extract = conn_data

    def extract_object_graph(self):
        """Extracts the object graph (logical tables and relationships)."""
        # Tableau XML can have different paths for object-graph
        for object_graph_text in ('object-graph', '_.fcp.ObjectModelEncapsulateLegacy.true...object-graph',
                                  '_.fcp.ObjectModelEncapsulateLegacy.false...object-graph'):
            if object_data := self.raw_extract.get(object_graph_text):
                self.object_graph = ObjectGraph()
                self.object_graph.parent_object = self
                self.object_graph.raw_extract = object_data
                break
        if not self.object_graph:
            logger.warning(f"No object-graph found for datasource '{self.name}'.")

    def yield_relations(self):
        """Yields all relations found within the datasource's connection."""
        if self.connection:
            yield from self.connection.yield_relations()


    def __str__(self):
        return f"{self.__class__.__name__} '{self.caption or self.name}'"

    def __repr__(self):
        return f"Tableau Datasource '{self.name}' ({self.caption or 'No Caption'})"


class Worksheet:
    """
    Represents a Tableau Worksheet, which translates into a LookML dashboard element.
    It contains layout information, used columns (column instances), and pane details.
    """
    name: str = ''
    parent_object: Workbook
    used_column_instances: dict[str, ColumnInstance] = {}
    titles: list[dict] = []
    panes: list[WorksheetPane] = []
    rows: list[ColumnInstance] = []
    cols: list[ColumnInstance] = []
    pane_texts: list[ColumnInstance] = []
    pane_wedge_sizes: list[ColumnInstance] = []
    pane_colors: list[ColumnInstance] = []
    _lookml_dashboardelement: DashboardElement | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the worksheet and triggers its parsing.
        """
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name', '')
        logger.info(f'Parsing {self}.')
        if not p_raw_extract:
            logger.warning("Raw extract is empty for worksheet.")
            return

        self.extract_titles()
        self.extract_column_instances()
        self.set_rows()
        self.set_cols()
        self.extract_pane()
        _ = self.lookml_dashboardelement # Trigger dashboard element creation


    @property
    def lookml_dashboardelement(self) -> DashboardElement:
        """
        Returns or creates the associated LookML DashboardElement.
        Infers the LookML dashboard element type based on Tableau mark class and axis data.
        """
        if self._lookml_dashboardelement:
            return self._lookml_dashboardelement

        new_dashboardelement = DashboardElement()
        new_dashboardelement.element_name_orig = self.name

        # Set title
        if self.titles:
            title_string_list = []
            for act_part in self.titles:
                text_content = act_part.get('#text', '').replace('"', "'")
                if text_content == 'Æ': # Tableau's internal newline character often
                    continue
                if title_string_list and title_string_list[-1] != '\n':
                    title_string_list.append(' ')
                title_string_list.append(self.name if text_content == '<Sheet Name>' else text_content)
            new_dashboardelement.title = '"' + "".join(title_string_list).strip() + '"'

        # Infer dashboard element type
        # Basic mapping from Tableau mark class to LookML dashboard element type
        dashboard_type_mapping = {
            'Text': LookMLDashboardElementTypeEnum.TABLE,
            'Bar': LookMLDashboardElementTypeEnum.LOOKER_COLUMN, # Default assumption, can be horizontal bar too
            'Line': LookMLDashboardElementTypeEnum.LOOKER_LINE,
            'Square': LookMLDashboardElementTypeEnum.TABLE, # Often used for heatmaps/tables
            'Area': LookMLDashboardElementTypeEnum.LOOKER_AREA,
            'Pie': LookMLDashboardElementTypeEnum.LOOKER_PIE,
            'Shape': LookMLDashboardElementTypeEnum.LOOKER_SCATTER,
            # 'GanttBar', 'Multipolygon' might need more specific handling or default to TABLE
        }

        mark_class = self.panes[0].mark_class if self.panes else 'Automatic'
        inferred_type = dashboard_type_mapping.get(mark_class)

        # Refine 'Automatic' type based on axis content
        if mark_class == 'Automatic':
            has_measure_in_rows = any(f.role == 'measure' for f in self.rows)
            has_measure_in_cols = any(f.role == 'measure' for f in self.cols)
            has_dim_in_rows = any(f.role == 'dimension' for f in self.rows)
            has_dim_in_cols = any(f.role == 'dimension' for f in self.cols)

            if has_measure_in_rows and has_measure_in_cols:
                inferred_type = LookMLDashboardElementTypeEnum.LOOKER_SCATTER
            elif has_dim_in_rows and has_measure_in_cols:
                inferred_type = LookMLDashboardElementTypeEnum.LOOKER_LINE # Common for time series
            elif has_dim_in_cols and has_measure_in_rows:
                inferred_type = LookMLDashboardElementTypeEnum.LOOKER_LINE # Common for time series
            elif has_measure_in_cols:
                inferred_type = LookMLDashboardElementTypeEnum.LOOKER_BAR
            elif has_measure_in_rows:
                inferred_type = LookMLDashboardElementTypeEnum.LOOKER_COLUMN
            elif not self.rows and not self.cols and len(self.pane_texts) == 1 and self.pane_texts[0].role == 'measure':
                inferred_type = LookMLDashboardElementTypeEnum.SINGLE_VALUE
            else:
                inferred_type = LookMLDashboardElementTypeEnum.TABLE # Default for complex or non-standard
            logger.info(f"Automatic mark class converted to LookML dashboard type '{inferred_type}'.")
        
        new_dashboardelement.type = inferred_type or LookMLDashboardElementTypeEnum.TABLE # Default if still None

        self.parent_object.lookml_project.add_lookml_dashboardelement(new_dashboardelement)
        self._lookml_dashboardelement = new_dashboardelement

        # Add to a test dashboard (for testing purposes)
        if not self.parent_object.lookml_project.lookml_dashboards:
            test_dashboard = Dashboard()
            test_dashboard.name_orig = f'test_dashboard_{self.parent_object.tableau_workbook_name}'
            test_dashboard.title = f'Test dashboard from Tableau Workbook {self.parent_object.tableau_workbook_name}'
            self.parent_object.lookml_project.add_lookml_dashboard(test_dashboard)
        else:
            _, test_dashboard = next(iter(self.parent_object.lookml_project.lookml_dashboards.items()))
        test_dashboard.add_dashboard_elements(new_dashboardelement)


        # Assign fields (dimensions and measures) and pivots
        new_dashboardelement.fields = [ci.lookml_derived_field for ci in
                                       chain(self.rows, self.cols, self.pane_texts,
                                             self.pane_wedge_sizes, self.pane_colors)
                                       if ci and ci.lookml_derived_field and ci.lookml_derived_field.lookml_struct_type in (LookMLFieldStructEnum.DIMENSION, LookMLFieldStructEnum.MEASURE, LookMLFieldStructEnum.DIMENSION_GROUP)]

        # Determine pivots based on chart type and axis data
        if new_dashboardelement.type in (LookMLDashboardElementTypeEnum.LOOKER_COLUMN, LookMLDashboardElementTypeEnum.LOOKER_BAR, LookMLDashboardElementTypeEnum.TABLE):
            # For bar/column/table, often the non-measure axis (or additional dimensions) are pivots
            if self.cols and any(f.role == 'measure' for f in self.rows): # e.g., column chart (measure on rows, dimension on cols)
                new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.cols if ci and ci.lookml_derived_field and ci.lookml_derived_field.lookml_struct_type == LookMLFieldStructEnum.DIMENSION]
            elif self.rows and any(f.role == 'measure' for f in self.cols): # e.g., bar chart (measure on cols, dimension on rows)
                new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.rows if ci and ci.lookml_derived_field and ci.lookml_derived_field.lookml_struct_type == LookMLFieldStructEnum.DIMENSION]
            elif new_dashboardelement.type == LookMLDashboardElementTypeEnum.TABLE:
                 new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.cols if ci and ci.lookml_derived_field] # All columns can be pivots in a table
        elif new_dashboardelement.type == LookMLDashboardElementTypeEnum.LOOKER_LINE:
            # For line charts, color (series) is often a pivot
            new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.pane_colors if ci and ci.lookml_derived_field]
        else:
            # Default pivots to columns for other types if not explicitly handled
            new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.cols if ci and ci.lookml_derived_field]


        # Link dashboard element to its model and explore
        try:
            # Find the first column instance with a valid parent path to infer model and explore
            # This logic assumes all fields in a worksheet belong to the same model/explore.
            first_valid_ci = next(iter(ci for ci in chain(self.rows, self.cols, self.pane_texts, self.pane_wedge_sizes, self.pane_colors) if ci and ci.parent_object and ci.parent_object.datasource and ci.parent_object.datasource.object_graph), None)
            if first_valid_ci:
                new_dashboardelement.lookml_model = first_valid_ci.parent_object.datasource.lookml_model
                new_dashboardelement.lookml_explore = first_valid_ci.parent_object.datasource.object_graph.lookml_explore
            else:
                logger.warning(f'No valid column instances found in worksheet "{self.name}" to infer LookML model/explore.')
        except StopIteration:
            logger.warning(f'No column instances found for worksheet "{self.name}", cannot infer LookML model/explore for dashboard element.')
        except AttributeError as e:
            logger.error(f'Attribute error while inferring LookML model/explore for worksheet "{self.name}": {e}')


        return self._lookml_dashboardelement

    def extract_titles(self):
        """Extracts title components from the worksheet raw data."""
        self.titles = []
        # Title can be a single string or a dict/list
        ds_title_parts_data = self.raw_extract.get('layout-options', {}).get('title', {}).get('formatted-text', {}).get('run')
        if isinstance(ds_title_parts_data, str):
            self.titles.append({'#text': ds_title_parts_data})
        else:
            for ds_title_part in iter_tag(ds_title_parts_data or []): # Ensure iter_tag gets an iterable
                if isinstance(ds_title_part, str):
                    self.titles.append({'#text': ds_title_part})
                elif isinstance(ds_title_part, dict):
                    self.titles.append(ds_title_part)
                else:
                    logger.warning(f"Unexpected title part type: {type(ds_title_part)}")


    def extract_column_instances(self):
        """
        Extracts column instances (usage of metadata columns in the worksheet)
        and links them to their corresponding metadata columns.
        """
        self.used_column_instances = {}
        # Ensure .get() provides a default empty dictionary or list
        ds_dependencies_data = self.raw_extract.get('table', {}).get('view', {}).get('datasource-dependencies')
        
        # This part ensures that if 'datasource-dependencies' is a single dict, it's treated as a list.
        # It also makes sure to default to an empty list if nothing is found.
        if isinstance(ds_dependencies_data, dict):
            ds_dependencies_list = [ds_dependencies_data]
        elif isinstance(ds_dependencies_data, list):
            ds_dependencies_list = ds_dependencies_data
        else:
            ds_dependencies_list = []

        for act_ds_dependency in iter_tag(ds_dependencies_list):
            ds_name = act_ds_dependency.get('@datasource')
            act_ds = self.parent_object.datasources.get(ds_name)
            if not act_ds:
                logger.warning(f"Datasource '{ds_name}' not found for dependencies in worksheet '{self.name}'.")
                continue

            # Ensure .get() provides an empty list if 'column' key is missing or not a list
            column_roles = {c.get('@name'): c.get('@role') for c in iter_tag(act_ds_dependency.get('column', []))}

            # Ensure .get() provides an empty list if 'column-instance' key is missing or not a list
            for act_column_instance_data in iter_tag(act_ds_dependency.get('column-instance', [])):
                column_ref = act_column_instance_data.get('@column')
                if not column_ref:
                    logger.warning(f"Column instance without '@column' attribute in worksheet '{self.name}': {act_column_instance_data}")
                    continue

                act_column_name_wo_brackets = without_square_brackets(column_ref.split('.')[-1]) # Get just the column name part
                # Removed datasource_name_part as it's not directly used for lookup here

                found_relation_column = False
                for act_relation in act_ds.yield_relations():
                    # Check if the column belongs to this relation
                    if act_column_name_wo_brackets in act_relation.columns:
                        new_ci = act_relation.columns[act_column_name_wo_brackets].add_column_instance(act_column_instance_data)
                        new_ci.role = column_roles.get(column_ref) # Use original full column ref for role lookup
                        self.used_column_instances[column_ref] = new_ci # Store with full Tableau ref
                        found_relation_column = True
                        break
                if not found_relation_column:
                    logger.warning(f"Column instance '{column_ref}' root not found in any relation for datasource '{ds_name}' in worksheet '{self.name}'.")


    def extract_pane(self):
        """Extracts pane information, including encodings (text, size, color)."""
        self.panes = []
        self.pane_texts = []
        self.pane_wedge_sizes = []
        self.pane_colors = []
        # Ensure .get() provides a default empty dictionary or list for 'pane'
        for act_pane_data in iter_tag(self.raw_extract.get('table', {}).get('panes', {}).get('pane', [])):
            new_pane = WorksheetPane()
            new_pane.parent_object = self
            new_pane.raw_extract = act_pane_data
            self.panes.append(new_pane)
            
            # Populate pane-specific lists using the full column reference key
            # Ensure .get() provides empty list if encoding type is missing
            for pane_text_column_key in new_pane.encodings.get('text', []):
                if pane_text_column_key in self.used_column_instances:
                    self.pane_texts.append(self.used_column_instances[pane_text_column_key])
            for pane_wedge_size_column_key in new_pane.encodings.get('wedge-size', []):
                if pane_wedge_size_column_key in self.used_column_instances:
                    self.pane_wedge_sizes.append(self.used_column_instances[pane_wedge_size_column_key])
            for pane_color_column_key in new_pane.encodings.get('color', []):
                if pane_color_column_key in self.used_column_instances:
                    self.pane_colors.append(self.used_column_instances[pane_color_column_key])


    def _set_axis_columns(self, axis_data: str, target_list: list):
        """Helper to parse axis data (rows/cols) and populate target_list."""
        if not axis_data:
            return
        for act_ci_match in re.finditer(r'\[[^[]+]\.\[[^[]+]', axis_data):
            ci_key = act_ci_match.group(0)
            if ci_key in self.used_column_instances:
                ci = self.used_column_instances[ci_key]
                target_list.append(ci)
                logger.debug(f'Adding {ci} to {self.name} axis.')
            else:
                logger.warning(f'Column instance key not found in used_column_instances for axis: {ci_key}')

    def set_rows(self):
        """Parses and sets columns used in the rows axis."""
        self.rows = []
        rows_data = self.raw_extract.get('table', {}).get('rows', '')
        self._set_axis_columns(rows_data, self.rows)
        if not self.rows:
            logger.info(f'{self.name} - no rows found.')

    def set_cols(self):
        """Parses and sets columns used in the columns axis."""
        self.cols = []
        cols_data = self.raw_extract.get('table', {}).get('cols', '')
        self._set_axis_columns(cols_data, self.cols)
        if not self.cols:
            logger.info(f'{self.name} - no cols found.')


    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class WorksheetPane:
    """
    Represents a pane within a Tableau worksheet, containing mark type and encoding details.
    """
    id: str = ''
    mark_class: str = ''
    encodings: dict[str, list[str]] = {}
    parent_object: Worksheet
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the pane and extracts its properties.
        """
        self._raw_extract = p_raw_extract
        self.id = p_raw_extract.get('@id', '')
        self.mark_class = p_raw_extract.get('mark', {}).get('@class', '')
        logger.debug(f"{self.parent_object} mark class is '{self.mark_class}'")

        self.encodings = {}
        # Ensure .items() is called on a dictionary, not None
        for encoding_type, encoding_data in p_raw_extract.get('encodings', {}).items():
            for enc_detail_data in iter_tag(encoding_data or []): # Ensure iter_tag receives an iterable
                column_key = enc_detail_data.get('@column')
                if column_key:
                    self.encodings.setdefault(encoding_type, []).append(column_key)
                else:
                    logger.warning(f"Encoding detail without '@column' in pane '{self.id}': {enc_detail_data}")


class ParameterTable:
    """
    Represents the 'Parameters' datasource in Tableau, containing global parameters.
    """
    name: str = ''
    parameter_fields: dict[str, ParameterField] = {}
    parent_object: Workbook
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the parameter table and extracts its fields.
        """
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name', '')
        logger.info(f'Parsing {self}.')
        
        self.parameter_fields = {}
        # Ensure .get() provides a default empty dictionary or list for 'column'
        parameter_fields_extract = p_raw_extract.get('column', [])
        
        for param_field_data in iter_tag(parameter_fields_extract):
            new_param_field = ParameterField()
            new_param_field.raw_extract = param_field_data
            new_param_field.parent_object = self
            if new_param_field.name:
                self.parameter_fields[new_param_field.name] = new_param_field
            else:
                logger.warning(f"Parameter field found without a name: {param_field_data}")

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class ParameterField:
    """
    Represents a single parameter field within a Tableau workbook.
    """
    name: str = ''
    caption: str = ''
    datatype: str = ''
    param_domain_type: str = ''
    role: str = ''
    type: str = ''
    default_format: str = ''
    value: str = ''
    parent_object: ParameterTable # Changed from Workbook to ParameterTable
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the parameter field and extracts its attributes.
        """
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name', '')
        self.caption = p_raw_extract.get('@caption', '')
        self.datatype = p_raw_extract.get('@datatype', '')
        self.param_domain_type = p_raw_extract.get('@param-domain-type', '')
        self.role = p_raw_extract.get('@role', '')
        self.type = p_raw_extract.get('@type', '')
        self.default_format = p_raw_extract.get('@default-format', '')
        self.value = p_raw_extract.get('@value', '')

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class Connection:
    """
    Represents a database connection within a Tableau datasource.
    Can be a direct connection or a federated connection with child named connections.
    """
    conn_class: str = ''
    conn_dialect: str | None = None
    conn_dbname: str | None = None
    conn_server: str | None = None
    conn_port: str | None = None
    conn_username: str | None = None
    conn_child_named_connections: dict[str, NamedConnection] = {}
    relation: Relation | None = None # The primary relation for this connection
    parent_object: Datasource | NamedConnection
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the connection and extracts its attributes.
        """
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            logger.warning("Raw extract is empty for connection.")
            return

        self.conn_class = p_raw_extract.get('@class', '')
        self.conn_dialect = p_raw_extract.get('@connection-dialect')
        self.conn_dbname = p_raw_extract.get('@dbname')
        self.conn_server = p_raw_extract.get('@server')
        self.conn_port = p_raw_extract.get('@port')
        self.conn_username = p_raw_extract.get('@username')
        
        self.conn_child_named_connections = {}
        if self.conn_class == 'federated':
            # Ensure .get() provides a default empty dictionary or list for 'named-connection'
            child_named_conns = p_raw_extract.get('named-connections', {}).get('named-connection', [])
            for ch_named_conn_item in iter_tag(ch_named_conns):
                new_nc = NamedConnection()
                new_nc.raw_extract = ch_named_conn_item
                new_nc.parent_object = self
                if new_nc.conn_name:
                    self.conn_child_named_connections[new_nc.conn_name] = new_nc
                else:
                    logger.warning(f"Named connection found without a name: {ch_named_conn_item}")
        
        self.extract_relations()
        # Metadata columns extraction needs relation to be established first
        self.extract_metadata_columns()

    def extract_relations(self):
        """Extracts the primary relation(s) associated with this connection."""
        for rel_text in ('relation', '_.fcp.ObjectModelEncapsulateLegacy.true...relation',
                         '_.fcp.ObjectModelEncapsulateLegacy.false...relation'):
            if ds_relation_data := self.raw_extract.get(rel_text):
                self.relation = Relation()
                self.relation.raw_extract = ds_relation_data
                self.relation.parent_object = self
                break
        if not self.relation:
            logger.warning(f"No primary relation found for connection class '{self.conn_class}'.")

    def extract_metadata_columns(self):
        """Extracts metadata columns and adds them to their respective relations."""
        # Ensure .get() provides a default empty dictionary or list for 'metadata-record'
        metadata_records_data = self.raw_extract.get('metadata-records', {}).get('metadata-record', [])
        for act_meta_record_item in iter_tag(metadata_records_data):
            if act_meta_record_item.get('@class', '') == 'column':
                new_meta_column = MetadataColumn()
                new_meta_column.parent_object = self
                new_meta_column.raw_extract = act_meta_record_item
                
                # Try to add the metadata column to its appropriate relation
                found_relation_for_meta = False
                if self.relation:
                    # Check the primary relation and its children
                    for rel in self.relation.yield_relations():
                        # The parent_name in metadata can be like '[Tableau_logical_table_name]'
                        # The rel.rel_name might be like 'Tableau_logical_table_name'
                        # So we need to normalize both for comparison.
                        normalized_parent_name = without_square_brackets(new_meta_column.parent_name) if new_meta_column.parent_name else ''
                        normalized_rel_name = without_square_brackets(rel.rel_name) if rel.rel_name else ''

                        if normalized_parent_name and normalized_parent_name == normalized_rel_name:
                            rel.add_metacolumn(new_meta_column)
                            found_relation_for_meta = True
                            break
                        # Also check against rel_table for relations of type 'table'
                        normalized_rel_table = without_square_brackets(rel.rel_table.split('.')[-1]) if rel.rel_table else ''
                        if normalized_parent_name and normalized_parent_name == normalized_rel_table:
                             rel.add_metacolumn(new_meta_column)
                             found_relation_for_meta = True
                             break
                if not found_relation_for_meta:
                    logger.warning(f"Metadata column '{new_meta_column.local_name}' (parent '{new_meta_column.parent_name}') found but no matching relation to attach it to.")


    def yield_relations(self):
        """Yields all relations recursively from this connection."""
        if self.relation:
            yield from self.relation.yield_relations()


class NamedConnection:
    """
    Represents a named connection within a federated Tableau connection.
    """
    conn_name: str | None = None
    conn_caption: str | None = None
    conn_object: Connection
    parent_object: Connection # Parent is another Connection (federated type)
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the named connection and extracts its attributes.
        """
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            logger.warning("Raw extract is empty for named connection.")
            return
        self.conn_name = p_raw_extract.get('@name')
        self.conn_caption = p_raw_extract.get('@caption')
        
        self.conn_object = Connection()
        self.conn_object.raw_extract = p_raw_extract.get('connection', {})
        self.conn_object.parent_object = self # Set parent to self (NamedConnection)

    def __repr__(self):
        return f"Tableau Named Connection '{self.conn_name}' ({self.conn_caption or 'No Caption'})"


class JoinExpression:
    """
    Represents a join expression within a Tableau relation,
    defining how tables are joined (e.g., 'AND', '=', etc.).
    """
    op: str = ''
    index: int = 0
    children_expressions: list[JoinExpression] = []
    parent_object: JoinExpression | Relation | Relationship
    _metacolumn: MetadataColumn | None = None
    _raw_extract: dict

    def __str__(self):
        if not self.children_expressions:
            return self.op
        return f"({(' ' + unescape(self.op) + ' ').join(str(ce) for ce in self.children_expressions)})"

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the join expression and extracts its properties.
        Recursively extracts child expressions.
        """
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            logger.warning("Raw extract is empty for join expression.")
            return
        self.op = p_raw_extract.get('@op', '')
        self.children_expressions = []
        expression_data = p_raw_extract.get('expression')
        
        for child_expr_data in iter_tag(expression_data or []): # Ensure iter_tag gets an iterable
            new_expr = JoinExpression()
            new_expr.index = len(self.children_expressions) # Assign index based on order
            new_expr.raw_extract = child_expr_data
            new_expr.parent_object = self
            self.children_expressions.append(new_expr)


    def _metacolumn_from_relation(self, p_used_relation: Relation) -> MetadataColumn | None:
        """
        Helper to find a MetadataColumn from a Relation based on 'remote-name' format.
        (e.g., '[TableauRelation].[ColumnName]')
        """
        try:
            # Handle cases like '[ds.name].[column_name]' or just '[column_name]'
            parts = [without_square_brackets(p) for p in self.op.split('.')]
            if len(parts) == 2: # Assumed format: [relation].[column]
                table_relation_name, table_field_name = parts
            elif len(parts) == 1: # Assumed format: [column] (local name in expression context)
                table_relation_name = None # Will try to match directly within p_used_relation's columns
                table_field_name = parts[0]
            else:
                logger.warning(f"Could not parse join expression op '{self.op}' into a valid column reference.")
                return None

        except ValueError:
            logger.warning(f"Could not parse join expression op '{self.op}' into table and field name.")
            return None

        for act_rel in p_used_relation.yield_relations():
            # If table_relation_name is specified, try to match it first
            if table_relation_name and without_square_brackets(act_rel.rel_name) != table_relation_name:
                continue
            
            mc = act_rel.columns.get(table_field_name)
            if mc:
                return mc
        
        logger.warning(f"Metadata column '{table_field_name}' not found for expression '{self.op}'.")
        return None


    def _metacolumn_from_relationship(self, p_relationship: Relationship) -> MetadataColumn | None:
        """
        Helper to find a MetadataColumn from a Relationship, typically using local names.
        """
        # Determine which logical table's relation to check based on index (0 for first, 1 for second)
        parent_rel_obj = None
        if self.index == 0 and p_relationship.first_logical_table:
            parent_rel_obj = p_relationship.first_logical_table.relation
        elif self.index == 1 and p_relationship.second_logical_table:
            parent_rel_obj = p_relationship.second_logical_table.relation

        if not parent_rel_obj:
            logger.warning(f"Parent relation not found for relationship {p_relationship} at index {self.index}.")
            return None

        # Iterate through the relations of the relevant logical table to find the column
        for act_rel in parent_rel_obj.yield_relations():
            mc = act_rel.columns.get(self.op) # Direct lookup by op (local column name)
            if mc:
                return mc
        
        logger.warning(f"Metadata column '{self.op}' not found in relations of {parent_rel_obj}.")
        return None

    @property
    def metacolumn(self) -> MetadataColumn | None:
        """
        Returns the MetadataColumn object that this join expression refers to.
        Traverses up the parent hierarchy to determine context (Relation or Relationship).
        """
        if self.children_expressions: # If it's an operator like 'AND', it doesn't represent a single column
            return None
        if self._metacolumn:
            return self._metacolumn # Return cached result

        parent_obj = self.parent_object
        while parent_obj and not (isinstance(parent_obj, Relation) or isinstance(parent_obj, Relationship)):
            parent_obj = getattr(parent_obj, 'parent_object', None) # Safely get parent_object

        if isinstance(parent_obj, Relation):
            self._metacolumn = self._metacolumn_from_relation(parent_obj)
        elif isinstance(parent_obj, Relationship):
            self._metacolumn = self._metacolumn_from_relationship(parent_obj)
        else:
            logger.error(f"Could not find a valid parent Relation or Relationship for join expression '{self.op}'.")
            self._metacolumn = None
        return self._metacolumn

    def yield_joins(self):
        """Yields tuples of (looker_field1, join_operator, looker_field2) for join conditions."""
        join_conditions = self.children_expressions if self.op == 'AND' else [self]
        for cond_expr in join_conditions:
            if len(cond_expr.children_expressions) == 2:
                mc1 = cond_expr.children_expressions[0].metacolumn
                mc2 = cond_expr.children_expressions[1].metacolumn
                if mc1 and mc2:
                    yield mc1.looker_field, unescape(cond_expr.op), mc2.looker_field
                else:
                    logger.warning(f"Skipping join condition '{cond_expr}' due to missing metadata columns.")
            else:
                logger.warning(f"Invalid join condition format in expression: {cond_expr}. Expected 2 children.")


class Relation:
    """
    Represents a relationship or table definition within a Tableau datasource.
    Can be a single table, a custom SQL (text), a union, or a join.
    """
    rel_name: str | None = None
    rel_type: str = ''
    rel_join: str = '' # For 'join' type relations
    rel_connection: str = ''
    rel_table: str = '' # For 'table' type relations (e.g., '[db].[schema].[table]')
    rel_sql_text: str = '' # For 'text' type relations (custom SQL)
    rel_object: LogicalTable | None = None # The LogicalTable object this relation is part of
    children_relations: list[Relation] = []
    parent_object: Datasource | Connection | Relation # Parent can be Datasource, Connection (for primary relation) or another Relation (for nested joins/unions)
    join_expression: JoinExpression | None = None
    columns: dict[str, MetadataColumn | CalculatedColumn] = {} # Columns associated with this specific relation
    _lookml_view: LookMLView | None = None
    _lookml_explore: LookMLExplore | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the relation and extracts its properties.
        Recursively extracts child relations if it's a collection, join, or union.
        """
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            logger.warning("Raw extract is empty for relation.")
            return

        self.rel_type = p_raw_extract.get('@type', '')
        self.rel_name = p_raw_extract.get('@name')
        self.rel_join = p_raw_extract.get('@join', 'inner')
        self.rel_connection = p_raw_extract.get('@connection', '')
        self.rel_table = p_raw_extract.get('@table', '')
        self.rel_sql_text = p_raw_extract.get('#text', '')

        self.children_relations = []
        self.columns = {} # Initialize columns for this specific relation instance

        if self.rel_type in ('collection', 'join', 'union'):
            # These types can have nested 'relation' elements
            for rel_tag_name in ('relation', '_.fcp.ObjectModelEncapsulateLegacy.true...relation',
                                 '_.fcp.ObjectModelEncapsulateLegacy.false...relation'):
                # Ensure .get() provides a default empty dictionary or list for the relation data
                child_relations_data = p_raw_extract.get(rel_tag_name, [])
                for child_rel_data in iter_tag(child_relations_data):
                    new_rel = Relation()
                    new_rel.raw_extract = child_rel_data
                    new_rel.parent_object = self
                    self.children_relations.append(new_rel)

        if self.rel_type == 'join':
            if clause_expr := p_raw_extract.get('clause', {}).get('expression'):
                self.join_expression = JoinExpression()
                self.join_expression.raw_extract = clause_expr
                self.join_expression.parent_object = self
            else:
                logger.warning(f"Join relation '{self.rel_name}' of type '{self.rel_type}' has no join expression.")


    @property
    def datasource(self) -> Datasource | None:
        """Helper to traverse up the parent chain to find the containing Datasource."""
        act_parent = self.parent_object
        while act_parent and not isinstance(act_parent, Datasource):
            act_parent = getattr(act_parent, 'parent_object', None)
        return act_parent

    @property
    def lookml_view(self) -> LookMLView | None:
        """
        Returns or creates the associated LookMLView object.
        Applies only to 'table', 'text' (custom SQL), and 'union' relations.
        """
        if self.rel_type not in ('table', 'text', 'union'):
            return None # This relation type does not correspond to a direct view

        if self._lookml_view:
            return self._lookml_view

        view_name_base = self.rel_name or os.path.basename(self.rel_table).replace('.', '_') or "unknown_view"
        self._lookml_view = LookMLView(view_name_base)
        
        if self.datasource and self.datasource.lookml_model:
            self.datasource.lookml_model.add_lookml_view(self._lookml_view)
            logger.info(f"Creating LookML view '{self._lookml_view.lookml_name}' from {self}.")
        else:
            logger.error(f"Cannot create LookML view '{self._lookml_view.lookml_name}': No datasource or model found.")
            return None # Critical error, cannot proceed without a model

        if self.rel_type == 'text':
            self._lookml_view.sql = self.rel_sql_text
        elif self.rel_type == 'table':
            if isinstance(self.rel_table, str):
                self._lookml_view.sql_table_items = [without_square_brackets(t) for t in re.findall(r'\[([^[]+)]', self.rel_table)]
            else:
                logger.warning(f"Relation table name is not a string for {self.rel_name}: {self.rel_table}")
                self._lookml_view.sql_table_items = []
        elif self.rel_type == 'union':
            # For unions, currently using the first child table. This is a simplification.
            if self.children_relations and self.children_relations[0].rel_table:
                self._lookml_view.sql_table_name = self.children_relations[0].rel_table
            else:
                logger.warning(f"Union relation '{self.rel_name}' has no valid child table to use for SQL.")
                self._lookml_view.sql_table_name = ""
        
        return self._lookml_view

    @property
    def lookml_explore(self) -> LookMLExplore | None:
        """
        Returns or creates the associated LookMLExplore object for this relation.
        Handles single views or complex join structures.
        """
        if self._lookml_explore:
            return self._lookml_explore

        if self.rel_type not in ('table', 'text', 'union', 'join'):
            return None # This relation type doesn't directly map to an explore

        new_explore = LookMLExplore()
        new_explore.join_sql_on = []

        if self.rel_type in ('table', 'text', 'union'):
            # Simple case: explore directly from a single view
            if self.lookml_view:
                new_explore.first_object = self.lookml_view
                new_explore.logical_table_name = self.lookml_view.lookml_name
            else:
                logger.error(f"Could not get LookML view for relation '{self.rel_name}' of type '{self.rel_type}'.")
                return None
        elif self.rel_type == 'join':
            if len(self.children_relations) < 2:
                logger.warning(f"Join relation '{self.rel_name}' needs at least two child relations, but found {len(self.children_relations)}.")
                return None

            # First object of the join (left side)
            first_child_explore = self.children_relations[0].lookml_explore or self.children_relations[0].lookml_view
            if not first_child_explore:
                logger.error(f"Could not get explore/view for first child of join relation '{self.rel_name}'.")
                return None
            new_explore.first_object = first_child_explore

            # Second object of the join (right side)
            second_child_explore = self.children_relations[1].lookml_explore or self.children_relations[1].lookml_view
            if not second_child_explore:
                logger.error(f"Could not get explore/view for second child of join relation '{self.rel_name}'.")
                return None
            new_explore.second_object = second_child_explore

            new_explore.join_type = Relationship._get_join_type_enum(self.rel_join)
            new_explore.join_relationship = JoinRelationshipEnum.MANY_TO_MANY # Default, infer if possible

            if self.join_expression:
                for mc1_field, join_rel_op, mc2_field in self.join_expression.yield_joins():
                    new_explore.join_sql_on.append((mc1_field, join_rel_op, mc2_field))
            else:
                logger.warning(f"Join relation '{self.rel_name}' has no join expression defined for sql_on clause.")

        self._lookml_explore = new_explore
        if self.datasource and self.datasource.lookml_model:
            self.datasource.lookml_model.add_explore(self._lookml_explore)
            logger.info(f"Generated LookML explore '{self._lookml_explore.lookml_name}' for {self}.")
        else:
            logger.error(f"Cannot add LookML explore '{self._lookml_explore.lookml_name}': No datasource or model found.")
            return None

        return self._lookml_explore


    def add_metacolumn(self, p_metacolumn: MetadataColumn) -> None:
        """
        Adds a MetadataColumn to this relation's column dictionary
        and its corresponding base field to the associated LookMLView.
        """
        # Ensure the column's remote name is used as the key for the columns dict
        if p_metacolumn.remote_name:
            self.columns[p_metacolumn.remote_name] = p_metacolumn
            p_metacolumn.relation = self # Link the metacolumn back to this relation
            
            if self.lookml_view:
                self.lookml_view.add_base_field(p_metacolumn.looker_field)
                logger.debug(f'Adding {p_metacolumn} to {self.lookml_view.lookml_name}.')
            else:
                logger.warning(f"No LookML view associated with relation '{self.rel_name}' to add column '{p_metacolumn.local_name}'.")
        else:
            logger.warning(f"Cannot add metadata column without a remote_name: {p_metacolumn.local_name}")


    def yield_relations(self):
        """Recursively yields this relation and all its children relations."""
        yield self
        for child_rel in self.children_relations:
            yield from child_rel.yield_relations()


    @property
    def base_str(self) -> str:
        """Returns a basic string representation of the relation structure."""
        if not self.children_relations:
            return f'"{self.rel_name or self.rel_table}"'
        if self.rel_name:
            return f'"{self.rel_name}" ({self.rel_type} -- {" -- ".join(x.base_str for x in self.children_relations)})'
        return f'{self.rel_type} ({" -- ".join(x.base_str for x in self.children_relations)})'

    def __str__(self):
        return f'Relation {self.base_str}'


class LogicalTable:
    """
    Represents a 'logical table' in Tableau's object graph, which often points
    to an underlying 'relation' (physical table, custom SQL, join, or union).
    """
    object_id: str | None = None
    object_caption: str = ''
    relation: Relation | None = None # This will be linked to an actual Relation object
    parent_object: ObjectGraph
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the logical table and extracts its properties.
        Links to its corresponding Relation object.
        """
        self._raw_extract = p_raw_extract
        self.object_id = p_raw_extract.get('@id')
        self.object_caption = p_raw_extract.get('@caption', '')
        
        properties_data = p_raw_extract.get('properties')
        if not properties_data:
            logger.warning(f"No properties found for logical table '{self.object_id}'.")
            return

        for act_property_data in iter_tag(properties_data or []): # Ensure iter_tag gets an iterable
            if (object_rel_data := act_property_data.get('relation')):
                # Find the actual Relation object from the datasource's connection
                ds = self.datasource
                if ds and ds.connection:
                    found_relation = False
                    for act_rel in ds.connection.yield_relations():
                        if act_rel.raw_extract == object_rel_data: # Compare raw extracts to find the match
                            self.relation = act_rel
                            act_rel.rel_object = self # Link the relation back to this logical table
                            found_relation = True
                            break
                    if not found_relation:
                        logger.warning(f"No matching relation found in datasource for logical table '{self.object_id}' with relation data: {object_rel_data}")
                else:
                    logger.warning(f"No datasource or connection found for logical table '{self.object_id}' to link relation.")


    @property
    def lookml_explore(self) -> LookMLExplore | None:
        """
        Returns the LookML Explore associated with this logical table.
        This often delegates to the underlying Relation's explore.
        """
        if self.relation and self.relation.lookml_explore:
            return self.relation.lookml_explore
        
        logger.warning(f"No direct LookML explore found for logical table '{self.object_id}' from its relation. Attempting to create a basic one.")
        if self.relation and self.relation.lookml_view:
            new_explore = LookMLExplore()
            new_explore.first_object = self.relation.lookml_view
            new_explore.logical_table_name = self.relation.lookml_view.lookml_name
            
            ds = self.datasource
            if ds and ds.lookml_model:
                ds.lookml_model.add_explore(new_explore) # Add the new explore to the model
            return new_explore
        
        logger.error(f"Could not generate LookML explore for logical table '{self.object_id}'.")
        return None # Indicate failure


    def yield_relations(self):
        """Yields relations associated with this logical table."""
        if self.relation:
            yield from self.relation.yield_relations()

    @property
    def datasource(self) -> Datasource | None:
        """Helper to traverse up the parent chain to find the containing Datasource."""
        act_parent = self.parent_object
        while act_parent and not isinstance(act_parent, Datasource):
            act_parent = getattr(act_parent, 'parent_object', None)
        return act_parent

    def __str__(self):
        return f"{self.__class__.__name__} '{self.object_id}'"


class MetadataColumn:
    """
    Represents a metadata column from Tableau's data source, typically mapping
    to a physical column in the database or a derived column.
    """
    remote_name: str | None = None
    remote_type: str | None = None
    local_name: str | None = None
    parent_name: str | None = None # The name of the relation/table it belongs to
    family: str | None = None
    remote_alias: str | None = None
    local_type: str | None = None # Tableau's inferred data type (string, integer, date, etc.)
    object_id: str | None = None
    logical_table: LogicalTable | None = None # To be linked during object_graph parsing
    relation: Relation | None = None # The specific Relation object this column belongs to
    parent_object: Connection # The connection object it was extracted from
    column_instances: dict[str, ColumnInstance] = {}
    _looker_field: ViewBaseField | None = None
    _raw_extract: dict

    @property
    def name(self):
        return self.local_name or self.remote_name or "unnamed_column"

    @property
    def looker_field(self) -> ViewBaseField:
        """
        Returns or creates the associated LookML ViewBaseField.
        Infers LookML type and timeframes based on Tableau's local type.
        """
        if self._looker_field:
            return self._looker_field
        
        self._looker_field = ViewBaseField(self.remote_name or self.local_name or "unknown_field")
        
        type_mapping = {
            'string': ViewBaseTypeEnum.STRING,
            'integer': ViewBaseTypeEnum.NUMBER,
            'real': ViewBaseTypeEnum.NUMBER,
            'boolean': ViewBaseTypeEnum.YESNO,
            'date': ViewBaseTypeEnum.TIME,
            'datetime': ViewBaseTypeEnum.TIME,
        }
        self._looker_field.type = type_mapping.get(self.local_type, ViewBaseTypeEnum.STRING)

        if self._looker_field.type == ViewBaseTypeEnum.TIME:
            self._looker_field.timeframes = {LookMLTimeframesEnum.RAW, LookMLTimeframesEnum.DATE,
                                             LookMLTimeframesEnum.WEEK, LookMLTimeframesEnum.MONTH,
                                             LookMLTimeframesEnum.QUARTER, LookMLTimeframesEnum.YEAR}
            self._looker_field.datatype = TimeDatatypeEnum.DATE if self.local_type == 'date' else TimeDatatypeEnum.DATETIME
            self._looker_field.lookml_struct_type = LookMLFieldStructEnum.DIMENSION_GROUP # For date dimensions

        self._looker_field.label = self.local_name or self.remote_alias or self.remote_name
        self._looker_field.description = f'Metarecord from Tableau datasource {self.datasource.name if self.datasource else "N/A"}.' \
                                         f' Original parent: {self.parent_name}.'
        return self._looker_field

    @property
    def datasource(self) -> Datasource | None:
        """Helper to traverse up the parent chain to find the containing Datasource."""
        act_parent = self.parent_object
        while act_parent and not isinstance(act_parent, Datasource):
            act_parent = getattr(act_parent, 'parent_object', None)
        return act_parent

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the metadata column and extracts its attributes.
        """
        self._raw_extract = p_raw_extract
        self.column_instances = {} # Initialize for this metadata column
        
        self.remote_name = p_raw_extract.get('remote-name')
        self.remote_type = p_raw_extract.get('remote-type')
        self.local_name = p_raw_extract.get('local-name')
        self.parent_name = p_raw_extract.get('parent-name')
        self.family = p_raw_extract.get('family')
        self.remote_alias = p_raw_extract.get('remote-alias')
        self.local_type = p_raw_extract.get('local-type')
        
        # Handle various keys for object-id
        self.object_id = p_raw_extract.get('object-id') \
                         or p_raw_extract.get('_.fcp.ObjectModelEncapsulateLegacy.true...object-id') \
                         or p_raw_extract.get('_.fcp.ObjectModelEncapsulateLegacy.false...object-id')


    def add_column_instance(self, p_ci_extract: dict):
        """
        Adds a ColumnInstance associated with this MetadataColumn.
        """
        ci_name = p_ci_extract.get('@name', f"instance_{len(self.column_instances)}") # Fallback for unique name
        if ci_name in self.column_instances:
            return self.column_instances[ci_name] # Return existing if already added
        
        new_column_instance = ColumnInstance()
        new_column_instance.parent_object = self
        new_column_instance.raw_extract = p_ci_extract
        self.column_instances[new_column_instance.name] = new_column_instance
        return new_column_instance

    def __hash__(self):
        # Use a tuple of immutable unique identifiers for hashing
        return hash((self.remote_name, self.parent_name, self.local_name))

    def __eq__(self, other):
        if not isinstance(other, MetadataColumn):
            return NotImplemented
        return (self.remote_name == other.remote_name and
                self.parent_name == other.parent_name and
                self.local_name == other.local_name)

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class ColumnInstance:
    """
    Represents a specific usage of a column (metadata or calculated) within a worksheet.
    It can have derivations (e.g., SUM, CountD, Year-Trunc).
    """
    name: str = ''
    derivation: str | None = None # Tableau's aggregation/derivation (e.g., 'Sum', 'Year-Trunc')
    pivot: str | None = None
    type: str | None = None # Tableau's type from column-instance (e.g., 'quantitative', 'ordinal')
    role: str | None = None # Tableau's role (e.g., 'dimension', 'measure')
    _lookml_derived_field: ViewDerivedField | None = None
    parent_object: MetadataColumn | CalculatedColumn | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the column instance and extracts its attributes.
        Triggers the creation of the associated LookML derived field.
        """
        self.name = p_raw_extract.get('@name', '')
        self.derivation = p_raw_extract.get('@derivation')
        self.pivot = p_raw_extract.get('@pivot')
        self.type = p_raw_extract.get('@type')
        self.set_lookml_derived_field()

    @property
    def lookml_derived_field(self):
        """
        Returns or creates the associated LookML ViewDerivedField.
        """
        if self._lookml_derived_field:
            return self._lookml_derived_field
        
        if not self.parent_object or not hasattr(self.parent_object, 'looker_field') or not self.parent_object.looker_field:
            logger.error(f"Cannot create derived field for column instance '{self.name}': Parent base field missing.")
            return None

        new_derived_field = ViewDerivedField(self.name)
        new_derived_field.parent_base_field = self.parent_object.looker_field
        new_derived_field.derivation_str = self.derivation # This setter will attempt to infer type/structure
        new_derived_field.label = self.name # Use the instance name as label

        self._lookml_derived_field = new_derived_field
        return self._lookml_derived_field

    def set_lookml_derived_field(self):
        """Explicitly calls the property to ensure initialization."""
        _ = self.lookml_derived_field

    def __hash__(self):
        return hash((self.name, self.derivation, self.parent_object.name if self.parent_object else None))

    def __eq__(self, other):
        if not isinstance(other, ColumnInstance):
            return NotImplemented
        return (self.name == other.name and
                self.derivation == other.derivation and
                self.parent_object == other.parent_object)

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class CalculatedColumn:
    """
    Placeholder class for Tableau calculated columns.
    (Parsing logic for these is complex and not fully implemented yet.)
    """
    pass


class ObjectGraph:
    """
    Represents the logical object graph within a Tableau datasource,
    defining how logical tables are related (joins, unions).
    This graph is translated into LookML Explores.
    """
    logical_tables_dict: OrderedDict[str, LogicalTable] = OrderedDict()
    relationships: list[Relationship] = []
    parent_object: Datasource
    _lookml_explore: LookMLExplore | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the object graph and triggers parsing
        of logical tables and relationships.
        """
        self._raw_extract = p_raw_extract
        self.logical_tables_dict = OrderedDict()
        self.relationships = []
        if not p_raw_extract:
            logger.warning("Raw extract is empty for object graph.")
            return
        self.extract_logical_tables()
        self.extract_relationships()
        _ = self.lookml_explore # Trigger explore generation


    def gen_lookml_explore(self) -> LookMLExplore | None:
        """
        Generates the top-level LookML Explore(s) from the object graph.
        Handles both single logical tables and complex join structures.
        """
        if self._lookml_explore:
            return self._lookml_explore

        if not self.logical_tables_dict:
            logger.warning("No logical tables found in object graph to generate explore.")
            return None

        if not self.relationships:
            # Case: Single logical table, no joins. Explore is directly from its view.
            _, act_lt = next(iter(self.logical_tables_dict.items()))
            if act_lt.relation and act_lt.relation.lookml_view:
                new_explore = LookMLExplore()
                new_explore.first_object = act_lt.relation.lookml_view
                new_explore.logical_table_name = act_lt.relation.lookml_view.lookml_name
                self._lookml_explore = new_explore
                logger.info(f"Generated simple explore '{new_explore.lookml_name}' for single logical table.")
            else:
                logger.error(f"Could not generate explore for logical table '{act_lt.object_id}': No valid view found.")
                return None
        else:
            # Case: Multiple tables with relationships (joins). Build a chain of explores.
            # Start with the first logical table involved in a relationship
            initial_logical_table = None
            if self.relationships:
                initial_logical_table = self.relationships[0].first_logical_table
            elif self.logical_tables_dict:
                initial_logical_table = next(iter(self.logical_tables_dict.values()))

            if not initial_logical_table:
                logger.error("No starting logical table found for complex explore generation.")
                return None

            main_explore_chain_head = initial_logical_table.lookml_explore
            if not main_explore_chain_head:
                logger.error(f"Failed to get initial explore for logical table '{initial_logical_table.object_id}'.")
                return None

            for act_relationship in self.relationships:
                second_logical_table = act_relationship.second_logical_table
                if not second_logical_table:
                    logger.warning(f"Second logical table not found for relationship {act_relationship.first_endpoint_id} -> {act_relationship.second_endpoint_id}. Skipping this join.")
                    continue

                second_explore_part = second_logical_table.lookml_explore
                if not second_explore_part:
                    logger.warning(f"Could not get explore for second logical table '{second_logical_table.object_id}'. Skipping join.")
                    continue

                new_chained_explore = LookMLExplore()
                new_chained_explore.first_object = main_explore_chain_head
                # The second_object of the explore join should be the View, not another Explore wrapper
                # So we take the first_object of the second_explore_part, which should be the actual view
                new_chained_explore.second_object = second_explore_part.first_object if isinstance(second_explore_part.first_object, LookMLView) else second_explore_part
                
                new_chained_explore.join_type = act_relationship._get_join_type_enum(act_relationship.rel_join)
                new_chained_explore.join_relationship = JoinRelationshipEnum.MANY_TO_MANY # Default, infer if possible
                
                # Populate join_sql_on from the relationship's join expression
                if act_relationship.join_expression:
                    for mc1_field, join_rel_op, mc2_field in act_relationship.join_expression.yield_joins():
                        new_chained_explore.join_sql_on.append((mc1_field, join_rel_op, mc2_field))
                else:
                    logger.warning(f"Relationship between {act_relationship.first_endpoint_id} and {act_relationship.second_endpoint_id} has no join expression.")

                main_explore_chain_head = new_chained_explore

            self._lookml_explore = main_explore_chain_head

        # Add the final constructed explore to the model
        if self._lookml_explore and self.parent_object.lookml_model:
            self.parent_object.lookml_model.add_explore(self._lookml_explore)
            logger.info(f"Final explore '{self._lookml_explore.lookml_name}' added to model.")
        else:
            logger.error(f"Cannot add final LookML explore to model for object graph.")
            return None

        return self._lookml_explore


    @property
    def lookml_explore(self) -> LookMLExplore | None:
        """Accessor for the generated LookML explore."""
        if self._lookml_explore:
            return self._lookml_explore
        return self.gen_lookml_explore() # Generate if not already present

    def extract_logical_tables(self):
        """Extracts logical table objects from the raw object graph data."""
        # Ensure .get() provides a default empty dictionary or list for 'object'
        objects_data = self.raw_extract.get('objects', {}).get('object', [])
        self.logical_tables_dict = OrderedDict() # Ensure it's empty before populating
        for object_data in iter_tag(objects_data):
            new_object = LogicalTable()
            new_object.parent_object = self
            new_object.raw_extract = object_data
            if new_object.object_id:
                self.logical_tables_dict[new_object.object_id] = new_object
            else:
                logger.warning(f"Logical table found without an ID: {object_data}")


    def extract_relationships(self):
        """Extracts relationship objects from the raw object graph data."""
        # Ensure .get() provides a default empty dictionary or list for 'relationship'
        relationships_data = self.raw_extract.get('relationships', {}).get('relationship', [])
        self.relationships = [] # Ensure it's empty before populating
        for relationship_data in iter_tag(relationships_data):
            new_relationship = Relationship()
            new_relationship.parent_object = self
            new_relationship.raw_extract = relationship_data
            self.relationships.append(new_relationship)


class Relationship:
    """
    Represents a relationship between two logical tables in Tableau's object graph.
    This corresponds to a join in LookML.
    """
    join_expression: JoinExpression | None = None
    first_endpoint_id: str | None = None
    second_endpoint_id: str | None = None
    parent_object: ObjectGraph
    _raw_extract: dict
    rel_join: str = 'inner' # Default join type string from Tableau

    @property
    def first_logical_table(self) -> LogicalTable | None:
        """Returns the first logical table involved in the relationship."""
        return self.parent_object.logical_tables_dict.get(self.first_endpoint_id)

    @property
    def second_logical_table(self) -> LogicalTable | None:
        """Returns the second logical table involved in the relationship."""
        return self.parent_object.logical_tables_dict.get(self.second_endpoint_id)

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        """
        Sets the raw dictionary for the relationship and extracts its attributes.
        Triggers the extraction of the join expression.
        """
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            logger.warning("Raw extract is empty for relationship.")
            return
        self.first_endpoint_id = p_raw_extract.get('first-end-point', {}).get('@object-id')
        self.second_endpoint_id = p_raw_extract.get('second-end-point', {}).get('@object-id')
        self.rel_join = p_raw_extract.get('@join', 'inner')
        self.extract_join_expression()

    def extract_join_expression(self):
        """Extracts the join expression defining the relationship."""
        join_expression_data = self.raw_extract.get('expression')
        if join_expression_data:
            self.join_expression = JoinExpression()
            self.join_expression.parent_object = self
            self.join_expression.raw_extract = join_expression_data
        else:
            logger.warning(f"Relationship between {self.first_endpoint_id} and {self.second_endpoint_id} has no join expression.")

    def _get_join_type_enum(self, join_str: str) -> JoinTypeEnum:
        """
        Converts Tableau's string join type to LookML's JoinTypeEnum.
        Handles Tableau's 'right' join by converting to 'full' for broader LookML compatibility.
        """
        mapping = {
            'inner': JoinTypeEnum.INNER,
            'left': JoinTypeEnum.LEFT_OUTER,
            'right': JoinTypeEnum.FULL_OUTER, # Tableau right joins can often be represented as full in LookML for broader compatibility
            'full': JoinTypeEnum.FULL_OUTER
        }
        return mapping.get(join_str, JoinTypeEnum.LEFT_OUTER) # Default to LEFT_OUTER


class PyhisicalTable:
    """Placeholder for a physical table object."""
    pass


class LogicalColumn:
    """Placeholder for a logical column object."""
    pass


class PhysicalColumn:
    """Placeholder for a physical column object."""
    pass


class TableConnection:
    """Placeholder for a table connection object."""
    pass


def main():
    pass


if __name__ == '__main__':
    main()

