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
    LookMLDashboardElementTypeEnum, LookMLFieldStructEnum
from collections import OrderedDict
from html import unescape
import re


def without_square_brackets(p_text: str) -> str:
    """
    Remove square brackets (if exists) from the string (e.g. '[word]' -> 'word')
    :param p_text: text to be processed
    :return: string without squared brackets
    """
    if p_text and p_text[0] == '[' and p_text[-1] == ']':
        return p_text[1:-1]
    return p_text


def iter_tag(p_tag_value: dict | list):
    """
    Helper function to iterate over a tag's value which might be a single dictionary
    or a list of dictionaries.
    """
    if isinstance(p_tag_value, dict):
        yield p_tag_value
    elif isinstance(p_tag_value, list):
        for act_val in p_tag_value:
            yield act_val
    else:
        # Handle cases where p_tag_value might be None or an unexpected type
        # In this context, we'll just yield nothing.
        pass


class Workbook:
    """
    workbook
    """
    _file_path: str
    tableau_workbook_name: str = ''
    parameter_table: ParameterTable
    datasources: dict[str, Datasource]
    worksheets: dict[str, Worksheet]
    _lookml_project: LookMLProject | None = None
    _raw_extract: dict
    _deployment_folder: str # New attribute to store the target deployment folder

    def __init__(self, p_deployment_folder: str = 'lookml_files'): # Modified init with default
        self._deployment_folder = p_deployment_folder

    @property
    def file_full_path(self):
        return self._file_path

    @file_full_path.setter
    def file_full_path(self, p_file_path: str):
        self._file_path = p_file_path
        logger.info(f'Loading extract from {p_file_path}.')
        self.tableau_workbook_name, self.raw_extract = load_tableau_extract_to_dict(p_file_path=p_file_path)

    @property
    def file_name(self):
        return os.path.basename(self._file_path)

    @property
    def file_name_wo_ext(self):
        return '.'.join(self.file_name.split('.')[:-1])

    @property
    def lookml_project(self) -> LookMLProject:
        if self._lookml_project:
            return self._lookml_project
        self._lookml_project = LookMLProject()
        self._lookml_project.name_orig = self.tableau_workbook_name
        self._lookml_project.deployment_folder = self._deployment_folder # Set deployment folder here
        return self._lookml_project

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        if not self.raw_extract:
            return
        self.extract_datasources()
        self.extract_worksheets()

    def extract_datasources(self):
        ds_part = self._raw_extract.get('workbook', {}).get('datasources', {}).get('datasource', {})
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
            self.datasources[new_datasource.name] = new_datasource

    def extract_worksheets(self):
        ds_part = self._raw_extract.get('workbook', {}).get('worksheets', {}).get('worksheet', {})
        self.worksheets = {}
        for ds_item in iter_tag(ds_part):
            new_worksheet = Worksheet()
            new_worksheet.parent_object = self
            new_worksheet.raw_extract = ds_item
            self.worksheets[new_worksheet.name] = new_worksheet


class Datasource:
    """
    workbook / datasources / datasource
    """
    name: str
    caption: str
    connection: Connection | None
    parent_object: Workbook
    object_graph: ObjectGraph | None = None
    # object_dict: dict[str, LogicalTable] | None
    _lookml_model: LookMLModel | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @property
    def lookml_model(self) -> LookMLModel:
        return self._set_lookml_model()

    def _set_lookml_model(self) -> LookMLModel:
        if self._lookml_model is None:
            self._lookml_model = LookMLModel()
            if self.caption:
                self._lookml_model.name_orig = self.caption
            else:
                self._lookml_model.name_orig = self.name
            act_project = self.parent_object.lookml_project
            act_project.add_lookml_model(self._lookml_model)
        return self._lookml_model

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name')
        self.caption = p_raw_extract.get('@caption')
        logger.info(f'Parsing {self}.')
        if not p_raw_extract:
            return
        self.extract_connection()
        self.extract_object_graph()
        # self.connection.extract_metadata_columns()
        self._set_lookml_model()

    def extract_connection(self):
        if conn_data := self.raw_extract.get('connection', {}):
            self.connection = Connection()
            self.connection.parent_object = self
            self.connection.raw_extract = conn_data

    def extract_object_graph(self):
        for object_graph_text in ('object-graph', '_.fcp.ObjectModelEncapsulateLegacy.true...object-graph',
                                  '_.fcp.ObjectModelEncapsulateLegacy.false...object-graph'):
            if object_data := self.raw_extract.get(object_graph_text, {}):
                self.object_graph = ObjectGraph()
                self.object_graph.parent_object = self
                self.object_graph.raw_extract = object_data
                break

    def __str__(self):
        if self.caption:
            return f"{self.__class__.__name__} '{self.caption}'"
        return f"{self.__class__.__name__} '{self.name}'"

    def __repr__(self):
        if self.caption:
            return f"Tableau Datasource '{self.name}' ({self.caption})"
        return f"Tableau Datasource '{self.name}'"

    def yield_relations(self):
        if not self.connection:
            return
        for act_rel in self.connection.yield_relations():
            yield act_rel


class Worksheet:
    """
    workbook / worksheets / worksheet
    """
    name: str
    parent_object: Workbook
    used_column_instances: dict[str, ColumnInstance]
    titles: list[dict]
    panes: list[WorksheetPane]
    rows: list[ColumnInstance]
    cols: list[ColumnInstance]
    pane_texts: list[ColumnInstance]
    pane_wedge_sizes: list[ColumnInstance]
    pane_colors: list[ColumnInstance]
    _lookml_dashboardelement: DashboardElement | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name')
        logger.info(f'Parsing {self}.')
        if not p_raw_extract:
            return
        # self.extract_columns()
        self.extract_titles()
        self.extract_column_instances()
        self.set_rows()
        self.set_cols()
        self.extract_pane()
        self._set_lookml_dashboardelement()

    @property
    def lookml_dashboardelement(self) -> DashboardElement:
        return self._set_lookml_dashboardelement()

    def _set_lookml_dashboardelement(self) -> DashboardElement:
        if self._lookml_dashboardelement:
            return self._lookml_dashboardelement
        new_dashboardelement = DashboardElement()
        if self.titles:
            title_string_list = []
            for act_part in self.titles:
                if act_part['#text'] == 'Æ':
                    # title_string_list.append('\n')
                    continue
                if title_string_list and title_string_list[-1] != '\n':
                    title_string_list.append(' ')
                if act_part['#text'] == '<Sheet Name>':
                    title_string_list.append(self.name)
                else:
                    title_string_list.append(act_part['#text'])
            new_dashboardelement.title = '"' + "".join(title_string_list).replace('"', "'") + '"'

        # TODO - specify proper mapping
        dashboard_type_mapping = {  # 'Automatic': LookMLDashboardElementTypeEnum.TABLE,
                                  'Text': LookMLDashboardElementTypeEnum.TABLE,
                                  'Bar': LookMLDashboardElementTypeEnum.LOOKER_COLUMN,
                                  'Line': LookMLDashboardElementTypeEnum.LOOKER_LINE,
                                  'Square': LookMLDashboardElementTypeEnum.TABLE,  # heatmap
                                  'Area': LookMLDashboardElementTypeEnum.LOOKER_AREA,
                                  'GanttBar': None,
                                  'Pie': LookMLDashboardElementTypeEnum.LOOKER_PIE,
                                  'Multipolygon': None,
                                  'Shape': LookMLDashboardElementTypeEnum.LOOKER_SCATTER}
        new_dashboardelement.type = dashboard_type_mapping.get(self.panes[0].mark_class)
        if self.panes[0].mark_class == 'Automatic':  # new_dashboardelement.type == 'Automatic':
            # Visualization has measures in each axis -> scatter plot
            if any(iter(f for f in self.rows if f.role == 'measure')) \
                    and any(iter(f for f in self.cols if f.role == 'measure')):
                new_dashboardelement.type = LookMLDashboardElementTypeEnum.LOOKER_SCATTER
            # Visualization has one continueous dimension + a measures on another axis -> line type
            elif all(iter((f.role == 'dimension' and f.type in ('ordinal', 'quantitative')) for f in self.rows)) \
                    and any(iter(f for f in self.cols if f.role == 'measure')):
                new_dashboardelement.type = LookMLDashboardElementTypeEnum.LOOKER_LINE
            # Visualization has one continueous dimension + a measures on another axis -> line type
            elif all(iter((f.role == 'dimension' and f.type in ('ordinal', 'quantitative')) for f in self.cols)) \
                    and any(iter(f for f in self.rows if f.role == 'measure')):
                new_dashboardelement.type = LookMLDashboardElementTypeEnum.LOOKER_LINE
            # Visualization has measure in cols -> bar type
            elif any(iter((True for f in self.cols if f.role == 'measure'))):
                new_dashboardelement.type = LookMLDashboardElementTypeEnum.LOOKER_BAR
            # Visualization has measure in rows -> column type
            elif any(iter((True for f in self.rows if f.role == 'measure'))):
                new_dashboardelement.type = LookMLDashboardElementTypeEnum.LOOKER_COLUMN
            elif not self.rows and not self.cols and len(self.pane_texts) == 1 and self.pane_texts[0].role == 'measure':
                new_dashboardelement.type = LookMLDashboardElementTypeEnum.SINGLE_VALUE
            else:
                new_dashboardelement.type = LookMLDashboardElementTypeEnum.TABLE
                ...
            logger.info(f"Automatic type is converted to LookML dashboard type '{new_dashboardelement.type}'.")

        new_dashboardelement.element_name_orig = self.name
        self.parent_object.lookml_project.add_lookml_dashboardelement(new_dashboardelement)

        ###### TEST DASHBOARD ######

        if not self.parent_object.lookml_project.lookml_dashboards:
            test_dashboard = Dashboard()
            test_dashboard.name_orig = f'test_dashboard {self.parent_object.tableau_workbook_name}'
            test_dashboard.title = f'Test dashboard from Tableau wb {self.parent_object.tableau_workbook_name}'
            self.parent_object.lookml_project.add_lookml_dashboard(test_dashboard)
        else:
            _, test_dashboard = next(iter(self.parent_object.lookml_project.lookml_dashboards.items()))
        test_dashboard.lookml_dashboard_elements[new_dashboardelement.lookml_name] = new_dashboardelement

        ###### TEST DASHBOARD ######

        # new_dashboardelement.dimensions = [ci.lookml_derived_field for ci in chain(self.rows, self.cols)
        #                                    if not ci.lookml_derived_field.type]
        # new_dashboardelement.measures = [ci.lookml_derived_field for ci in chain(self.rows, self.cols)
        #                                  if ci.lookml_derived_field.type]

        new_dashboardelement.fields = []
        for ci in chain(self.rows, self.cols, self.pane_texts, self.pane_wedge_sizes, self.pane_colors):
            if ci.lookml_derived_field in new_dashboardelement.fields:
                continue
            new_dashboardelement.fields.append(ci.lookml_derived_field)
        if new_dashboardelement.type == LookMLDashboardElementTypeEnum.LOOKER_COLUMN:
            new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.cols[1:]]
        elif new_dashboardelement.type == LookMLDashboardElementTypeEnum.LOOKER_BAR:
            new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.rows[1:]]
        elif new_dashboardelement.type == LookMLDashboardElementTypeEnum.TABLE:
            new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.cols]
        # elif new_dashboardelement.type == LookMLDashboardElementTypeEnum.LOOKER_PIE:
        #     new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.cols]
        elif new_dashboardelement.type == LookMLDashboardElementTypeEnum.LOOKER_LINE:
            new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.pane_colors]
        else:
            new_dashboardelement.pivots = [ci.lookml_derived_field for ci in self.cols]
        try:
            rel_field = next(iter(chain(self.rows, self.cols, self.pane_wedge_sizes, self.pane_texts,
                                        self.pane_colors)))
            new_dashboardelement.lookml_model = rel_field.parent_object.parent_object.parent_object.lookml_model
            new_dashboardelement.lookml_explore = rel_field.parent_object.parent_object.parent_object.object_graph.lookml_explore
        except StopIteration:
            logger.warning('No cols and rows for vizualization, no dashboard element is generated.')

    def extract_titles(self):
        self.titles = []
        ds_title_parts_data = self.raw_extract.get('layout-options', {}).get('title', {}).get('formatted-text', {})\
            .get('run')
        if isinstance(ds_title_parts_data, str):
            self.titles.append({'#text': ds_title_parts_data})
        else:
            for ds_title_part in iter_tag(ds_title_parts_data):
                if isinstance(ds_title_part, str):
                    self.titles.append({'#text': ds_title_part})
                else:
                    self.titles.append(ds_title_part)

    def extract_column_instances(self):
        self.used_column_instances = {}
        ds_dependencies = self.raw_extract.get('table', {}).get('view', {}).get('datasource-dependencies')
        for act_ds_dependency in iter_tag(ds_dependencies):
            act_ds = self.parent_object.datasources[act_ds_dependency.get('@datasource')]
            column_role = {}
            for act_column_data in iter_tag(act_ds_dependency.get('column', {})):
                column_role[act_column_data.get('@name')] = act_column_data.get('@role')
            for act_column_instance_data in iter_tag(act_ds_dependency.get('column-instance', {})):
                act_column_name = without_square_brackets(act_column_instance_data.get('@column'))
                for act_relation in act_ds.yield_relations():
                    if act_column_name in act_relation.columns:
                        new_ci = act_relation.columns[act_column_name].add_column_instance(act_column_instance_data)
                        new_ci.role = column_role.get(act_column_instance_data.get('@column'))
                        self.used_column_instances[f'[{act_ds.name}].{new_ci.name}'] = new_ci
                        break
                else:
                    logger.warning(f'Column instance root is not found for {act_column_instance_data.get("@column")}')

    def extract_pane(self):
        self.panes = []
        self.pane_texts = []
        self.pane_wedge_sizes = []
        self.pane_colors = []
        for act_pane_data in iter_tag(self.raw_extract.get('table', {}).get('panes', {}).get('pane', {})):
            new_pane = WorksheetPane()
            new_pane.parent_object = self
            new_pane.raw_extract = act_pane_data
            self.panes.append(new_pane)
            for pane_text_column in new_pane.encodings.get('text', []):
                self.pane_texts.append(self.used_column_instances[pane_text_column])
            for pane_wedge_size_column in new_pane.encodings.get('wedge-size', []):
                self.pane_wedge_sizes.append(self.used_column_instances[pane_wedge_size_column])
            for pane_color_column in new_pane.encodings.get('color', []):
                self.pane_colors.append(self.used_column_instances[pane_color_column])

    def set_rows(self):
        self.rows = []
        rows_data = self.raw_extract.get('table', {}).get('rows', '')
        if not rows_data:
            logger.info(f'{self} - no rows found.')
            return
        for act_ci_match in re.finditer(r'\[[^[]+]\.\[[^[]+]', rows_data):
            ci_key = act_ci_match.group(0)
            if ci_key in self.used_column_instances:
                ci = self.used_column_instances[ci_key]
                self.rows.append(ci)
                logger.info(f'Adding {ci} to {self} rows.')
            else:
                logger.warning(f'Column instance key not found in used_column_instances: {ci_key}')

    def set_cols(self):
        self.cols = []
        cols_data = self.raw_extract.get('table', {}).get('cols', '')
        if not cols_data:
            logger.info(f'{self} - no cols found.')
            return
        for act_ci_match in re.finditer(r'\[[^[]+]\.\[[^[]+]', cols_data):
            ci_key = act_ci_match.group(0)
            if ci_key in self.used_column_instances:
                ci = self.used_column_instances[ci_key]
                self.cols.append(ci)
                logger.info(f'Adding {ci} to {self} cols.')
            else:
                logger.warning(f'Column instance key not found in used_column_instances: {ci_key}')


    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class WorksheetPane:
    """
        workbook / worksheets / worksheet / table / panes / pane
        """
    id: str = ''
    mark_class: str = ''
    encodings: dict[str, list[str]] | None = None
    parent_object: Worksheet
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.id = p_raw_extract.get('@id')
        self.mark_class = p_raw_extract.get('mark', {}).get('@class')
        logger.info(f"{self.parent_object} mark class is '{self.mark_class}'")
        self.encodings = {}
        for act_encoding_type, act_encoding_data in self.raw_extract.get('encodings', {}).items():
            for act_enc_detail_data in iter_tag(act_encoding_data):
                self.encodings.setdefault(act_encoding_type, []).append(act_enc_detail_data['@column'])


class ParameterTable:
    name: str
    parameter_fields: dict[str, ParameterField]
    parent_object: Workbook
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name')
        logger.info(f'Parsing {self}.')
        self.parameter_fields = {}
        parameter_fields_extract = p_raw_extract.get('column')
        param_fields_iter = []
        if isinstance(parameter_fields_extract, dict):
            param_fields_iter = [parameter_fields_extract]
        elif isinstance(parameter_fields_extract, list):
            param_fields_iter = parameter_fields_extract
        for param_field_data in param_fields_iter:
            new_param_field = ParameterField()
            new_param_field.raw_extract = param_field_data
            new_param_field.parent_object = self
            self.parameter_fields[new_param_field.name] = new_param_field

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class ParameterField:
    name: str
    caption: str
    datatype: str
    param_domain_type: str
    role: str
    type: str
    default_format: str
    value: str
    parent_object: Workbook
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name')
        self.caption = p_raw_extract.get('@caption')
        self.datatype = p_raw_extract.get('@datatype')
        self.param_domain_type = p_raw_extract.get('@param-domain-type')
        self.role = p_raw_extract.get('@role')
        self.type = p_raw_extract.get('@type')
        self.default_format = p_raw_extract.get('@default-format')
        self.value = p_raw_extract.get('@value')

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class Connection:
    """
    workbook / datasources / datasource / connection (/ named-connections / named-connection / connection)
    """
    conn_class: str
    conn_dialect: str | None
    conn_dbname: str | None
    conn_server: str | None
    conn_port: str | None
    conn_username: str | None
    conn_child_named_connections: dict[str, NamedConnection] | None
    relation: Relation | None = None
    parent_object: Datasource | NamedConnection
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            return
        self.conn_class = p_raw_extract.get('@class')
        self.conn_dialect = p_raw_extract.get('@connection-dialect')
        self.conn_dbname = p_raw_extract.get('@dbname')
        self.conn_server = p_raw_extract.get('@server')
        self.conn_port = p_raw_extract.get('@port')
        self.conn_username = p_raw_extract.get('@username')
        self.conn_child_named_connections = {}
        if self.conn_class == 'federated' and \
                (child_named_conns := p_raw_extract.get('named-connections', {}).get('named-connection')):
            child_named_conns_iter = []
            if isinstance(child_named_conns, list):
                child_named_conns_iter = child_named_conns
            elif isinstance(child_named_conns, dict):
                child_named_conns_iter = [child_named_conns]
            for ch_named_conn_item in child_named_conns_iter:
                new_nc = NamedConnection()
                new_nc.raw_extract = ch_named_conn_item
                new_nc.parent_object = self
                self.conn_child_named_connections[new_nc.conn_name] = new_nc
        self.extract_relations()
        self.extract_metadata_columns()

    def extract_relations(self):
        for rel_text in ('relation', '_.fcp.ObjectModelEncapsulateLegacy.true...relation',
                         '_.fcp.ObjectModelEncapsulateLegacy.false...relation'):
            if ds_relation := self.raw_extract.get(rel_text, {}):
                self.relation = Relation()
                self.relation.raw_extract = ds_relation
                self.relation.parent_object = self
                break

    def extract_metadata_columns(self):
        for act_meta_record_item in iter_tag(self.raw_extract.get('metadata-records', {}).get('metadata-record')):
            if act_meta_record_item.get('@class', '') == 'column':
                new_meta_column = MetadataColumn()
                new_meta_column.parent_object = self
                new_meta_column.raw_extract = act_meta_record_item
                if self.relation: # Ensure relation exists before adding metadata columns to it
                    self.relation.add_metacolumn(new_meta_column)
                else:
                    logger.warning(f"Metadata column '{new_meta_column.local_name}' found but no relation to attach it to.")


    def yield_relations(self):
        if self.relation:
            for chield_rel in self.relation.yield_relations():
                yield chield_rel


class NamedConnection:
    """
    workbook / datasources / datasource / connection / named-connections / named-connection
    """
    conn_name: str | None
    conn_caption: str | None
    conn_object: Connection
    parent_object: Datasource
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            return
        self.conn_name = p_raw_extract.get('@name')
        self.conn_caption = p_raw_extract.get('@caption')
        self.conn_object = Connection()
        self.conn_object.raw_extract = p_raw_extract.get('connection', {})
        self.conn_object.parent_object = self

    def __repr__(self):
        if self.conn_caption:
            return f"Tableau Named Connection '{self.conn_name}' ({self.conn_caption})"
        return f"Tableau Named Connection '{self.conn_name}'"


class JoinExpression:
    op: str
    index: int = 0
    children_expressions: list[JoinExpression]
    parent_object: JoinExpression | Relation | Relationship
    _metacolumn: MetadataColumn | None = None
    _raw_extract: dict

    def __str__(self):
        if not self.children_expressions:
            return self.op
        return f"({(' ' + unescape(self.op) + ' ').join(iter(str(ce) for ce in self.children_expressions))})"

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            return
        self.op = p_raw_extract.get('@op')
        self.children_expressions = []
        expression_data = p_raw_extract.get('expression')
        if isinstance(expression_data, dict):
            expression_data = [expression_data] # Ensure it's always a list for iteration
        for i, child_expr_data in enumerate(expression_data or []):
            new_exr = JoinExpression()
            new_exr.index = i
            new_exr.raw_extract = child_expr_data
            new_exr.parent_object = self
            self.children_expressions.append(new_exr)

    def _metacolumn_from_relation(self, p_used_relation: Relation) -> MetadataColumn | None:
        # join by remote name
        try:
            table_relation_name, table_field_name = map(without_square_brackets, self.op.split('.'))
        except ValueError:
            logger.warning(f"Could not parse join expression op '{self.op}' into table and field name.")
            return None

        for act_rel in p_used_relation.yield_relations():
            if act_rel.rel_name == table_relation_name:
                self._metacolumn = act_rel.columns.get(table_field_name)
                if not self._metacolumn:
                    logger.warning(f"Metadata column '{table_field_name}' not found in relation '{table_relation_name}'.")
                return self._metacolumn
        logger.warning(f"Relation '{table_relation_name}' not found for join expression op '{self.op}'.")
        return None


    def _metacolumn_from_relationship(self, p_relationship: Relationship) -> MetadataColumn | None:
        # join by local name
        if self.index == 0:
            parent_rel = p_relationship.first_logical_table.relation
        else:
            parent_rel = p_relationship.second_logical_table.relation

        if not parent_rel:
            logger.warning(f"Parent relation not found for relationship {p_relationship}.")
            return None

        for act_rel in parent_rel.yield_relations():
            for mc in act_rel.columns.values():
                if mc.name == self.op:
                    self._metacolumn = mc
                    return self._metacolumn
        logger.warning(f"Metadata column '{self.op}' not found in relations of {parent_rel}.")
        return None

    @property
    def metacolumn(self):
        if self.children_expressions:
            return None
        if not self._metacolumn:
            parent_obj = self.parent_object
            # Traverse up the hierarchy to find the relevant Relation or Relationship object
            while parent_obj and not (isinstance(parent_obj, Relation) or isinstance(parent_obj, Relationship)):
                parent_obj = parent_obj.parent_object

            if isinstance(parent_obj, Relation):
                self._metacolumn = self._metacolumn_from_relation(parent_obj)
            elif isinstance(parent_obj, Relationship):
                self._metacolumn = self._metacolumn_from_relationship(parent_obj)
            else:
                logger.error(f"Could not find a valid parent Relation or Relationship for join expression '{self.op}'.")
                self._metacolumn = None # Ensure it's explicitly None on failure
        return self._metacolumn

    def yield_joins(self):
        if self.op == 'AND':
            join_rel_list = self.children_expressions
        else:
            join_rel_list = [self]
        for act_join_rel in join_rel_list:
            mc1 = act_join_rel.children_expressions[0].metacolumn
            mc2 = act_join_rel.children_expressions[1].metacolumn
            if mc1 and mc2: # Only yield if both metacolumns are successfully found
                yield mc1.looker_field, unescape(act_join_rel.op), mc2.looker_field
            else:
                logger.warning(f"Skipping join due to missing metacolumns in expression: {act_join_rel}")


class Relation:
    rel_name: str | None
    rel_type: str
    rel_join: str
    rel_connection: str
    rel_table: str
    rel_sql_text: str
    rel_object: LogicalTable | None
    children_relations: list[Relation]
    parent_object: Datasource | Relation
    join_expression: JoinExpression | None = None
    columns: dict[str, MetadataColumn | CalculatedColumn]
    _lookml_view: LookMLView | None = None
    _lookml_explore: LookMLExplore | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            return
        self.rel_type = p_raw_extract.get('@type')
        self.rel_name = p_raw_extract.get('@name')
        self.rel_join = p_raw_extract.get('@join')
        self.rel_connection = p_raw_extract.get('@connection')
        self.rel_table = p_raw_extract.get('@table')
        self.rel_sql_text = p_raw_extract.get('#text')
        self.children_relations = []
        self.columns = {} # Initialize columns here
        if self.rel_type in ('collection', 'join', 'union'):
            for rel_text in ('relation',
                             '_.fcp.ObjectModelEncapsulateLegacy.true...relation',
                             '_.fcp.ObjectModelEncapsulateLegacy.false...relation'):
                if not p_raw_extract.get(rel_text):
                    continue
                # Ensure it's iterable even if it's a single dict
                child_relations_data = p_raw_extract.get(rel_text)
                if isinstance(child_relations_data, dict):
                    child_relations_data = [child_relations_data]

                for child_rel_data in child_relations_data:
                    new_rel = Relation()
                    new_rel.raw_extract = child_rel_data
                    new_rel.parent_object = self
                    self.children_relations.append(new_rel)
        if self.rel_type == 'join':
            self.join_expression = JoinExpression()
            self.join_expression.raw_extract = p_raw_extract.get('clause', {}).get('expression', {})
            self.join_expression.parent_object = self

    @property
    def datasource(self) -> Datasource | None:
        act_parent = self.parent_object
        while act_parent and not isinstance(act_parent, Datasource):
            act_parent = act_parent.parent_object
        return act_parent

    @property
    def lookml_view(self) -> LookMLView | None:
        if self.rel_type not in ('table', 'text', 'union'):
            return None
        if not self._lookml_view:
            self._lookml_view = LookMLView(self.rel_name or "unknown_view") # Provide a fallback name
            self.datasource.lookml_model.add_lookml_view(self._lookml_view)
            logger.info(f"Creating LookML view '{self._lookml_view.lookml_name}' from {self}.")
            if self.rel_type == 'text':
                self._lookml_view.sql = self.rel_sql_text
            elif self.rel_type == 'table':
                # Ensure rel_table is a string before regex
                if isinstance(self.rel_table, str):
                    self._lookml_view.sql_table_items = [t.group(1) for t in re.finditer(r'\[([^[]+)]', self.rel_table)]
                else:
                    logger.warning(f"Relation table name is not a string for {self.rel_name}: {self.rel_table}")
                    self._lookml_view.sql_table_items = []
            elif self.rel_type == 'union':
                # TODO - handle Tableau unions in logical tables - currently the first table is used only
                if self.children_relations and isinstance(self.children_relations[0].rel_table, str):
                    self._lookml_view.sql_table_name = self.children_relations[0].rel_table
                else:
                    logger.warning(f"Union relation has no valid child table for {self.rel_name}.")
                    self._lookml_view.sql_table_name = "" # Fallback
            else:
                # This path should ideally not be reached if rel_type is validated above
                raise NotImplementedError(f"LookML view generation not implemented for relation type: {self.rel_type}")

        return self._lookml_view

    @property
    def lookml_explore(self) -> LookMLExplore | None:
        if self._lookml_explore:
            return self._lookml_explore
        logger.info(f'Generating LookML explore to {self}.')
        if self.rel_type not in ('table', 'text', 'union', 'join'):
            return None
        self._lookml_explore = LookMLExplore()
        self._lookml_explore.join_sql_on = []
        if self.rel_type in ('table', 'text', 'union'):
            self._lookml_explore.first_object = self.lookml_view
            # For single tables/views, the explore name is usually derived from the view name
            self._lookml_explore.logical_table_name = self.lookml_view.lookml_name # Set logical_table_name for simple explores
        elif self.rel_type == 'join':
            if self.children_relations:
                if self.children_relations[0].rel_type in ('table', 'text', 'union'):
                    self._lookml_explore.first_object = self.children_relations[0].lookml_view
                else:
                    self._lookml_explore.first_object = self.children_relations[0].lookml_explore

                if len(self.children_relations) > 1 and self.children_relations[1].rel_type in ('table', 'text', 'union'):
                    self._lookml_explore.second_object = self.children_relations[1].lookml_view
                else:
                    # Handle cases where second object is not a simple view or is missing
                    logger.warning(f"Second object for join relation '{self.rel_name}' is not a simple view or is missing. Type: {self.children_relations[1].rel_type if len(self.children_relations) > 1 else 'N/A'}")
                    self._lookml_explore.second_object = None # Ensure it's None

                if self.rel_join == 'right':
                    logger.warning('Right join in relation - converted to full join.')
                self._lookml_explore.join_type = {'inner': JoinTypeEnum.INNER,
                                                  'left': JoinTypeEnum.LEFT_OUTER,
                                                  'right': JoinTypeEnum.FULL_OUTER, # Tableau right joins can often be represented as full in LookML for broader compatibility
                                                  'full': JoinTypeEnum.FULL_OUTER}[self.rel_join]
                # Populate join_sql_on
                if self.join_expression:
                    for mc1, join_rel, mc2 in self.join_expression.yield_joins():
                        if mc1 and mc2: # Only append if both metacolumns were found
                            self._lookml_explore.join_sql_on.append((mc1.looker_field, join_rel, mc2.looker_field))
                        else:
                            logger.warning(f"Skipping part of join_sql_on for {self.rel_name} due to missing metadata columns.")
                else:
                    logger.warning(f"Join relation '{self.rel_name}' has no join expression.")
            else:
                logger.warning(f"Join relation '{self.rel_name}' has no child relations.")
                return None # Cannot create explore without child relations
        return self._lookml_explore

    def add_metacolumn(self, p_metacolumn: MetadataColumn) -> None:
        used_relation_name = without_square_brackets(p_metacolumn.parent_name)
        # Iterate through self and its children to find the correct relation to add the column to
        for act_relation in self.yield_relations():
            # Check against relation's internal name first, then against parent_name from metadata
            # For 'table' relations, rel_name often contains brackets, so use without_square_brackets
            if act_relation.rel_name and without_square_brackets(act_relation.rel_name) == used_relation_name:
                p_metacolumn.relation = act_relation
                act_relation.columns[p_metacolumn.remote_name] = p_metacolumn
                if act_relation.lookml_view: # Only add if a LookML view is associated
                    act_relation.lookml_view.add_base_field(p_metacolumn.looker_field)
                    logger.info(f'Adding {p_metacolumn} to {act_relation.lookml_view.lookml_name}.')
                else:
                    logger.warning(f"No LookML view associated with relation '{act_relation.rel_name}' for column '{p_metacolumn.local_name}'.")
                return # Column added, exit
        logger.warning(f"Could not find matching relation for metadata column '{p_metacolumn.local_name}' (parent: '{p_metacolumn.parent_name}').")


    def yield_relations(self):
        yield self
        for chield_rel in self.children_relations:
            for gc_rel in chield_rel.yield_relations():
                yield gc_rel

    @property
    def base_str(self) -> str:
        if not self.children_relations:
            return f'"{self.rel_name}"'
        if self.rel_name:
            return f'"{self.rel_name}" ({" -- ".join(x.base_str for x in self.children_relations)})'
        return f'{self.rel_type} ({" -- ".join(x.base_str for x in self.children_relations)})'

    def __str__(self):
        return f'Relation {self.base_str}'


class LogicalTable:
    """
    workbook / datasources / datasource / object-graph / objects / object
    """
    object_id: str | None
    object_caption: str
    relation: Relation
    parent_object: ObjectGraph
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.object_id = p_raw_extract.get('@id')
        self.object_caption = p_raw_extract.get('@caption')
        # Properties can be a dict (single property) or a list (multiple properties)
        properties_data = p_raw_extract.get('properties', {})
        if isinstance(properties_data, dict) and 'relation' in properties_data:
            properties_data = [properties_data] # Wrap single dict in list for consistent iteration

        for act_property_data in iter_tag(properties_data):
            if not (object_rel := act_property_data.get('relation', {})):
                continue
            # Need to find the actual Relation object from the datasource's connection
            # Traverse up to the datasource and its connection to find the relation
            ds = self.datasource
            if ds and ds.connection:
                found_relation = False
                for act_rel in ds.connection.yield_relations():
                    # Compare raw_extracts for equality to link to the correct Relation object
                    if act_rel.raw_extract == object_rel:
                        self.relation = act_rel
                        act_rel.rel_object = self
                        found_relation = True
                        break
                if not found_relation:
                    logger.warning(f"No matching relation found in datasource for logical table '{self.object_id}'.")
            else:
                logger.warning(f"No datasource or connection found for logical table '{self.object_id}' to link relation.")

    @property
    def lookml_explore(self) -> LookMLExplore:
        # This logical table represents a node in the object graph, its explore
        # will be the explore of its underlying relation.
        if self.relation and self.relation.lookml_explore:
            return self.relation.lookml_explore
        else:
            # Fallback if no explore can be directly derived from its relation.
            # This might need more sophisticated handling depending on complex Tableau structures.
            logger.warning(f"No LookML explore found for logical table '{self.object_id}' from its relation. Attempting to create a basic one.")
            # Create a simple explore based on its view if available
            if self.relation and self.relation.lookml_view:
                new_explore = LookMLExplore()
                new_explore.first_object = self.relation.lookml_view
                new_explore.logical_table_name = self.relation.lookml_view.lookml_name
                # Add this new explore to the model if it's not already there
                ds = self.datasource
                if ds and ds.lookml_model:
                    ds.lookml_model.add_explore(new_explore)
                return new_explore
            raise RuntimeError(f"Could not generate LookML explore for logical table '{self.object_id}'.")


    def yield_relations(self):
        if not self.relation:
            return
        for act_rel in self.relation.yield_relations():
            yield act_rel

    @property
    def datasource(self) -> Datasource | None:
        act_parent = self.parent_object
        while act_parent and not isinstance(act_parent, Datasource):
            act_parent = act_parent.parent_object
        return act_parent

    def __str__(self):
        return f"{self.__class__.__name__} '{self.object_id}'"


class MetadataColumn:
    """
    workbook / datasources / datasource / connection / metadata-records / metadata-record
    """
    remote_name: str | None
    remote_type: str | None
    local_name: str | None
    parent_name: str | None
    family: str | None
    remote_alias: str | None
    local_type: str | None
    object_id: str | None
    logical_table: LogicalTable | None # This needs to be linked during object_graph parsing
    relation: Relation | None # This will be set by Relation.add_metacolumn
    parent_object: Connection
    column_instances: dict[str, ColumnInstance] | None = None
    _looker_field: ViewBaseField | None = None
    _raw_extract: dict

    @property
    def name(self):
        return self.local_name

    @property
    def looker_field(self) -> ViewBaseField:
        if self._looker_field:
            return self._looker_field
        self._looker_field = ViewBaseField(self.remote_name or self.local_name or "unknown_field") # Fallback name
        if self.local_type == 'string':
            self._looker_field.type = ViewBaseTypeEnum.STRING
        elif self.local_type in ('integer', 'real'):
            self._looker_field.type = ViewBaseTypeEnum.NUMBER # Corrected typo: .ype -> .type
        elif self.local_type == 'boolean':
            self._looker_field.type = ViewBaseTypeEnum.YESNO
        elif self.local_type == 'date' or self.local_type == 'datetime': # Added datetime
            self._looker_field.type = ViewBaseTypeEnum.TIME
            self._looker_field.timeframes = {LookMLTimeframesEnum.RAW, LookMLTimeframesEnum.DATE,
                                             LookMLTimeframesEnum.WEEK, LookMLTimeframesEnum.MONTH,
                                             LookMLTimeframesEnum.QUARTER, LookMLTimeframesEnum.YEAR}
            self._looker_field.datatype = TimeDatatypeEnum.DATE if self.local_type == 'date' else TimeDatatypeEnum.DATETIME # More precise datatype
            self._looker_field.lookml_struct_type = LookMLFieldStructEnum.DIMENSION_GROUP
        else:
            self._looker_field.type = ViewBaseTypeEnum.STRING # Default to string for unknown types
            logger.warning(f"Unknown local type '{self.local_type}' for column '{self.name}'. Defaulting to STRING.")
        self._looker_field.label = self.local_name # Use local_name as label
        self._looker_field.description = f'Metarecord parsed from Tableau {self.datasource} (Relation: {self.relation.rel_name if self.relation else "N/A"}).'
        return self._looker_field

    @property
    def datasource(self) -> Datasource | None:
        act_parent = self.parent_object
        while act_parent and not isinstance(act_parent, Datasource):
            act_parent = act_parent.parent_object
        return act_parent

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.column_instances = {}
        self.remote_name = p_raw_extract.get('remote-name')
        self.remote_type = p_raw_extract.get('remote-type')
        self.local_name = p_raw_extract.get('local-name')
        self.parent_name = p_raw_extract.get('parent-name')
        self.family = p_raw_extract.get('family')
        self.remote_alias = p_raw_extract.get('remote-alias')
        self.local_type = p_raw_extract.get('local-type')
        # Handle different keys for object-id
        if 'object-id' in p_raw_extract:
            self.object_id = p_raw_extract.get('object-id')
        elif '_.fcp.ObjectModelEncapsulateLegacy.true...object-id' in p_raw_extract:
            self.object_id = p_raw_extract.get('_.fcp.ObjectModelEncapsulateLegacy.true...object-id')
        elif '_.fcp.ObjectModelEncapsulateLegacy.false...object-id' in p_raw_extract:
            self.object_id = p_raw_extract.get('_.fcp.ObjectModelEncapsulateLegacy.false...object-id')
        else:
            self.object_id = None # Explicitly None if not found

    def add_column_instance(self, p_ci_extract: dict):
        if self.column_instances is None:
            self.column_instances = {}
        # Ensure a unique key is used, if '@name' is not guaranteed to be unique
        ci_name = p_ci_extract.get('@name')
        if ci_name in self.column_instances:
            return self.column_instances[ci_name]
        new_column_instance = ColumnInstance()
        new_column_instance.parent_object = self
        new_column_instance.raw_extract = p_ci_extract
        self.column_instances[new_column_instance.name] = new_column_instance
        return new_column_instance

    def __hash__(self):
        # Hash based on a unique identifier, e.g., combination of remote_name and parent_name
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
    name: str = ''
    derivation: str | None = None
    pivot: str | None = None
    type: str | None = None
    role: str | None = None
    _lookml_derived_field: ViewDerivedField | None = None
    parent_object: MetadataColumn | CalculatedColumn | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.name = p_raw_extract.get('@name')
        self.derivation = p_raw_extract.get('@derivation')
        self.pivot = p_raw_extract.get('@pivot')
        self.type = p_raw_extract.get('@type')
        self.set_lookml_derived_field()

    @property
    def lookml_derived_field(self):
        return self.set_lookml_derived_field()

    def set_lookml_derived_field(self):
        if self._lookml_derived_field:
            return self._lookml_derived_field
        
        # Ensure parent_object and its looker_field exist
        if not self.parent_object or not hasattr(self.parent_object, 'looker_field') or not self.parent_object.looker_field:
            logger.error(f"Cannot create derived field for column instance '{self.name}': Parent base field missing.")
            return None

        new_derived_field = ViewDerivedField(self.name)
        new_derived_field.parent_base_field = self.parent_object.looker_field
        new_derived_field.derivation_str = self.derivation
        new_derived_field.label = self.name # Use the instance name as label

        self._lookml_derived_field = new_derived_field
        return self._lookml_derived_field

    def __hash__(self):
        # Hash based on a unique identifier, e.g., combination of name and parent object's identifier
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
    # Placeholder for calculated columns, which are currently not fully parsed
    pass


class ObjectGraph:
    logical_tables_dict: OrderedDict[str, LogicalTable]
    relationships: list[Relationship]
    parent_object: Datasource
    _lookml_explore: LookMLExplore | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        self.logical_tables_dict = OrderedDict()
        self.relationships = []
        if not p_raw_extract:
            return
        self.extract_logical_tables()
        self.extract_relationships()
        self.gen_lookml_explore()

    def gen_lookml_explore(self) -> LookMLExplore:
        if self._lookml_explore:
            return self._lookml_explore

        if not self.relationships:
            if self.logical_tables_dict:
                _, act_lt = next(iter(self.logical_tables_dict.items()))
                # For single logical tables without joins, the explore is directly from its relation's view
                if act_lt.relation and act_lt.relation.lookml_view:
                    new_explore = LookMLExplore()
                    new_explore.first_object = act_lt.relation.lookml_view
                    new_explore.logical_table_name = act_lt.relation.lookml_view.lookml_name
                    self._lookml_explore = new_explore
                else:
                    logger.warning("No relationships and no valid view for a single logical table in object graph.")
                    self._lookml_explore = None # Cannot create a valid explore
            else:
                logger.warning("No logical tables and no relationships in object graph to generate explore.")
                self._lookml_explore = None # Cannot create a valid explore
            # Add the explore to the model if it was created
            if self._lookml_explore and self.parent_object.lookml_model:
                self.parent_object.lookml_model.add_explore(self._lookml_explore)
            return self._lookml_explore

        main_explore_chain_head = None

        # Iterate through relationships to build the explore chain
        for i, act_relationship in enumerate(self.relationships):
            current_explore_part = None
            if i == 0:
                # Initialize the first part of the explore chain
                if act_relationship.first_logical_table.relation.lookml_explore:
                    current_explore_part = act_relationship.first_logical_table.relation.lookml_explore
                else:
                    logger.error(f"Could not get initial explore from first logical table relation: {act_relationship.first_logical_table.object_id}")
                    return None # Critical error, cannot build explore

            if act_relationship.second_logical_table.relation.lookml_explore:
                second_explore_part = act_relationship.second_logical_table.relation.lookml_explore
            else:
                logger.error(f"Could not get second explore from second logical table relation: {act_relationship.second_logical_table.object_id}")
                return None # Critical error, cannot build explore

            # If this is the first relationship, or we're building a new branch
            if not main_explore_chain_head:
                # If first_object of current_explore_part is not an Explore, it's a View,
                # so we need to wrap it in a new Explore.
                if isinstance(current_explore_part.first_object, LookMLView):
                    new_main_explore = LookMLExplore()
                    new_main_explore.first_object = current_explore_part.first_object
                    new_main_explore.logical_table_name = current_explore_part.first_object.lookml_name
                    main_explore_chain_head = new_main_explore
                else:
                    main_explore_chain_head = current_explore_part

            # Now, chain the second part to the current main_explore_chain_head
            new_chained_explore = LookMLExplore()
            new_chained_explore.first_object = main_explore_chain_head
            new_chained_explore.second_object = second_explore_part.first_object # This is the view from the joined relation
            new_chained_explore.join_type = act_relationship.join_expression.op_to_join_type(act_relationship.rel_join) # Assuming a method to convert join string to enum
            new_chained_explore.join_relationship = JoinRelationshipEnum.MANY_TO_MANY # Default or infer
            new_chained_explore.join_sql_on = [] # Prepare for join conditions

            if act_relationship.join_expression:
                for mc1, join_rel, mc2 in act_relationship.join_expression.yield_joins():
                    if mc1 and mc2:
                        new_chained_explore.join_sql_on.append((mc1.looker_field, join_rel, mc2.looker_field))
                    else:
                        logger.warning(f"Skipping join condition for relationship between {act_relationship.first_logical_table.object_id} and {act_relationship.second_logical_table.object_id} due to missing metadata columns.")

            main_explore_chain_head = new_chained_explore

        self._lookml_explore = main_explore_chain_head

        # Add the final constructed explore to the model
        if self._lookml_explore and self.parent_object.lookml_model:
            self.parent_object.lookml_model.add_explore(self._lookml_explore)

        return self._lookml_explore


    @property
    def lookml_explore(self) -> LookMLExplore:
        if self._lookml_explore:
            return self._lookml_explore
        return self.gen_lookml_explore()

    def extract_logical_tables(self):
        # The 'object' key can be a single dict or a list of dicts
        objects_data = self.raw_extract.get('objects', {}).get('object')
        if isinstance(objects_data, dict):
            objects_data = [objects_data] # Ensure it's iterable

        for object_data in iter_tag(objects_data):
            new_object = LogicalTable()
            new_object.parent_object = self
            new_object.raw_extract = object_data
            if new_object.object_id:
                self.logical_tables_dict[new_object.object_id] = new_object
            else:
                logger.warning(f"Logical table found without an ID: {object_data}")


    def extract_relationships(self):
        # The 'relationship' key can be a single dict or a list of dicts
        relationships_data = self.raw_extract.get('relationships', {}).get('relationship')
        if isinstance(relationships_data, dict):
            relationships_data = [relationships_data] # Ensure it's iterable

        for relationship_data in iter_tag(relationships_data):
            new_relationship = Relationship()
            new_relationship.parent_object = self
            new_relationship.raw_extract = relationship_data
            self.relationships.append(new_relationship)


class Relationship:
    join_expression: JoinExpression | None = None
    first_endpoint_id: str
    second_endpoint_id: str
    parent_object: ObjectGraph
    _raw_extract: dict
    rel_join: str = 'inner' # Default join type for relationship, to be inferred from expression

    @property
    def first_logical_table(self) -> LogicalTable | None:
        return self.parent_object.logical_tables_dict.get(self.first_endpoint_id)

    @property
    def second_logical_table(self) -> LogicalTable | None:
        return self.parent_object.logical_tables_dict.get(self.second_endpoint_id)

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
        if not p_raw_extract:
            return
        self.first_endpoint_id = p_raw_extract.get('first-end-point', {}).get('@object-id')
        self.second_endpoint_id = p_raw_extract.get('second-end-point', {}).get('@object-id')
        self.rel_join = p_raw_extract.get('@join', 'inner') # Get join type from relationship itself
        self.extract_join_expression()

    def extract_join_expression(self):
        join_expression_data = self.raw_extract.get('expression')
        if join_expression_data:
            self.join_expression = JoinExpression()
            self.join_expression.parent_object = self
            self.join_expression.raw_extract = join_expression_data
        else:
            logger.warning(f"Relationship between {self.first_endpoint_id} and {self.second_endpoint_id} has no join expression.")


class PyhisicalTable:
    pass


class LogicalColumn:
    pass


class PhysicalColumn:
    pass


class TableConnection:
    pass


def main():
    pass


if __name__ == '__main__':
    main()

