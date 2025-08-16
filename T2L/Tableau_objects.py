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
    if isinstance(p_tag_value, dict):
        yield p_tag_value
    elif isinstance(p_tag_value, list):
        for act_val in p_tag_value:
            yield act_val


"""
class TableauBaseObject:
    parent_object: TableauBaseObject | None = None
    _raw_extract: dict

    @property
    def raw_extract(self):
        return self._raw_extract

    @raw_extract.setter
    def raw_extract(self, p_raw_extract: dict):
        self._raw_extract = p_raw_extract
"""


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
        return self._file_path.split(os.sep)[-1]

    @property
    def file_name_wo_ext(self):
        return '.'.join(self.file_name.split('.')[:-1])

    @property
    def lookml_project(self) -> LookMLProject:
        if self._lookml_project:
            return self._lookml_project
        self._lookml_project = LookMLProject()
        self._lookml_project.name_orig = self.tableau_workbook_name
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
                    logger.warning(f'Column instance root is not found')

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
            ci = self.used_column_instances[act_ci_match.group(0)]
            self.rows.append(ci)
            logger.info(f'Adding {ci} to {self} rows.')

    def set_cols(self):
        self.cols = []
        cols_data = self.raw_extract.get('table', {}).get('cols', '')
        if not cols_data:
            logger.info(f'{self} - no cols found.')
            return
        for act_ci_match in re.finditer(r'\[[^[]+]\.\[[^[]+]', cols_data):
            ci = self.used_column_instances[act_ci_match.group(0)]
            self.cols.append(ci)
            logger.info(f'Adding {ci} to {self} cols.')

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
                self.relation.add_metacolumn(new_meta_column)

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
        for i, child_expr_data in enumerate(p_raw_extract.get('expression', [])):
            new_exr = JoinExpression()
            new_exr.index = i
            new_exr.raw_extract = child_expr_data
            new_exr.parent_object = self
            self.children_expressions.append(new_exr)

    def _metacolumn_from_relation(self, p_used_relation: Relation) -> MetadataColumn:
        # join by remote name
        table_relation_name, table_field_name = map(without_square_brackets, self.op.split('.'))
        for act_rel in p_used_relation.yield_relations():
            if act_rel.rel_name == table_relation_name:
                self._metacolumn = act_rel.columns[table_field_name]
                return self._metacolumn

    def _metacolumn_from_relationship(self, p_relationship: Relationship) -> MetadataColumn:
        # join by local name
        if self.index == 0:
            parent_rel = p_relationship.first_logical_table.relation
        else:
            parent_rel = p_relationship.second_logical_table.relation
        for act_rel in parent_rel.yield_relations():
            for mc in act_rel.columns.values():
                if mc.name == self.op:
                    self._metacolumn = mc
                    return self._metacolumn

    @property
    def metacolumn(self):
        if self.children_expressions:
            return None
        if not self._metacolumn:
            parent_rel = self.parent_object
            while not (isinstance(parent_rel, Relation) or isinstance(parent_rel, Relationship)):
                parent_rel = parent_rel.parent_object
            if isinstance(parent_rel, Relation):
                self._metacolumn = self._metacolumn_from_relation(parent_rel)
            else:
                self._metacolumn = self._metacolumn_from_relationship(parent_rel)
        return self._metacolumn

    def yield_joins(self):
        if self.op == 'AND':
            join_rel_list = self.children_expressions
        else:
            join_rel_list = [self]
        for act_join_rel in join_rel_list:
            yield act_join_rel.children_expressions[0].metacolumn, \
                unescape(act_join_rel.op), \
                act_join_rel.children_expressions[1].metacolumn


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
        self.columns = {}
        if self.rel_type in ('collection', 'join', 'union'):
            for rel_text in ('relation',
                             '_.fcp.ObjectModelEncapsulateLegacy.true...relation',
                             '_.fcp.ObjectModelEncapsulateLegacy.false...relation'):
                if not p_raw_extract.get(rel_text):
                    continue
                for child_rel_data in p_raw_extract.get(rel_text):
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
            self._lookml_view = LookMLView(self.rel_name)
            self.datasource.lookml_model.add_lookml_view(self._lookml_view)
            logger.info(f"Creating LookML view '{self._lookml_view.lookml_name}' from {self}.")
            if self.rel_type == 'text':
                self._lookml_view.sql = self.rel_sql_text
            elif self.rel_type == 'table':
                self._lookml_view.sql_table_items = [t.group(1) for t in re.finditer(r'\[([^[]+)]', self.rel_table)]
            elif self.rel_type == 'union':
                # TODO - handle Tableau unions in logical tables - currently the first table is used only
                self._lookml_view.sql_table_name = self.children_relations[0].rel_table
            else:
                raise NotImplementedError()

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
        elif self.rel_type == 'join':
            if self.children_relations[0].rel_type in ('table', 'text', 'union'):
                self._lookml_explore.first_object = self.children_relations[0].lookml_view
            else:
                self._lookml_explore.first_object = self.children_relations[0].lookml_explore
            self._lookml_explore.second_object = self.children_relations[1].lookml_view
            if self.rel_join == 'right':
                logger.warning('Right join in relation - converted to full join.')
            self._lookml_explore.join_type = {'inner': JoinTypeEnum.INNER,
                                              'left': JoinTypeEnum.LEFT_OUTER,
                                              'right': JoinTypeEnum.FULL_OUTER,
                                              'full': JoinTypeEnum.FULL_OUTER}[self.rel_join]
            for mc1, join_rel, mc2 in self.join_expression.yield_joins():
                self._lookml_explore.join_sql_on.append((mc1.looker_field, join_rel, mc2.looker_field))
        return self._lookml_explore

    def add_metacolumn(self, p_metacolumn: MetadataColumn) -> None:
        used_relation_name = without_square_brackets(p_metacolumn.parent_name)
        for act_relation in self.yield_relations():
            if used_relation_name == act_relation.rel_name:
                p_metacolumn.relation = act_relation
                act_relation.columns[p_metacolumn.remote_name] = p_metacolumn
                act_relation.lookml_view.add_base_field(p_metacolumn.looker_field)
                logger.info(f'Adding {p_metacolumn} to {act_relation}')

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
        for act_property_data in iter_tag(p_raw_extract.get('properties', {})):
            if not (object_rel := act_property_data.get('relation', {})):
                return
            for act_rel in self.parent_object.parent_object.yield_relations():
                if act_rel.raw_extract == object_rel:
                    self.relation = act_rel
                    act_rel.rel_object = self
                    break

    @property
    def lookml_explore(self) -> LookMLExplore:
        for act_rel in self.relation.yield_relations():
            if act_rel.lookml_explore:
                # if not act_rel.lookml_explore.lookml_name:
                #     self.datasource.lookml_model.add_explore(act_rel.lookml_explore)
                return act_rel.lookml_explore
            # if act_rel.

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
    logical_table: LogicalTable | None
    relation: Relation | None
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
        self._looker_field = ViewBaseField(self.remote_name)
        if self.local_type == 'string':
            self._looker_field.type = ViewBaseTypeEnum.STRING
        elif self.local_type in ('integer', 'real'):
            self._looker_field.ype = ViewBaseTypeEnum.NUMBER
        elif self.local_type == 'boolean':
            self._looker_field.type = ViewBaseTypeEnum.YESNO
        elif self.local_type == 'date':
            self._looker_field.type = ViewBaseTypeEnum.TIME
            self._looker_field.timeframes = {LookMLTimeframesEnum.RAW, LookMLTimeframesEnum.DATE,
                                             LookMLTimeframesEnum.WEEK, LookMLTimeframesEnum.MONTH,
                                             LookMLTimeframesEnum.QUARTER, LookMLTimeframesEnum.YEAR}
            self._looker_field.datatype = TimeDatatypeEnum.DATE
            self._looker_field.lookml_struct_type = LookMLFieldStructEnum.DIMENSION_GROUP
        else:
            self._looker_field.type = ViewBaseTypeEnum.STRING
        self._looker_field.description = f'Metarecord parsed from Tableau {self.datasource} {self.relation}.'
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
        self.object_id = p_raw_extract.get('object-id')
        if 'object-id' in p_raw_extract:
            self.object_id = p_raw_extract.get('object-id')
        elif '_.fcp.ObjectModelEncapsulateLegacy.true...object-id' in p_raw_extract:
            self.object_id = p_raw_extract.get('_.fcp.ObjectModelEncapsulateLegacy.true...object-id')
        else:
            self.object_id = p_raw_extract.get('_.fcp.ObjectModelEncapsulateLegacy.false...object-id')

    def add_column_instance(self, p_ci_extract: dict):
        if self.column_instances is None:
            self.column_instances = {}
        if p_ci_extract.get('@name') in self.column_instances:
            return self.column_instances[p_ci_extract.get('@name')]
        new_column_instance = ColumnInstance()
        new_column_instance.parent_object = self
        new_column_instance.raw_extract = p_ci_extract
        self.column_instances[new_column_instance.name] = new_column_instance
        return new_column_instance

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return id(self) == id(other)

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"

#
# class Column:
#     name: str = ''
#     datatype: str | None = None
#     role: str | None = None
#     type: # str | None = None
#     sematic_role: str | None = None
#     parent_object: MetadataColumn | CalculatedColumn | None = None
#     _raw_extract: dict
#
#     @property
#     def raw_extract(self):
#         return self._raw_extract
#
#     @raw_extract.setter
#     def raw_extract(self, p_raw_extract: dict):
#         self._raw_extract = p_raw_extract
#         self.datatype = p_raw_extract.get('@datatype')
#         self.name = p_raw_extract.get('@name')
#         self.role = p_raw_extract.get('@role')
#         self.type = p_raw_extract.get('@type')
#         self.sematic_role = p_raw_extract.get('@sematic-role')
#
#     @property
#     def looker_field(self):
#         return self.parent_object.looker_field
#
#     def __str__(self):
#         return f"{self.__class__.__name__} '{self.name}'"


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
        new_derived_field = ViewDerivedField(self.name)
        # TODO - linking to calculated fields
        new_derived_field.parent_base_field = self.parent_object.looker_field
        new_derived_field.derivation_str = self.derivation
        self._lookml_derived_field = new_derived_field
        return self._lookml_derived_field

    def __str__(self):
        return f"{self.__class__.__name__} '{self.name}'"


class CalculatedColumn:
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
            _, act_lt = next(iter(self.logical_tables_dict.items()))
            return act_lt.lookml_explore
        main_explore = LookMLExplore()
        for i, act_relationship in enumerate(self.relationships):
            if i == 0:
                if act_relationship.first_logical_table.relation.rel_type in ('union', 'table', 'text'):
                    main_explore = act_relationship.first_logical_table.relation.lookml_view
                else:
                    main_explore = act_relationship.first_logical_table.lookml_explore
            for next_explore in act_relationship.second_logical_table.lookml_explore.yield_child_explores():
                new_main_explore = LookMLExplore()
                new_main_explore.first_object = main_explore
                if isinstance(next_explore.first_object, LookMLView):
                    new_main_explore.second_object = next_explore.first_object
                    new_main_explore.join_sql_on = []
                    for mc1, join_rel, mc2 in act_relationship.join_expression.yield_joins():
                        new_main_explore.join_sql_on.append((mc1.looker_field, join_rel, mc2.looker_field))
                    main_explore = new_main_explore
                    new_main_explore = LookMLExplore()
                    new_main_explore.first_object = main_explore
                new_main_explore.second_object = next_explore.second_object
                new_main_explore.join_type = next_explore.join_type
                new_main_explore.join_relationship = next_explore.join_relationship
                new_main_explore.join_sql_on = next_explore.join_sql_on
                main_explore = new_main_explore
        self._lookml_explore = main_explore
        self.parent_object.lookml_model.add_explore(main_explore)
        return self._lookml_explore

    @property
    def lookml_explore(self) -> LookMLExplore:
        if self._lookml_explore:
            return self._lookml_explore
        return self.gen_lookml_explore()

    def extract_logical_tables(self):
        for object_data in iter_tag(self.raw_extract.get('objects', {}).get('object', [])):
            new_object = LogicalTable()
            new_object.parent_object = self
            new_object.raw_extract = object_data
            self.logical_tables_dict[new_object.object_id] = new_object

    def extract_relationships(self):
        for relationship_data in iter_tag(self.raw_extract.get('relationships', {}).get('relationship', [])):
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

    @property
    def first_logical_table(self) -> LogicalTable:
        return self.parent_object.logical_tables_dict[self.first_endpoint_id]

    @property
    def second_logical_table(self) -> LogicalTable:
        return self.parent_object.logical_tables_dict[self.second_endpoint_id]

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
        self.extract_join_expression()

    def extract_join_expression(self):
        join_expression = self.raw_extract.get('expression')
        self.join_expression = JoinExpression()
        self.join_expression.parent_object = self
        self.join_expression.raw_extract = join_expression


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
