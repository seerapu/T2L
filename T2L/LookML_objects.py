# TODO - using parameters - severity: 2
# TODO - implement dimensiongroup for date fields - severity: 2
# TODO - optimizing structure - severity: 3
# TODO - proper logging - severity: 3
# TODO - explore join with date fields - severity: 3
# TODO - connection name - severity: 4
# TODO - extend derived date field related_name - severity: 5
# TODO - visualization with multi explore

# generating primary key ?
# model connection ?

from __future__ import annotations
from textwrap import indent
from typing import Generator
from collections import OrderedDict
from .LookML_enums import ViewBaseTypeEnum, JoinTypeEnum, JoinRelationshipEnum, \
    LookMLFieldStructEnum, LookMLTimeframesEnum, TimeDatatypeEnum, LookMLDashboardElementTypeEnum, \
    LookMLMeasureTypeEnum
from utils.constants import LOOKER_CONNECTION_NAME
import re
import os
from utils.main_logger import logger


def lookml_name_generator(p_text: str) -> Generator[str]:
    base_text_list = []
    for act_char in p_text[:60]:
        if re.match('[a-z0-9_]', act_char.lower()):
            base_text_list.append(act_char.lower())
        elif act_char in (' ', "'", '"', '(', ')', '-', ':'):
            base_text_list.append('_')
        else:
            base_text_list.append('X')
    base_text = ''.join(base_text_list)
    base_text = re.sub(r'_+', '_', base_text)
    yield base_text
    if base_text[-1] == '_':
        base_text = base_text[:-1]
    for i in range(1, 1000):
        yield f'{base_text}_{i}'
    raise StopIteration()


class LookMLProject:
    def __init__(self):
        self._lookml_name = ''
        self.name_orig = ''
        self.lookml_models: OrderedDict[str, LookMLModel] = OrderedDict()
        self.lookml_dashboards: OrderedDict[str, Dashboard] = OrderedDict()
        self.lookml_dashboardelements: OrderedDict[str, DashboardElement] = OrderedDict()
        self.deployment_folder = 'lookml_files'
        self.quote_char_start = '`'
        self.quote_char_end = '`'

    def deploy_object(self) -> list[str]:
        deployed_files = []
        if not os.path.exists(self.deployment_folder):
            os.makedirs(self.deployment_folder)
        if not os.path.exists(self.project_folder):
            os.makedirs(self.project_folder)
        for _, act_model in self.lookml_models.items():
            deployed_files += act_model.deploy_object()
        for _, act_dashboard in self.lookml_dashboards.items():
            deployed_files += act_dashboard.deploy_object()
        return deployed_files

    @property
    def project_folder(self) -> str:
        return os.path.join(self.deployment_folder, self.lookml_name)

    @property
    def lookml_name(self):
        if not self._lookml_name:
            self._lookml_name = next(lookml_name_generator(self.name_orig))
        return self._lookml_name

    def add_lookml_model(self, p_lookml_model: LookMLModel):
        if p_lookml_model in self.lookml_models.values():
            return
        p_lookml_model.lookml_project = self
        for de_lookml_name in lookml_name_generator(p_lookml_model.name_orig):
            if de_lookml_name not in self.lookml_models.keys():
                self.lookml_models[de_lookml_name] = p_lookml_model
                p_lookml_model.lookml_name = de_lookml_name
                break

    def add_lookml_dashboard(self, p_lookml_dashboard: Dashboard):
        if p_lookml_dashboard in self.lookml_dashboards.values():
            return
        p_lookml_dashboard.lookml_project = self
        for de_lookml_name in lookml_name_generator(p_lookml_dashboard.name_orig):
            if de_lookml_name not in self.lookml_dashboards.keys():
                self.lookml_dashboards[de_lookml_name] = p_lookml_dashboard
                p_lookml_dashboard.lookml_name = de_lookml_name
                return

    def add_lookml_dashboardelement(self, p_lookml_dashboardelement: DashboardElement):
        if p_lookml_dashboardelement in self.lookml_dashboardelements.values():
            return
        p_lookml_dashboardelement.lookml_project = self
        for de_lookml_name in lookml_name_generator(p_lookml_dashboardelement.element_name_orig):
            if de_lookml_name not in self.lookml_dashboardelements.keys():
                self.lookml_dashboardelements[de_lookml_name] = p_lookml_dashboardelement
                p_lookml_dashboardelement.lookml_name = de_lookml_name
                return


class LookMLModel:
    def __init__(self):
        self.lookml_name = ''
        self.name_orig = ''
        self.lookml_project: LookMLProject | None = None
        self.lookml_explores: OrderedDict[str, LookMLExplore] = OrderedDict()
        self.lookml_views: OrderedDict[str, LookMLView] = OrderedDict()

    @property
    def quote_char_start(self):
        return self.lookml_project.quote_char_start

    @property
    def quote_char_end(self):
        return self.lookml_project.quote_char_end

    @property
    def model_folder(self):
        return os.path.join(self.lookml_project.project_folder, 'models')

    def deploy_object(self) -> list[str]:
        deployed_file_names = []
        for _, act_view in self.lookml_views.items():
            deployed_file_names += act_view.deploy_object()
        if not os.path.exists(self.model_folder):
            os.makedirs(self.model_folder)
        model_file_name = os.path.join(self.model_folder, f'{self.lookml_name}.model.lkml')
        logger.info(f'Deploying {self} into {model_file_name}.')
        with open(model_file_name, 'w+') as model_file:
            for _, act_explore in self.lookml_explores.items():
                model_file.write(act_explore.lookml_str)
                model_file.write('\n')
                logger.info(f'Adding {act_explore}.')
        deployed_file_names.append(model_file_name)
        return deployed_file_names

    def add_lookml_view(self, p_lookml_view: LookMLView) -> None:
        if p_lookml_view in self.lookml_views.values():
            return
        p_lookml_view.lookml_model = self
        for fieldname_gen in lookml_name_generator(p_lookml_view.orig_name):
            if fieldname_gen not in self.lookml_views:
                self.lookml_views[fieldname_gen] = p_lookml_view
                p_lookml_view.lookml_name = fieldname_gen
                break

    def add_explore(self, p_lookml_explore: LookMLExplore) -> None:
        if p_lookml_explore in self.lookml_explores.values():
            return
        p_lookml_explore.lookml_view = self
        base_name = ''
        if p_lookml_explore.logical_table_name:
            base_name = p_lookml_explore.logical_table_name
        else:
            for act_explore in p_lookml_explore.yield_child_explores():
                base_name = act_explore.first_object.lookml_name
                break
        for fieldname_gen in lookml_name_generator(base_name):
            if fieldname_gen in self.lookml_explores:
                continue
            self.lookml_explores[fieldname_gen] = p_lookml_explore
            p_lookml_explore.lookml_name = fieldname_gen
            break

    def __str__(self):
        return f"{self.__class__.__name__} '{self.lookml_name}'"

class LookMLParameter:
    def __init__(self, parameter_name: str, parameter_value):
        self.parameter_name = parameter_name
        self.parameter_value = parameter_value

    def lookml_str(self):
        param_suffix = ''
        if self.parameter_name == 'sql':
            param_suffix = ' ;;'
        return f'{self.parameter_name}: {self.parameter_value}{param_suffix}'

    def __str__(self):
        return f"{self.__class__.__name__} '{self.parameter_name}'"


class ViewBaseField:
    """
    Represents columns from the physical source table as a LookML dimension field.
    """
    def __init__(self, p_source_field: str):
        self.lookml_name = ''
        self.lookml_struct_type = LookMLFieldStructEnum.DIMENSION
        self.type = ViewBaseTypeEnum.STRING
        self.timeframes: set[LookMLTimeframesEnum] | None = None
        self.datatype: TimeDatatypeEnum | None = None
        self.source_field = p_source_field
        self.lookml_view: LookMLView | None = None
        self.label: str = ''
        self.description: str = ''

    @property
    def quote_char_start(self):
        return self.lookml_view.quote_char_start

    @property
    def quote_char_end(self):
        return self.lookml_view.quote_char_end

    @property
    def sql(self) -> str:
        return f'${{TABLE}}.{self.quote_char_start}{self.source_field}{self.quote_char_end} ;;'

    @property
    def lookml_name_with_date_suffix(self):
        if self.type == ViewBaseTypeEnum.TIME:
            return f'{self.lookml_name}_raw'
        return self.lookml_name

    @property
    def lookml_parameters_dict(self) -> OrderedDict[str, str]:
        rd = OrderedDict()
        rd['type'] = self.type
        if self.timeframes:
            rd['timeframes'] = '[' + ', '.join(self.timeframes) + ']'
        if self.datatype:
            rd['datatype'] = self.datatype
        rd['sql'] = self.sql
        if self.label:
            rd['label'] = '"' + self.label.replace('"', "'") + '"'
        if self.description:
            rd['description'] = '"' + self.description.replace('"', "'") + '"'
        return rd

    @property
    def lookml_field_dict(self) -> OrderedDict[str, str | OrderedDict]:
        rd = OrderedDict()
        rd[str(self.lookml_struct_type)] = self.lookml_name
        rd['internal_object_parameters'] = self.lookml_parameters_dict
        return rd

    @property
    def lookml_str(self):
        return f'{self.lookml_struct_type}: {self.lookml_name} {{\n' \
             + indent('\n'.join(param_key + ': ' + param_value for param_key, param_value
                                in self.lookml_parameters_dict.items()), '  ') + f'\n' \
               f'}}'

    def __str__(self):
        return f"{self.__class__.__name__} '{self.lookml_name}'"


class ViewDerivedField:
    def __init__(self, p_source_field: str):
        self.parent_base_field: ViewBaseField | None = None
        self.lookml_name = ''
        self.lookml_struct_type: LookMLFieldStructEnum | None = None
        self.type: LookMLMeasureTypeEnum | None = None
        self._devivation_str = ''
        self.source_field = p_source_field
        self.label: str = ''
        self.description: str = ''

    @property
    def related_field_name(self) -> str:
        return self.lookml_name or self.parent_base_field.lookml_name

    @property
    def derivation_str(self):
        return self._devivation_str

    @property
    def sql(self):
        if not self.type:
            return ''
        return f'${{{self.parent_base_field.lookml_name}}} ;;'

    @derivation_str.setter
    def derivation_str(self, p_devivation_str: str):
        self._devivation_str = p_devivation_str
        derivation_mapping = {'Sum': LookMLMeasureTypeEnum.SUM,
                              'Count': LookMLMeasureTypeEnum.COUNT,
                              'CountD': LookMLMeasureTypeEnum.COUNT_DISTINCT,
                              'Avg': LookMLMeasureTypeEnum.AVERAGE}
        self.type = derivation_mapping.get(p_devivation_str)
        if self.type:
            self.lookml_struct_type = LookMLFieldStructEnum.MEASURE
            self.parent_base_field.lookml_view.add_derived_field(self)
            return
        self.lookml_struct_type = None
        if p_devivation_str in ('Year', 'Year-Trunc'):
            self.lookml_name = f'{self.parent_base_field.lookml_name}_year'
        elif p_devivation_str == 'Month-Trunc':
            self.lookml_name = f'{self.parent_base_field.lookml_name}_month'
        elif p_devivation_str == 'Quarter-Trunc':
            self.lookml_name = f'{self.parent_base_field.lookml_name}_quarter'

            # TODO - other possibilities

    @property
    def lookml_parameters_dict(self) -> OrderedDict[str, str]:
        rd = OrderedDict()
        if self.type:
            rd['type'] = self.type
        if self.label:
            rd['label'] = self.label.replace('"', "'")
        if self.sql:
            rd['sql'] = self.sql
        if self.description:
            rd['description'] = self.description.replace('"', "'")
        return rd

    @property
    def lookml_field_dict(self) -> OrderedDict[str, str | OrderedDict]:
        rd = OrderedDict()
        if str(self.lookml_struct_type):
            rd[str(self.lookml_struct_type)] = self.lookml_name
        if pd := self.lookml_parameters_dict:
            rd['internal_object_parameters'] = pd
        return rd

    @property
    def lookml_str(self):
        if not self.type:
            return ''
        return f'{self.lookml_struct_type}: {self.lookml_name} {{\n' \
               + indent('\n'.join(param_key + ': ' + param_value for param_key, param_value
                                  in self.lookml_parameters_dict.items()), '  ') + f'\n' \
               f'}}'

    def __str__(self):
        return f"{self.__class__.__name__} '{self.related_field_name}'"


class ViewParameterField:
    """
    Represents a LookML parameter field.
    """
    name: str
    type: ViewBaseTypeEnum = ViewBaseTypeEnum.STRING
    label: str = ''
    defaul_value: str = ''

    @property
    def sql(self) -> str:
        return f'${{TABLE}}.{self.name} ;;'

    def iter_lookml_field_params(self):
        yield f'type: {self.type}'
        yield f'sql: {self.sql}'
        if self.label:
            yield f'label: {self.label}'

    def lookml_str(self) -> str:
        rl = [f'parameter: {self.name} {{'] \
             + [indent(fp, '  ') for fp in self.iter_lookml_field_params()] \
             + ['}']
        return '\n'.join(rl)

    def __str__(self):
        return f"{self.__class__.__name__}: '{self.name}'"


class LookMLView:
    def __init__(self, p_orig_name: str):
        self.lookml_name = ''
        self.orig_name = p_orig_name
        self.extension: bool = False
        self.label: str = ''
        self.sql_table_items: list[str] = []
        self.sql: str = ''
        self.lookml_model: LookMLModel | None = None
        self.fields: OrderedDict[str, ViewBaseField | ViewDerivedField] = OrderedDict()
        self.parameters: list
        self.calculated_fields: list

    @property
    def quote_char_start(self):
        return self.lookml_model.quote_char_start

    @property
    def quote_char_end(self):
        return self.lookml_model.quote_char_end

    @property
    def sql_table_name(self) -> str:
        return '.'.join(iter(f'{self.quote_char_start}{ti}{self.quote_char_end}' for ti in self.sql_table_items))

    def deploy_object(self) -> list[str]:
        view_folder = os.path.join(self.lookml_model.lookml_project.project_folder, 'views')
        if not os.path.exists(view_folder):
            os.makedirs(view_folder)
        view_file_name = os.path.join(view_folder, f"{self.lookml_name}.view.lkml")
        with open(view_file_name, 'w+') as view_file:
            view_file.write(self.lookml_str)
        logger.info(f'Deploying {self} into {view_file_name}.')
        return [view_file_name]

    def add_base_field(self, p_base_field: ViewBaseField) -> None:
        if p_base_field and p_base_field in self.fields.values():
            return
        p_base_field.lookml_view = self
        for fieldname_gen in lookml_name_generator(p_base_field.source_field):
            if fieldname_gen in self.fields:
                continue
            self.fields[fieldname_gen] = p_base_field
            p_base_field.lookml_name = fieldname_gen
            break

    def add_derived_field(self, p_derived_field: ViewDerivedField) -> None:
        if p_derived_field and p_derived_field in self.fields.values():
            return
        if not p_derived_field.type:
            # TODO - logging
            return
        # p_derived_field.lookml_view = self
        derived_name = f'{p_derived_field.parent_base_field.lookml_name}_{p_derived_field.type}'
        for fieldname_gen in lookml_name_generator(derived_name):
            if fieldname_gen in self.fields:
                continue
            self.fields[fieldname_gen] = p_derived_field
            p_derived_field.lookml_name = fieldname_gen
            break

    def iter_lookml_field_params(self):
        if self.extension:
            yield 'extension: required'
        if self.sql_table_name:
            yield f'sql_table_name: {self.sql_table_name} ;;'
        if self.sql:
            yield f'derived_table: {{\n' \
                  f'  sql:\n' \
                  f'{indent(self.sql, "    ")} ;;' \
                  f'}}'
        for _, lookml_field in self.fields.items():
            yield indent(lookml_field.lookml_str, '  ')
        if self.label:
            act_label_escaped = self.label.replace('"', "'")
            yield f'label: {act_label_escaped}'

    def __hash__(self):
        return hash(self.lookml_name)

    def __eq__(self, other):
        return id(self) == id(other)

    @property
    def lookml_str(self):
        return f'view: {self.lookml_name} {{\n' \
             + indent('\n'.join(fp for fp in self.iter_lookml_field_params()), '  ') + f'\n' \
               f'}}'

    def __str__(self):
        return f"{self.__class__.__name__} '{self.lookml_name}'"


class LookMLExplore:
    lookml_name: str = ''
    connection_name: str = LOOKER_CONNECTION_NAME
    logical_table_name: str = ''
    first_object: LookMLExplore | LookMLView
    second_object: LookMLView | None = None
    join_type: JoinTypeEnum = JoinTypeEnum.LEFT_OUTER
    join_relationship: JoinRelationshipEnum = JoinRelationshipEnum.MANY_TO_MANY
    join_sql_on: list[tuple[ViewBaseField, str, ViewBaseField]] | None = None
    lookml_model: LookMLModel | None = None

    def yield_child_explores(self):
        explore_queue = [self]
        last_explore = self
        while isinstance(last_explore.first_object, LookMLExplore):
            last_explore = last_explore.first_object
            explore_queue.append(last_explore)
        while explore_queue:
            yield explore_queue.pop()

    def explore_field_name(self, p_derived_field: ViewDerivedField) -> str:
        inner_explore = next(self.yield_child_explores())
        if p_derived_field.parent_base_field in inner_explore.first_object.fields.values():
            return f'{self.lookml_name}.{p_derived_field.related_field_name}'
        return f'{p_derived_field.parent_base_field.lookml_view.lookml_name}.{p_derived_field.related_field_name}'

    @property
    def lookml_str(self):
        view_aliases: dict[str, LookMLView] = {}
        expore_text_list = [f'connection: "{self.connection_name}"', '', 'include: "../views/*.view.lkml"',
                            'include: "../dashboards/*.dashboard.lookml"', '']
        for act_explore in self.yield_child_explores():
            if isinstance(act_explore.first_object, LookMLView):
                view_aliases[self.lookml_name] = act_explore.first_object
                expore_text_list.append(f'explore: {self.lookml_name} {{')
                if self.lookml_name != act_explore.first_object.lookml_name:
                    expore_text_list.append(f'  from: {act_explore.first_object.lookml_name}')
            if act_explore.second_object:
                name_gen = ''
                for name_gen in lookml_name_generator(act_explore.second_object.lookml_name):
                    if name_gen not in view_aliases:
                        view_aliases[name_gen] = act_explore.second_object
                        break
                expore_text_list.append(f'  join: {name_gen} {{')
                if name_gen != act_explore.second_object.lookml_name:
                    expore_text_list.append(f'    from: {act_explore.second_object.lookml_name}')
                expore_text_list.append(f'    type: {act_explore.join_type}')
                expore_text_list.append(f'    relationship: {act_explore.join_relationship}')
                sql_on_text_list = []
                for (col1, rel_str, col2) in act_explore.join_sql_on:
                    col1_table_alias = col2_table_alias = ''
                    for view_alias, used_view in view_aliases.items():
                        if used_view == col1.lookml_view:
                            col1_table_alias = view_alias
                            if col2_table_alias:
                                break
                        elif used_view == col2.lookml_view:
                            col2_table_alias = view_alias
                            if col1_table_alias:
                                break
                    sql_on_text_list.append(f'(${{{col1_table_alias}.{col1.lookml_name_with_date_suffix}}} '
                                            f'{rel_str} '
                                            f'${{{col2_table_alias}.{col2.lookml_name_with_date_suffix}}})')
                expore_text_list.append("    sql_on: " + '\n            AND '.join(sql_on_text_list) + " ;;")
                expore_text_list.append('  }')
        expore_text_list.append('}')
        return '\n'.join(expore_text_list)

    def __str__(self):
        return f"{self.__class__.__name__} '{self.lookml_name}'"


class Dashboard:
    def __init__(self):
        self.name_orig = 'dashboard_default_name'
        self.lookml_name = ''
        self.title = ''
        self.layout = 'newspaper'
        self.lookml_dashboard_elements: OrderedDict[str, DashboardElement] = OrderedDict()
        self.lookml_project: LookMLProject | None = None

    def deploy_object(self) -> list[str]:
        dashboard_folder = os.path.join(self.lookml_project.project_folder, 'dashboards')
        if not os.path.exists(dashboard_folder):
            os.makedirs(dashboard_folder)
        dashboard_file_name = os.path.join(dashboard_folder, f"{self.lookml_name}.dashboard.lookml")
        with open(dashboard_file_name, 'w+') as dashboard_file:
            dashboard_file.write(self.lookml_str)
        logger.info(f'Deploying {self} into {dashboard_file_name}.')
        return [dashboard_file_name]

    def add_dashboard_elements(self, p_dashboard_element: DashboardElement):
        if p_dashboard_element in self.lookml_dashboard_elements.values():
            return
        for de_lookml_name in lookml_name_generator(p_dashboard_element.element_name_orig):
            if de_lookml_name in self.lookml_dashboard_elements.keys():
                continue
            self.lookml_dashboard_elements[de_lookml_name] = p_dashboard_element
            break

    def get_dashboard_element_lookml_name(self, p_dashboard_element: DashboardElement) -> str | None:
        for de_name, de_object in self.lookml_dashboard_elements.items():
            if de_object is p_dashboard_element:
                return de_name

    @property
    def lookml_str(self) -> str:
        rs_list = [f'- dashboard: {self.lookml_name}']
        if self.title:
            rs_list.append(f'  title: {self.title}')
        if self.layout:
            rs_list.append(f'  layout: {self.layout}')
        if self.lookml_dashboard_elements:
            rs_list.append('\n')
            rs_list.append('  elements:')
            for _, de in self.lookml_dashboard_elements.items():
                rs_list.append(indent(de.lookml_str, '  '))
                rs_list.append('\n')
        return '\n'.join(rs_list)

    def __str__(self):
        return f"{self.__class__.__name__} '{self.lookml_name}'"


class DashboardElement:
    lookml_project: LookMLProject | None = None
    element_name_orig: str = ''
    lookml_name = ''
    lookml_model: LookMLModel | None = None
    lookml_explore: LookMLExplore | None = None
    title: str | None = None
    type: LookMLDashboardElementTypeEnum | None = None
    # dimensions: list[ViewDerivedField] | None = None
    # measures: list[ViewDerivedField] | None = None
    fields: list[ViewDerivedField] | None = None
    pivots: list[ViewDerivedField] | None = None

    @property
    def lookml_parameters_dict(self) -> OrderedDict[str, str]:
        rd = OrderedDict()
        if self.lookml_model:
            rd['model'] = self.lookml_model.lookml_name
        if self.lookml_explore:
            rd['explore'] = self.lookml_explore.lookml_name
        if self.type:
            rd['type'] = self.type
        if self.title:
            rd['title'] = self.title
        # if self.dimensions:
        #     rd['dimensions'] = f"[{', '.join(self.lookml_explore.lookml_name + '.' + f.related_field_name
        #     for f in self.dimensions)}]"
        # if self.measures:
        #     rd['measures'] = f"[{', '.join(self.lookml_explore.lookml_name + '.' + f.related_field_name
        #     for f in self.measures)}]"

        if self.lookml_explore and self.fields:
            rd['fields'] = "[" + ', '.join(self.lookml_explore.explore_field_name(f) for f in self.fields) + "]"
        if self.lookml_explore and self.pivots:
            rd['pivots'] = "[" + ', '.join(self.lookml_explore.explore_field_name(f) for f in self.pivots) + "]"
        return rd

    @property
    def lookml_field_dict(self) -> OrderedDict[str, str | OrderedDict]:
        rd = OrderedDict()
        rd['name'] = self.lookml_name
        rd['internal_object_parameters'] = self.lookml_parameters_dict
        return rd

    @property
    def lookml_str(self):
        rs_list = [f"- name: {self.lookml_name}"]
        for param_name, param_value in self.lookml_parameters_dict.items():
            rs_list.append(f'  {param_name}: {param_value}')
        return '\n'.join(rs_list)


def main():
    pass


if __name__ == '__main__':
    main()
