# model/Tableau_objects.py (Excerpt, focus on Workbook class changes)

# ... (existing imports) ...

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


# ... (existing functions like without_square_brackets, iter_tag, etc.) ...


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

