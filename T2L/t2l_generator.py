import argparse
from model.Tableau_objects import Workbook
import sys
import logging
import os

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


EXPORT_DIRECTORY = 'exports'


class CommandLine:
    def __init__(self):
        parser = argparse.ArgumentParser(
            description=f"Converts Tableau extracts (*.twbx / *.twb) to LookML files into '{os.sep}exports' folder.")
        parser.add_argument("-f", "--file_path", help="Tableau extract path", required=True)

        argument = parser.parse_args()

        self.file_path = argument.file_path


def convert_tableau_to_lookml(p_file_full_path: str):
    tableau_wb = Workbook()
    tableau_wb.file_full_path = p_file_full_path
    tableau_wb.lookml_project.deploy_object()


def main():
    if len(sys.argv) == 3:
        sys_args = CommandLine()
        file_full_path = sys_args.file_path
        logger.info(f'CMD file path: {file_full_path}')
    else:
        file_full_path = "superstore_test_with_viz.twb"
    convert_tableau_to_lookml(file_full_path)


if __name__ == '__main__':
    main()
