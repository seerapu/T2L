from utils.tableau_extract_downloader import get_tableau_workbook_file
from model.Tableau_objects import Workbook
from utils.constants import TABLEAU_SITE_ID, TABLEAU_TOKEN_NAME, TABLEAU_TOKEN_SECRET, TABLEAU_WORKBOOK_NAME, \
        TABLEAU_SERVER_URL
from utils.git_repo_utils import get_local_branch, deploy_lookml_project_to_remote


def main():
    twb_path, _ = get_tableau_workbook_file(server_url=TABLEAU_SERVER_URL,
                                            token_name=TABLEAU_TOKEN_NAME,
                                            token_secret=TABLEAU_TOKEN_SECRET,
                                            site_id=TABLEAU_SITE_ID,
                                            workbook_name=TABLEAU_WORKBOOK_NAME)

    if not twb_path:
        raise FileNotFoundError

    tableau_wb = Workbook()
    tableau_wb.file_full_path = twb_path

    if repo := get_local_branch():
        deploy_lookml_project_to_remote(repo, tableau_wb.lookml_project)
    else:
        tableau_wb.lookml_project.deploy_object()
    return


if __name__ == '__main__':
    main()
