# streamlit_app.py

import streamlit as st
import os
import sys
import logging
from dotenv import load_dotenv
import configparser

# Add the project directory to the Python path to allow imports from `utils` and `model`
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env file
load_dotenv()

# --- MOCK UTILITY IMPORTS ---
# In a real scenario, these would be your actual utility files.
# For this demo, we're providing simplified mocks.
try:
    from model.Tableau_objects import Workbook
    from t2l_generator import convert_tableau_to_lookml # This is not directly called, but kept for context
    from utils.tableau_extract_downloader import get_tableau_workbook_file
    from utils.constants import LOOKER_CONNECTION_NAME, TABLEAU_SERVER_URL, TABLEAU_TOKEN_NAME, TABLEAU_TOKEN_SECRET, TABLEAU_SITE_ID, TABLEAU_WORKBOOK_NAME
    from utils.main_logger import logger # Import the logger from your utils
except ImportError as e:
    st.error(f"Error importing core modules: {e}")
    st.info("Please ensure your 'model', 't2l_generator', and 'utils' files are correctly placed and all dependencies are installed.")
    st.stop()

# --- External Package Imports ---
try:
    import looker_sdk
    from looker_sdk import models
except ImportError:
    st.warning("`looker_sdk` not found. Please install it: `pip install looker_sdk`")
    looker_sdk = None
    models = None

try:
    import git
except ImportError:
    st.warning("`GitPython` not found. Please install it: `pip install GitPython`")
    git = None


# --- Streamlit App Configuration ---
st.set_page_config(
    page_title="Tableau to LookML Converter",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Tableau to LookML Converter & Deployer ✨")
st.markdown("Convert your Tableau workbooks to LookML and deploy them to your Looker instance.")

# Initialize session state for LookML repo path
if 'lookml_repo_path' not in st.session_state:
    st.session_state.lookml_repo_path = os.path.join(os.getcwd(), 'lookml_project_repo') # Default to a new folder in current working directory

# --- Helper Functions for Looker SDK and Git ---

@st.cache_resource
def get_looker_sdk():
    """Initializes and returns the Looker SDK client."""
    if looker_sdk is None:
        return None
    try:
        # Assuming looker.ini is in the root directory
        sdk = looker_sdk.init40("looker.ini")
        st.success("Connected to Looker API successfully! ✅")
        return sdk
    except Exception as e:
        st.error(f"Failed to initialize Looker SDK. Check your `looker.ini` configuration. Error: {e}")
        logger.error(f"Looker SDK initialization error: {e}", exc_info=True)
        return None

def git_commit_and_push(repo_path: str, commit_message: str):
    """Performs Git commit and push operations."""
    if git is None:
        st.error("GitPython is not installed. Cannot perform Git operations.")
        return False
    try:
        repo = git.Repo(repo_path)
        # Check if there are changes to commit
        if repo.is_dirty(untracked_files=True):
            # Add all changes to staging
            st.info("Staging all changes...")
            repo.git.add(A=True) # Adds all new/modified/deleted files
            st.info("Committing changes...")
            # Commit changes
            repo.index.commit(commit_message)
            st.success(f"Git commit successful: '{commit_message}' ✅")

            # Push changes to remote
            if repo.remotes:
                origin = repo.remotes.origin
                with st.spinner("Pushing changes to remote Git repository..."):
                    origin.push()
                st.success("Git push successful! 🚀")
            else:
                st.warning("No remote Git repository configured. Changes committed locally but not pushed.")
            return True
        else:
            st.info("No changes detected in the LookML project directory. Nothing to commit.")
            return False
    except git.InvalidGitRepositoryError:
        st.error(f"'{repo_path}' is not a valid Git repository. Please initialize it first in the section above.")
        return False
    except Exception as e:
        st.error(f"Git operation failed: {e}")
        logger.error(f"Git operation error: {e}", exc_info=True)
        return False


# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Upload Tableau File", "Convert to LookML", "Deploy to Looker"])

# --- Tab 1: Upload Tableau File ---
with tab1:
    st.header("Upload Tableau Workbook (.twb / .twbx)")
    uploaded_file = st.file_uploader(
        "Choose a Tableau Workbook file",
        type=["twb", "twbx"],
        help="Upload your Tableau workbook (.twb) or packaged workbook (.twbx) file here."
    )

    if uploaded_file is not None:
        temp_dir = "temp_tableau_files"
        os.makedirs(temp_dir, exist_ok=True)

        file_path = os.path.join(temp_dir, uploaded_file.name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.session_state["uploaded_tableau_file_path"] = file_path
        st.session_state["uploaded_tableau_file_name"] = uploaded_file.name

        st.success(f"File '{uploaded_file.name}' uploaded successfully! 🎉")
        st.info("Proceed to the 'Convert to LookML' tab to process your file.")
        st.json({
            "File Name": uploaded_file.name,
            "File Size": f"{uploaded_file.size / 1024:.2f} KB",
            "Temporary Path": file_path
        })
    else:
        st.session_state["uploaded_tableau_file_path"] = None
        st.session_state["uploaded_tableau_file_name"] = None
        st.warning("No file uploaded yet.")

# --- Tab 2: Convert to LookML ---
with tab2:
    st.header("Convert to LookML")
    st.markdown("Choose your source for conversion:")

    conversion_source = st.radio(
        "Select Conversion Source:",
        ("Uploaded File", "Tableau Server"),
        index=0,
        help="Select whether to convert an uploaded file or fetch from Tableau Server."
    )

    if conversion_source == "Uploaded File":
        st.subheader("Convert from Uploaded File")
        uploaded_file_path = st.session_state.get("uploaded_tableau_file_path")
        uploaded_file_name = st.session_state.get("uploaded_tableau_file_name")

        if uploaded_file_path:
            st.info(f"Using uploaded file: **{uploaded_file_name}**")
            if st.button("Generate LookML from Uploaded File 🚀"):
                with st.spinner(f"Converting Tableau workbook to LookML into `{st.session_state.lookml_repo_path}`... This might take a moment."):
                    try:
                        # Use the path from session state as the deployment folder
                        # Ensure the directory exists before passing it to Workbook
                        os.makedirs(st.session_state.lookml_repo_path, exist_ok=True)
                        workbook = Workbook(p_deployment_folder=st.session_state.lookml_repo_path)
                        workbook.file_full_path = uploaded_file_path
                        deployed_files = workbook.lookml_project.deploy_object()

                        st.session_state["generated_lookml_files"] = deployed_files
                        st.success("LookML files generated successfully! 🎉")
                        st.write(f"Generated files (saved in `{st.session_state.lookml_repo_path}` directory):")
                        for f in deployed_files:
                            st.code(f)
                            if f.endswith(".lkml") and os.path.exists(f):
                                with open(f, 'r') as lookml_f:
                                    st.expander(f"View content of {os.path.basename(f)}").code(lookml_f.read(), language='lookml')

                    except Exception as e:
                        st.error(f"Error during conversion: {e}")
                        logger.error(f"Conversion error: {e}", exc_info=True)
                        st.exception(e)
        else:
            st.warning("Please upload a Tableau file in the 'Upload Tableau File' tab first.")

    elif conversion_source == "Tableau Server":
        st.subheader("Convert from Tableau Server")
        st.markdown(
            """
            To connect to Tableau Server, ensure the following environment variables are set in your `.env` file:
            - `TABLEAU_SERVER_URL`
            - `TABLEAU_TOKEN_NAME`
            - `TABLEAU_TOKEN_SECRET`
            - `TABLEAU_SITE_ID` (optional, default to empty string for default site)
            - `TABLEAU_WORKBOOK_NAME` (required for specific workbook conversion)
            """
        )

        st.expander("Current Tableau Server Configuration (from .env)").json({
            "TABLEAU_SERVER_URL": TABLEAU_SERVER_URL if TABLEAU_SERVER_URL else "Not set",
            "TABLEAU_TOKEN_NAME": TABLEAU_TOKEN_NAME if TABLEAU_TOKEN_NAME else "Not set",
            "TABLEAU_TOKEN_SECRET": "********" if TABLEAU_TOKEN_SECRET else "Not set",
            "TABLEAU_SITE_ID": TABLEAU_SITE_ID if TABLEAU_SITE_ID else "Default Site (empty)",
            "TABLEAU_WORKBOOK_NAME": TABLEAU_WORKBOOK_NAME if TABLEAU_WORKBOOK_NAME else "Not set"
        })

        if st.button("Generate LookML from Tableau Server 🌐"):
            if not all([TABLEAU_SERVER_URL, TABLEAU_TOKEN_NAME, TABLEAU_TOKEN_SECRET, TABLEAU_WORKBOOK_NAME]):
                st.error("Please ensure all required Tableau Server environment variables are set in your `.env` file.")
            else:
                with st.spinner(f"Attempting to fetch workbook from Tableau Server and convert into `{st.session_state.lookml_repo_path}`..."):
                    try:
                        twb_path, _ = get_tableau_workbook_file(
                            server_url=TABLEAU_SERVER_URL,
                            token_name=TABLEAU_TOKEN_NAME,
                            token_secret=TABLEAU_TOKEN_SECRET,
                            site_id=TABLEAU_SITE_ID,
                            workbook_name=TABLEAU_WORKBOOK_NAME
                        )

                        if not twb_path:
                            st.error("Failed to retrieve workbook from Tableau Server.")
                        else:
                            # Ensure the directory exists before passing it to Workbook
                            os.makedirs(st.session_state.lookml_repo_path, exist_ok=True)
                            workbook = Workbook(p_deployment_folder=st.session_state.lookml_repo_path)
                            workbook.file_full_path = twb_path
                            deployed_files = workbook.lookml_project.deploy_object()

                            st.session_state["generated_lookml_files"] = deployed_files
                            st.success("LookML files generated from Tableau Server successfully! 🎉")
                            st.write(f"Generated files (saved in `{st.session_state.lookml_repo_path}` directory):")
                            for f in deployed_files:
                                st.code(f)
                                if f.endswith(".lkml") and os.path.exists(f):
                                    with open(f, 'r') as lookml_f:
                                        st.expander(f"View content of {os.path.basename(f)}").code(lookml_f.read(), language='lookml')
                    except Exception as e:
                        st.error(f"Error connecting to Tableau Server or during conversion: {e}")
                        logger.error(f"Tableau Server conversion error: {e}", exc_info=True)
                        st.exception(e)

# --- Tab 3: Deploy to Looker ---
with tab3:
    st.header("Deploy LookML to Looker Instance")
    st.markdown(
        """
        Deployment typically involves pushing your generated LookML files to a Git repository connected to your Looker instance.
        You'll need a `looker.ini` file configured with your Looker API credentials in the root directory.
        """
    )

    st.subheader("Looker API Configuration")
    st.markdown(
        """
        The application will attempt to use `looker.ini` for SDK initialization.
        Ensure it has the following structure:
        ```ini
        [Looker]
        base_url = YOUR_LOOKER_INSTANCE_URL
        client_id = YOUR_LOOKER_CLIENT_ID
        client_secret = YOUR_LOOKER_CLIENT_SECRET
        verify_ssl = True # or False if you have SSL issues (not recommended for production)
        ```
        """
    )

    # Display looker.ini content if available
    if os.path.exists("looker.ini"):
        config = configparser.ConfigParser()
        config.read("looker.ini")
        # Hide sensitive information for display
        display_config = {s: {k: ('********' if 'secret' in k else v) for k, v in config.items(s)} for s in config.sections()}
        st.expander("View `looker.ini` content (sensitive info hidden)").json(display_config)
    else:
        st.warning("`looker.ini` not found in the root directory. Please create it.")
        st.expander("Example `looker.ini` content").code("""
[Looker]
base_url = https://your-instance.looker.com:19999
client_id = YOUR_LOOKER_CLIENT_ID
client_secret = YOUR_LOOKER_CLIENT_SECRET
verify_ssl = True
""", language='ini')


    st.subheader("LookML Project Git Repository Setup")
    st.markdown("Specify the local path to your Looker project's Git repository. This is where the generated LookML files will be saved and managed by Git.")

    # User input for the Git repository path
    st.session_state.lookml_repo_path = st.text_input(
        "LookML Project Git Repository Path:",
        value=st.session_state.lookml_repo_path,
        help="This directory should be your local clone of the Git repository connected to your Looker project. All generated LookML will be placed here."
    )

    # Check if the path exists and is a Git repo
    is_git_repo = False
    path_exists = os.path.exists(st.session_state.lookml_repo_path)

    if path_exists:
        try:
            _ = git.Repo(st.session_state.lookml_repo_path)
            is_git_repo = True
            st.success(f"Path **`{st.session_state.lookml_repo_path}`** is a valid Git repository. ✅")
        except git.InvalidGitRepositoryError:
            st.warning(f"Path **`{st.session_state.lookml_repo_path}`** exists but is not a Git repository.")
            if st.button(f"Initialize Git Repository at `{st.session_state.lookml_repo_path}`"):
                try:
                    git.Repo.init(st.session_state.lookml_repo_path)
                    st.success(f"Git repository initialized at **`{st.session_state.lookml_repo_path}`** 🎉")
                    st.rerun() # Rerun to update the status
                except Exception as e:
                    st.error(f"Failed to initialize Git repository: {e}")
                    logger.error(f"Git init error: {e}", exc_info=True)
    else:
        st.warning(f"Path **`{st.session_state.lookml_repo_path}`** does not exist.")
        st.info("The application can create this directory for you.")
        if st.button(f"Create Directory and Initialize Git at `{st.session_state.lookml_repo_path}`"):
            try:
                os.makedirs(st.session_state.lookml_repo_path, exist_ok=True)
                git.Repo.init(st.session_state.lookml_repo_path)
                st.success(f"Directory created and Git repository initialized at **`{st.session_state.lookml_repo_path}`** 🎉")
                st.rerun() # Rerun to update the status
            except Exception as e:
                st.error(f"Failed to create directory or initialize Git repository: {e}")
                logger.error(f"Directory/Git init error: {e}", exc_info=True)


    st.subheader("Deployment Actions")
    st.markdown(
        """
        Once your LookML files are generated into the specified repository path, you can use the buttons below to
        commit these changes to your **local Git repository** and then **push them to your remote**.
        Looker will then automatically detect these changes from your connected Git repository.
        """
    )

    if path_exists and is_git_repo:
        st.info(f"Generated LookML files will be placed into and committed from: `{st.session_state.lookml_repo_path}`")
        if git is not None:
            if st.button("Commit and Push LookML Changes to Git ⬆️"):
                commit_msg = st.text_input("Git Commit Message:", value="Generated LookML from Tableau conversion", key="git_commit_msg")
                if commit_msg:
                    git_commit_and_push(st.session_state.lookml_repo_path, commit_msg)
                else:
                    st.warning("Please provide a commit message.")
        else:
            st.error("GitPython is not installed. Please install it to enable Git deployment features.")
    else:
        st.warning("Please ensure the LookML Project Git Repository Path is valid and initialized as a Git repository before attempting deployment actions.")


    st.subheader("Looker API Interaction (Advanced)")
    st.markdown(
        """
        While file-based changes are best handled via Git, the Looker SDK can be used for other deployment
        related tasks, such as programmatically deploying a project to production after Git sync.
        """
    )

    sdk = get_looker_sdk()
    if sdk:
        looker_project_id = st.text_input("Looker Project ID (optional for API deployment):", help="Enter the ID of your Looker project (e.g., 'your_project_name').")
        if looker_project_id:
            if st.button("Deploy Looker Project to Production via API (Manual Trigger) ⚙️"):
                if models is None:
                    st.error("Looker SDK models not loaded. Cannot perform API deployment.")
                else:
                    with st.spinner(f"Deploying Looker project '{looker_project_id}' to production..."):
                        try:
                            # This is a conceptual call; direct file pushes are usually Git-driven
                            # You might refresh a project or deploy a specific branch.
                            # Example: Triggering a deploy from a specific branch
                            # sdk.deploy_project(body=models.WriteDeployProject(
                            #     project_id=looker_project_id,
                            #     ref="main" # or your development branch
                            # ))
                            st.success(f"Looker project '{looker_project_id}' deployment triggered successfully (simulated)! 🚀")
                            st.info("Note: Actual LookML file changes are primarily synced via Git. This button simulates a 'deploy to production' trigger which would usually follow a Git push.")
                        except Exception as e:
                            st.error(f"Failed to trigger Looker project deployment: {e}")
                            logger.error(f"Looker project deployment API error: {e}", exc_info=True)
                            st.exception(e)
        else:
            st.info("Enter a Looker Project ID to enable API deployment options.")
    else:
        st.warning("Looker SDK not initialized. Please ensure `looker_sdk` is installed and `looker.ini` is correctly configured.")

    st.subheader("Debugging Steps for Production Deployment 🐛")
    st.markdown(
        """
        When deploying to a production Looker instance, you might encounter issues. Here are common debugging steps:

        1.  **Verify Looker API Credentials**:
            * Double-check `base_url`, `client_id`, and `client_secret` in your `looker.ini`.
            * Ensure the API user has **developer permissions** for the Looker project and instance.
            * Test connectivity using a simple Looker SDK script outside of this app (e.g., `sdk.me()`).

        2.  **Git Integration Checks**:
            * **Local Git Status**: After generation, check your local `lookml_files` directory and your Git repository status (`git status`). Are the new/updated files staged and committed?
            * **Remote Repository**: Verify that your changes are pushed to the remote Git repository that your Looker project is connected to (`git log origin/main`).
            * **Looker Project Configuration**: In Looker (Admin -> Project), ensure your LookML project is correctly configured with the Git repository URL, SSH key (if used), and branch.
            * **Deploying in Looker**: After pushing to Git, you typically need to "Deploy to Production" from within the Looker IDE or use the `deploy_project_to_production` API endpoint if automating.

        3.  **Generated LookML Syntax**:
            * **Looker IDE Validation**: Even if the conversion script runs, the generated LookML might have syntax errors not caught by the script. Open the generated files in the Looker IDE and use the **LookML Validator** to identify issues.
            * **Linter Errors**: Pay attention to any linter warnings or errors in the Looker IDE.
            * **Data Explorer**: Test the views and explores in Looker's Explore UI to ensure they correctly query your database.

        4.  **Database Connection Issues**:
            * Ensure the `connection` name in your generated LookML models (`.model.lkml` files) matches an existing database connection in your Looker instance (Admin -> Databases -> Connections).
            * Verify the Looker connection has the necessary permissions to access the underlying database tables.

        5.  **Logging and Error Messages**:
            * **App Logs**: Check the logs of this Streamlit app for any Python exceptions during conversion or simulated deployment.
            * **Looker Logs**: If you have access to Looker server logs, these can provide deeper insights into deployment failures or LookML parsing issues.

        6.  **Dependency Conflicts (Python Environment)**:
            * Ensure all necessary Python libraries (`looker_sdk`, `GitPython`, `lxml`, `python-dotenv`, `streamlit`) are installed and their versions are compatible. Use `pip freeze > requirements.txt` to manage dependencies.

        7.  **File Paths and Permissions**:
            * Verify that the Streamlit application has write permissions to create the `lookml_files` directory and write `.lkml` files, and read permissions for `looker.ini`.
        """
    )
