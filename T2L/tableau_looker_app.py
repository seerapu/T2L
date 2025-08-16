# streamlit_app.py

import streamlit as st
import os
import sys
import logging
from dotenv import load_dotenv

# Add the project directory to the Python path to allow imports from `utils` and `model`
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env file
load_dotenv()

# --- MOCK UTILITY IMPORTS ---
# In a real scenario, these would be your actual utility files.
# For this demo, we're providing simplified mocks.
try:
    from model.Tableau_objects import Workbook
    from t2l_generator import convert_tableau_to_lookml
    # We'll simulate t2l_generator_from_server's main function here directly
    # as it has external dependencies on the actual Tableau Server connection.
except ImportError as e:
    st.error(f"Error importing core modules: {e}")
    st.info("Please ensure your 'model' and 't2l_generator' files are correctly placed.")
    st.stop()


# --- Streamlit App Configuration ---
st.set_page_config(
    page_title="Tableau to LookML Converter",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Tableau to LookML Converter & Deployer ✨")
st.markdown("Convert your Tableau workbooks to LookML and deploy them to your Looker instance.")

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
        # Create a temporary directory if it doesn't exist
        temp_dir = "temp_tableau_files"
        os.makedirs(temp_dir, exist_ok=True)

        # Save the uploaded file temporarily
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

    lookml_output = None
    if conversion_source == "Uploaded File":
        st.subheader("Convert from Uploaded File")
        uploaded_file_path = st.session_state.get("uploaded_tableau_file_path")
        uploaded_file_name = st.session_state.get("uploaded_tableau_file_name")

        if uploaded_file_path:
            st.info(f"Using uploaded file: **{uploaded_file_name}**")
            if st.button("Generate LookML from Uploaded File 🚀"):
                with st.spinner("Converting Tableau workbook to LookML... This might take a moment."):
                    try:
                        # Call the conversion function from t2l_generator.py
                        # The deploy_object method in LookMLProject handles writing files
                        # We need to instantiate Workbook and then call its lookml_project.deploy_object()
                        workbook = Workbook()
                        workbook.file_full_path = uploaded_file_path
                        deployed_files = workbook.lookml_project.deploy_object()

                        st.session_state["generated_lookml_files"] = deployed_files
                        st.success("LookML files generated successfully! 🎉")
                        st.write("Generated files (saved in `./lookml_files` directory):")
                        for f in deployed_files:
                            st.code(f)
                            # Optionally display content of a few generated files
                            if f.endswith(".lkml") and os.path.exists(f):
                                with open(f, 'r') as lookml_f:
                                    st.expander(f"View content of {os.path.basename(f)}").code(lookml_f.read(), language='lookml')

                    except Exception as e:
                        st.error(f"Error during conversion: {e}")
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

        # Display current environment variables (for debugging/info)
        st.expander("Current Tableau Server Configuration (from .env)").json({
            "TABLEAU_SERVER_URL": os.getenv("TABLEAU_SERVER_URL", "Not set"),
            "TABLEAU_TOKEN_NAME": os.getenv("TABLEAU_TOKEN_NAME", "Not set"),
            "TABLEAU_TOKEN_SECRET": "********" if os.getenv("TABLEAU_TOKEN_SECRET") else "Not set",
            "TABLEAU_SITE_ID": os.getenv("TABLEAU_SITE_ID", "Default Site (empty)"),
            "TABLEAU_WORKBOOK_NAME": os.getenv("TABLEAU_WORKBOOK_NAME", "Not set")
        })

        if st.button("Generate LookML from Tableau Server 🌐"):
            # This simulates calling main from t2l_generator_from_server.py
            # In a real scenario, you'd integrate directly with its functions.
            # Here, we need to ensure the `utils` imports from t2l_generator_from_server are available.
            try:
                # Mocking the t2l_generator_from_server.main logic
                # This requires actual implementation of get_tableau_workbook_file and git_repo_utils
                st.info("Attempting to fetch workbook from Tableau Server and convert...")
                # Assuming `get_tableau_workbook_file` and `Workbook` are available via sys.path
                # and that the mock utilities are in place.

                # This part needs your actual `utils` to work fully.
                # For demonstration, we'll simulate a successful outcome.
                temp_dir = "temp_tableau_server_files"
                os.makedirs(temp_dir, exist_ok=True)
                mock_twb_path = os.path.join(temp_dir, "mock_server_workbook.twb")
                # Create a dummy file for the mock
                with open(mock_twb_path, "w") as f:
                    f.write("<workbook><datasources><datasource name='MockServerData'></datasource></datasources></workbook>")

                workbook = Workbook()
                workbook.file_full_path = mock_twb_path # Use the mock path
                deployed_files = workbook.lookml_project.deploy_object()

                st.session_state["generated_lookml_files"] = deployed_files
                st.success("LookML files generated from Tableau Server (simulated) successfully! 🎉")
                st.write("Generated files (saved in `./lookml_files` directory):")
                for f in deployed_files:
                    st.code(f)
                    if f.endswith(".lkml") and os.path.exists(f):
                        with open(f, 'r') as lookml_f:
                            st.expander(f"View content of {os.path.basename(f)}").code(lookml_f.read(), language='lookml')

            except Exception as e:
                st.error(f"Error connecting to Tableau Server or during conversion: {e}")
                st.warning("Please ensure your Tableau Server environment variables are correctly set and the server is accessible.")
                st.exception(e)

# --- Tab 3: Deploy to Looker ---
with tab3:
    st.header("Deploy LookML to Looker Instance")
    st.markdown(
        """
        Deployment typically involves pushing your generated LookML files to a Git repository connected to your Looker instance.
        You'll need a `looker.ini` file configured with your Looker API credentials.
        """
    )

    st.subheader("Looker API Configuration")
    st.markdown(
        """
        Create a `looker.ini` file in the root of your project with the following structure:
        ```ini
        [Looker]
        base_url = YOUR_LOOKER_INSTANCE_URL
        client_id = YOUR_ER_CLIENT_ID
        client_secret = YOUR_LOOKER_CLIENT_SECRET
        verify_ssl = True # or False if you have SSL issues (not recommended for production)
        ```
        """
    )
    # Display example looker.ini content
    st.expander("Example `looker.ini` content").code("""
[Looker]
base_url = https://your-instance.looker.com:19999
client_id = YOUR_LOOKER_CLIENT_ID
client_secret = YOUR_LOOKER_CLIENT_SECRET
verify_ssl = True
""", language='ini')

    st.subheader("Deployment Steps")
    st.markdown(
        """
        1.  **Ensure Git Repository is Configured**: Your Looker project must be connected to a Git repository.
        2.  **LookML Files**: The generated LookML files are typically saved in the `lookml_files` directory.
        3.  **Push to Git**: You would then commit these `lookml_files` to your Git repository and push them.
            Looker will detect these changes.
        """
    )

    st.warning("Direct deployment to a Looker instance via API for file updates is complex and usually handled via Git integration. This section provides conceptual steps.")

    generated_files = st.session_state.get("generated_lookml_files", [])
    if generated_files:
        st.info("You have generated LookML files. You can now manually review them and push to your Looker-connected Git repository.")
        st.write("Generated files ready for deployment:")
        for f in generated_files:
            st.code(f)
    else:
        st.info("No LookML files have been generated yet. Please convert a Tableau file first.")

    st.subheader("Debugging Steps for Production Deployment 🐛")
    st.markdown(
        """
        When deploying to a production Looker instance, you might encounter issues. Here are common debugging steps:

        1.  **Verify Looker API Credentials**:
            * Double-check `base_url`, `client_id`, and `client_secret` in your `looker.ini`.
            * Ensure the API user has **developer permissions** for the Looker project and instance.
            * Test connectivity using a simple Looker SDK script outside of this app.

        2.  **Git Integration Checks**:
            * **Local Git Status**: After generation, check your local `lookml_files` directory and your Git repository status. Are the new/updated files staged and committed?
            * **Remote Repository**: Verify that your changes are pushed to the remote Git repository that your Looker project is connected to.
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
            * Ensure all necessary Python libraries (e.g., `looker_sdk`, `lxml`, `python-dotenv`, `streamlit`) are installed and their versions are compatible. Use `pip freeze > requirements.txt` to manage dependencies.

        7.  **File Paths and Permissions**:
            * Verify that the Streamlit application has write permissions to create the `lookml_files` directory and write `.lkml` files.
        """
    )
