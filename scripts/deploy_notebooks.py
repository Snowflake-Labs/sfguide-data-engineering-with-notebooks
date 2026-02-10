#------------------------------------------------------------------------------
# Hands-On Lab: Intro to Data Engineering with Notebooks
# Script:       deploy_notebooks.py
# Author:       Jeremiah Hansen
# Last Updated: 2/9/2026
#------------------------------------------------------------------------------

from snowflake.snowpark import Session


def main(session: Session, database_name: str, schema_name: str, notebook_project_name: str, local_folder_path: str) -> str:
    """
    Deploy a notebook project to Snowflake.

    1. Gets a temporary stage from the session
    2. Uploads all files from the local folder to the stage
    3. Creates or updates the notebook project from the staged files
    """
    # Step 1: Get a temporary stage from the session
    session_stage = session.get_session_stage()
    print(f"Using session stage: {session_stage}")

    # Step 2: Upload all files from the local folder to the stage
    print(f"Uploading files from: {local_folder_path}")
    session.file.put(f"file://{local_folder_path}/*", session_stage, auto_compress=False, overwrite=True)

    # Step 3: Check if the notebook project already exists
    print(f"Checking if notebook project exists: {notebook_project_name}")
    result = session.sql(f"SHOW NOTEBOOK PROJECTS IN {database_name}.{schema_name}").collect()

    project_exists = any(row["name"] == notebook_project_name for row in result)

    # Step 4: Create or alter the notebook project
    full_project_name = f"{database_name}.{schema_name}.{notebook_project_name}"
    stage_path = session_stage

    if project_exists:
        print(f"Notebook project exists, adding new version...")
        session.sql(f"ALTER NOTEBOOK PROJECT {full_project_name} ADD VERSION FROM '{stage_path}'").collect()
    else:
        print(f"Creating new notebook project...")
        session.sql(f"CREATE NOTEBOOK PROJECT {full_project_name} FROM '{stage_path}'").collect()

    return f"Notebook project {full_project_name} deployed successfully"


# For local debugging
if __name__ == "__main__":
    import sys
    from session_utils import get_snowpark_session

    # Get a Snowpark session (works in notebook, local, and CI/CD)
    # Note: Session is intentionally never closed to avoid issues in notebooks
    session = get_snowpark_session()

    if len(sys.argv) > 4:
        print(main(session, sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))
    else:
        print("Usage: python deploy_notebooks.py <database> <schema> <notebook_project> <local_folder_path>")
