#------------------------------------------------------------------------------
# Hands-On Lab: Intro to Data Engineering with Notebooks
# Script:       deploy_task_dag.py
# Author:       Jeremiah Hansen
# Last Updated: 1/30/2026
#------------------------------------------------------------------------------

from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask
from datetime import timedelta


# Create the tasks using the DAG API
def main(session: Session, database_name: str, schema_name: str, notebook_project_name: str) -> str:
    # Set the environment context
#    session.use_schema(f"{database_name}.{schema_name}")

    warehouse_name = "DEMO_WH"
    dag_name = "DEMO_DAG"
    compute_pool = "SYSTEM_COMPUTE_POOL_CPU"
    runtime = "V2.2-CPU-PY3.12"
    external_access_integration = "pypi_access"

    api_root = Root(session)
    schema = api_root.databases[database_name].schemas[schema_name]
    dag_op = DAGOperation(schema)

    print(f"Defining DAG: {dag_name}")
    print(f"  Notebook project: {database_name}.{schema_name}.{notebook_project_name}")
    print(f"  Compute pool: {compute_pool}")
    print(f"  Runtime: {runtime}")

    # Define the DAG
    with DAG(dag_name, schedule=timedelta(days=1), warehouse=warehouse_name) as dag:
        dag_task1 = DAGTask("LOAD_EXCEL_FILES_TASK", definition=f'''
            EXECUTE NOTEBOOK PROJECT {database_name}.{schema_name}.{notebook_project_name}
                MAIN_FILE = '06_load_excel_files.ipynb'
                COMPUTE_POOL = {compute_pool}
                RUNTIME = '{runtime}'
                QUERY_WAREHOUSE = {warehouse_name}
                EXTERNAL_ACCESS_INTEGRATIONS = ('{external_access_integration}')
                ARGUMENTS = '--database-name {database_name} --schema-name {schema_name}'
            ''', warehouse=warehouse_name)
        dag_task2 = DAGTask("LOAD_DAILY_CITY_METRICS", definition=f'''
            EXECUTE NOTEBOOK PROJECT {database_name}.{schema_name}.{notebook_project_name}
                MAIN_FILE = '07_load_daily_city_metrics.ipynb'
                COMPUTE_POOL = {compute_pool}
                RUNTIME = '{runtime}'
                QUERY_WAREHOUSE = {warehouse_name}
                ARGUMENTS = '--database-name {database_name} --schema-name {schema_name}'
            ''', warehouse=warehouse_name)

        # Define the dependencies between the tasks
        dag_task1 >> dag_task2 # dag_task1 is a predecessor of dag_task2

    print(f"  Task 1: LOAD_EXCEL_FILES_TASK -> 06_load_excel_files.ipynb")
    print(f"  Task 2: LOAD_DAILY_CITY_METRICS -> 07_load_daily_city_metrics.ipynb")

    # Create the DAG in Snowflake
    print(f"Deploying DAG to {database_name}.{schema_name}...")
    dag_op.deploy(dag, mode="orreplace")

    return f"DAG {dag_name} deployed successfully"


# For local debugging
if __name__ == "__main__":
    import sys

    session = Session.builder.getOrCreate()
    if len(sys.argv) > 3:
        print(main(session, sys.argv[1], sys.argv[2], sys.argv[3]))
    else:
        print("Usage: python deploy_task_dag.py <database> <schema> <notebook_project>")
