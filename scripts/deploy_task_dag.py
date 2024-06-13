#------------------------------------------------------------------------------
# Hands-On Lab: Intro to Data Engineering with Notebooks
# Script:       deploy_task_dag.py
# Author:       Jeremiah Hansen
# Last Updated: 6/11/2024
#------------------------------------------------------------------------------


from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask
from datetime import timedelta

# Create the tasks using the DAG API
def main(session: Session, database_name, schema_name) -> str:
    # Set the environment context
    env = 'PROD' if schema_name == 'PROD_SCHEMA' else 'DEV'
    session.use_schema(f"{database_name}.{schema_name}")

    warehouse_name = "DEMO_WH"
    dag_name = "DEMO_DAG"
    api_root = Root(session)
    schema = api_root.databases[database_name].schemas[schema_name]
    dag_op = DAGOperation(schema)

    # Define the DAG
    with DAG(dag_name, schedule=timedelta(days=1), warehouse=warehouse_name) as dag:
        dag_task1 = DAGTask("LOAD_EXCEL_FILES_TASK", definition=f'''EXECUTE NOTEBOOK "{database_name}"."{schema_name}"."{env}_06_load_excel_files"()''', warehouse=warehouse_name)
        dag_task2 = DAGTask("LOAD_DAILY_CITY_METRICS", definition=f'''EXECUTE NOTEBOOK "{database_name}"."{schema_name}"."{env}_07_load_daily_city_metrics"()''', warehouse=warehouse_name)

        # Define the dependencies between the tasks
        dag_task1 >> dag_task2 # dag_task1 is a predecessor of dag_task2

    # Create the DAG in Snowflake
    dag_op.deploy(dag, mode="orreplace")

    #dag_op.run(dag)


# For local debugging
if __name__ == "__main__":
    import sys

    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # type: ignore
        else:
            print(main(session))  # type: ignore
