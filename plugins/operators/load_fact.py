from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    Loads Fact table in Redshift with data from the staging tables
    
    :param redshift_conn_id: Redshift connection ID
    :param table: Target table in Redshift that will be loaded
    :param select_sql: SQL query for selecting data to be loaded
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_name="",
                 append=True, # defaults to True since we are inserting into a fact table
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append = append
#         self.sql_select_statement = sql_select_statement

    def execute(self, context):
        self.log.info("Getting Redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append:
            self.log.info(f"Insert mode is append. Appending rows to table: {self.table} in Redshift")
            if self.table == "songplays":
                select_query=SqlQueries.songplay_table_insert_append
            else:
                select_query=""
        else:
            self.log.info("Insert mode is truncate, deleting rows from Redshift Table: {}".format(self.table))
            redshift_hook.run("TRUNCATE {}".format(self.table))
            if self.table == "songplays":
                select_query=SqlQueries.songplay_table_insert
            else:
                select_query=""
        
        self.log.info(f"Loading data into fact table {self.table} in Redshift")
        
        insert_sql = f"""
            INSERT INTO {self.table}
            {select_query}
        """
        redshift_hook.run(insert_sql)
