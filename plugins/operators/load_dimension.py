from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table in Redshift using data from staging tables
    
    :param redshift_conn_id: Redshift connection ID
    :param table: Target table in Redshift that will be loaded
    :param select_sql: SQL query for selecting data to be loaded
    :param append_insert: Whether the append or truncate method
        of inserting data will be used. Default is false since we are loading dimensions
    :param primary key: When using the append method of inserting, use this key to update existing records
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_name="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append=append

    def execute(self, context):
        self.log.info("Getting Redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Loading data into dimension table {self.table} in Redshift")
        
        if not self.append:
            self.log.info("Insert mode is truncate, deleting rows from Redshift Table: {}".format(self.table))
            redshift_hook.run("TRUNCATE {}".format(self.table))
            
            if self.table == "users":
                select_query = SqlQueries.user_table_insert
            elif self.table == "songs":
                select_query = SqlQueries.song_table_insert
            elif self.table == "artists":
                select_query = SqlQueries.artist_table_insert
            elif self.table == "time":
                select_query = SqlQueries.time_table_insert
            else:
                select_query = ""
        else:
            self.log.info("Insert mode is append, appending records to table: {}".format(self.table))
            
            if self.table == "users":
                select_query = SqlQueries.user_table_insert_append
            elif self.table == "songs":
                select_query = SqlQueries.song_table_insert_append
            elif self.table == "artists":
                select_query = SqlQueries.artist_table_insert_append
            elif self.table == "time":
                select_query = SqlQueries.time_table_insert_append
            else:
                select_query = ""
        
        insert_sql = f"""
                INSERT INTO {self.table}
                {select_query}
            """
        
        self.log.info("SQL query to be used: {}".format(select_query))
        self.log.info(insert_sql)
        redshift_hook.run(insert_sql)
