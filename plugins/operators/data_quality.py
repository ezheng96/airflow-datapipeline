from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check with an example SQL query and compares actual result to expected result
    
    :param redshift_conn_id: Redshift connection ID
    :param table: Table that the data quality check will be run against
    :param check_type: The type of data quality check. Currently supports either "null" or "count"
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 check_type="null", #defaults to check for null values
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.check_type = check_type

    def execute(self, context):
        self.log.info("Getting Redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Running data validation check of type {self.check_type} on table: {self.table}.")
        
        if self.check_type == "count":
            if self.table == "songplays":
                query = SqlQueries.songplays_count_check
            elif self.table == "users":
                query = SqlQueries.users_count_check
            elif self.table == "songs":
                query = SqlQueries.songs_count_check
            elif self.table == "artists":
                query = SqlQueries.artists_count_check
            elif self.table == "time":
                query = SqlQueries.time_count_check
            else:
                raise ValueError(f"No table defined")
        else:
            if self.table == "songplays":
                query = SqlQueries.songplays_null_check
            elif self.table == "users":
                query = SqlQueries.users_null_check
            elif self.table == "songs":
                query = SqlQueries.songs_null_check
            elif self.table == "artists":
                query = SqlQueries.artists_null_check
            elif self.table == "time":
                query = SqlQueries.time_null_check
            else:
                raise ValueError(f"No table defined")
            
        records = redshift_hook.get_records(query)
        # check is count, ensure the number of records is greater than 0
        if self.check_type == "count":
            if not records or records[0][0] == 0:
                raise ValueError(f"""
                Data quality check failed, {self.table} returned no rows
                """)
            else:
                self.log.info(f"RECORDS: {records}")
                self.log.info(f"{self.table} has {records[0][0]} records")
        else: # checking for nulls, ensure
            if records[0][0] != 0:
                raise ValueError(f"""
                    Data quality check failed. \
                    There are {records[0][0]} Null records
                """)
            else:
                self.log.info(f"There are no null in the key fields of {self.table}")
        self.log.info("Data quality check passed")        
        