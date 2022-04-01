from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Copies JSON data from S3 to staging tables in Redshift data warehouse
    
    :param redshift_conn_id: Redshift connection ID
    :param aws_credentials_id: AWS credentials ID
    :param table: Target Redshift staging table to copy the data into
    :param s3_bucket: bucket in S3 containing the JSON files
    :param s3_key: Path in S3 bucket where the JSON files are
    :param copy_json_option: Either a JSONPaths file or 'auto', determining how to map
     elements in the JSON source data to the columns in the target table
    :param region: AWS Region where the source data is located
    """
    
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json_option="auto",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region=region

    def execute(self, context):
        self.log.info("Getting aws credentials and initializing redshift hook")
        aws_hook = AwsHook(self.aws_credentials_id)
        
        # getting credentials
        credentials = aws_hook.get_credentials()
        
        # setting up postgres hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Clearing data from target table {self.table}")
        # clearing data from the staging table
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")
        
        # format the key with the context variables if s3_key is parameterized
        rendered_key = self.s3_key.format(**context)
        
        # getfull s3 path
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info(f"Generating the full SQL statement")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_json_option
        )
        
        self.log.info(f"Running: {formatted_sql}")
        redshift.run(formatted_sql)





