from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table,
                 redshift_conn_id,
                 aws_credentials_id,
                 s3_bucket,
                 s3_key,
                 region = "us-west-2",
                 json_path="",
                 delimiter=",",
                 ignore_headers=1,                 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.json_path=json_path
        
    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Cleaning Redshift Table '{self.table}'")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copying data from S3 to Redshift Table '{self.table}'")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        redshift.run(f"""
            COPY {self.table} 
            FROM '{s3_path}' 
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.json_path}'
            """
        )





