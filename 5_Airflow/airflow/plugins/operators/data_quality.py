from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 #tables=[],  
                 sql_stmt = [],
                 expected_result = [],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.expected_result = expected_result
        #self.tables = tables
        
    def execute(self, context):
        redshift = PostgresHook(
            postgres_conn_id=self.redshift_conn_id
        )
        #self.log.info('DataQualityOperator not implemented yet')
        # hard coded initial test
        """
        for table in self.tables:
            self.log.info(f'Checking "{table}" Redshift table')
            
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records[0]) < 1 or len(records) < 1:
                raise ValueError(f"Data quality check failed: Table {table} has no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed: Table {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")        
        """
        
        self.log.info(f'Running data quality checks')
        
        tests = zip(self.sql_stmt, self.expected_result)
        for query, expected in tests:
            self.log.info(f'Run data quality query: {query}')
            records = redshift.get_records(query)
            try:
                result=records[0][0]
            except:
                result = None
            self.log.info(f'Result: {result}')
            if result == expected:
                self.log.info('Data quality check passed.')
            else:
                self.log.info('Data quality check failed.')
                raise ValueError('Data quality check failed.')            
        
            
            