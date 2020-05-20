from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON '{}'
        """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.json_path = json_path
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')



    def execute(self, context):
        """
            loads data from S3 buckets to redshift  in staging tables.
                - redshift_conn_id: redshift cluster connection
                - aws_credentials_id: AWS connection
                - table: redshift cluster table name
                - s3_bucket: S3 bucket of source
                - s3_key: S3 keys of source 
             
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("DELETING DATA FROM REDSHIFT TABLE")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("LOADING DATA FROM  S3 TO REDSHIFT")


        rendered_s3_key = self.s3_key.format(**context)
        if self.execution_date:
            year =self.execution_date.strftime("%Y")
            month =self.execution_date.strftime("%m")
            day =self.execution_date.strftime("%d")
            
            s3_path =  f"s3://{self.s3_bucket}/{year}/{month}/{day}/{rendered_s3_key}"
        else:
            s3_path = f"s3://{self.s3_bucket}/{rendered_s3_key}"



        
        stmt = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )

        redshift.run(stmt)

        self.log.info(f"SUCCESS:  {self.table} LOADED FROM {s3_path} TO  REDSHIFT")





