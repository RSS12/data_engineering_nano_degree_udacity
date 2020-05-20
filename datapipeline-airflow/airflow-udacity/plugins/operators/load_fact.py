from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                table = "",
                truncate_table = False,
                query = "",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_table = truncate_table
        self.query =  query


    def execute(self, context):
        self.log.info('LoadFactOperation starting execution function')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table==True :
            self.log.info(f'Truncating Table {self.table}')
            redshift.run("DELETE FROM {}".format(self.table))
        else:
            pass    

        self.log.info(f'Running query {self.query}')
        redshift.run(f"Insert into {self.table} {self.query}")


