import logging

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HttpOperator(BaseOperator):

    template_fields = ('endpoint', 'data', 'headers',)
    template_ext = ()

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 method='POST',
                 data=None,
                 headers=None,
                 response_check=None,
                 extra_options=None,
                 xcom_push_flag=False,
                 http_conn_id='http_default',
                 *args, **kwargs):
        """
        If xcom_push is True, response of an HTTP request will also
        be pushed to an XCom.
        """
        super(HttpOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.xcom_push_flag = xcom_push_flag

    def execute(self, context):
        http_hook = HttpHook(
            self.method, http_conn_id=self.http_conn_id)

        logging.info("Getting response...")
        response = http_hook.run(endpoint=self.endpoint,
                                 data=self.data,
                                 headers=self.headers,
                                 extra_options=self.extra_options)
        logging.info(response.json())
        if self.xcom_push_flag:
            logging.info("Pushing data to XCOM")
            return {'response': response.json()}
