import logging

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HttpOperator(BaseOperator):
    """
    Makes an API requests for a givin endpoint. If xcom flag is set to True,
    the response is saved in the context.

    Args:
        endpoint (str): API endpoint.
        method (str): Request method.
        data (str): Data passed in the request.
        headers (str): Headers passed in the request.
        extra_options (obj): Extra options passed in the request.
        xcom_push_flag (boolean): If True return the response to xcom.
        http_conn_id (str): Connection id in Airflow connections.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.

    Attributes:
        endpoint (str): API endpoint.
        method (str): Request method.
        data (str): Data passed in the request.
        headers (str): Headers passed in the request.
        extra_options (obj): Extra options passed in the request.
        xcom_push_flag (boolean): If True return the response to xcom.
        http_conn_id (str): Connection id in Airflow connections.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.
    """
    template_fields = ('endpoint', 'data', 'headers',)
    template_ext = ()

    ui_color = '#E83845'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 method='POST',
                 data=None,
                 headers=None,
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
        self.extra_options = extra_options or {}
        self.xcom_push_flag = xcom_push_flag

    def execute(self, context):
        """
        Creates a Http Hook and get response from endpoint.

        Args:
            context (obj): context from run enviroment.
        """
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
