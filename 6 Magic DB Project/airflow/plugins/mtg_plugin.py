from __future__ import absolute_import, division, print_function

from airflow.plugins_manager import AirflowPlugin

from operators.http_operator import HttpOperator
from operators.mtgjson_operator import MtgJsonOperator
from operators.upload_s3_operator import UploadS3Operator


# Defining the plugin class
class MtgPlugin(AirflowPlugin):
    name = "mtg_plugin"
    operators = [
        HttpOperator,
        UploadS3Operator,
        MtgJsonOperator
    ]
