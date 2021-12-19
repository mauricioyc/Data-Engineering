from __future__ import absolute_import, division, print_function

from airflow.plugins_manager import AirflowPlugin

from operators.data_quality import DataQualityOperator
from operators.helpers.sql_queries import SqlQueries
from operators.http_operator import HttpOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.mtgjson_operator import MtgJsonOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.upload_s3_operator import UploadS3Operator


# Defining the plugin class
class MtgPlugin(AirflowPlugin):
    name = "mtg_plugin"
    operators = [
        HttpOperator,
        UploadS3Operator,
        MtgJsonOperator,
        StageToRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator,
        SqlQueries
    ]
