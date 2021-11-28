import configparser
import json
import logging

import boto3
import pandas as pd

logger = logging.getLogger('spam.auxiliary.ConfigureRedshift')


class ConfigureRedshift():
    """
    Class that sets the ec2, s3, redshift and iam parameters to create and
    delete a cluster and role for a redshift dwh.
    """

    def __init__(self, config_path='./aws.cfg'):
        """
        Creates the ec2, s3, redshift and iam parameters from a configuration
        file in a given path.
        """
        self.config_path = config_path

        config = configparser.ConfigParser()
        config.read_file(open(self.config_path))

        self.KEY = config.get('AWS', 'KEY')
        self.SECRET = config.get('AWS', 'SECRET')

        self.DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
        self.DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

        self.DWH_CLUSTER_IDENTIFIER = config.get(
            "DWH", "DWH_CLUSTER_IDENTIFIER")
        self.DWH_DB = config.get("DWH", "DWH_DB")
        self.DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
        self.DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
        self.DWH_PORT = config.get("DWH", "DWH_PORT")

        self.DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

        self.ec2 = boto3.resource('ec2',
                                  region_name="us-west-2",
                                  aws_access_key_id=self.KEY,
                                  aws_secret_access_key=self.SECRET
                                  )

        self.s3 = boto3.resource('s3',
                                 region_name="us-west-2",
                                 aws_access_key_id=self.KEY,
                                 aws_secret_access_key=self.SECRET
                                 )

        self.iam = boto3.client('iam', aws_access_key_id=self.KEY,
                                aws_secret_access_key=self.SECRET,
                                region_name='us-west-2'
                                )

        self.redshift = boto3.client('redshift',
                                     region_name="us-west-2",
                                     aws_access_key_id=self.KEY,
                                     aws_secret_access_key=self.SECRET
                                     )

    def create_role(self):
        """
        - Creates a IAM Role.
        - Attach policy for S3 read.
        - Save the ARN role in a class parameter.
        """
        try:
            logger.info("1.1 Creating a new IAM Role")

            dwhRole = self.iam.create_role(
                Path='/',
                RoleName=self.DWH_IAM_ROLE_NAME,
                Description="Allows Redshift clusters to call AWS services on \
                    your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                                    'Effect': 'Allow',
                                    'Principal': {'Service':
                                                  'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )

            logger.info("1.2 Attaching Policy")

            self.iam.attach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME,
                                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                        )['ResponseMetadata']['HTTPStatusCode']

            logger.info("1.3 Get the IAM role ARN")

            self.roleArn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)[
                'Role']['Arn']

        except Exception as e:
            logger.error(f"Error: {e}")
            self.roleArn = "Failed"

        return(self.roleArn)

    def create_redshift_cluster(self):
        """
        Create a Redshift cluster with the configuration given.
        """
        logger.info("CREATE CLUSTER")
        try:
            response = self.redshift.create_cluster(
                # HW
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                # Identifiers & Credentials
                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,

                # Roles (for s3 access)
                IamRoles=[self.roleArn]
            )

            logger.debug(response)

            status = response
        except Exception as e:
            logger.error(e)
            status = e

        return(status)

    def create_tcp_cluster(self):
        """
        Allows TCP inbound connection in the cluster security group.
        """
        logger.info("Add TCP to Cluster")
        try:
            props = self.get_cluster_props()
            vpc = self.ec2.Vpc(id=props['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            logger.debug(defaultSg)

            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT)
            )
        except Exception as e:
            logger.error(e)

    def get_cluster_props(self):
        """
        Gets cluster description information.
        """
        self.props = self.redshift.describe_clusters(
            ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        return(self.props)

    def delete_cluster(self):
        """
        Deletes the created cluster.
        """
        logger.info("Deleting Cluster")
        self.redshift.delete_cluster(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                                     SkipFinalClusterSnapshot=True)

    def delete_role(self):
        """
        Deletes the created role.
        """
        logger.info("Deleting Role")
        self.iam.detach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME,
                                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        self.iam.delete_role(RoleName=self.DWH_IAM_ROLE_NAME)

    @staticmethod
    def prettyRedshiftProps(props):
        """
        Makes the cluster description props readable in a pandas DataFrame.
        """
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                      "MasterUsername", "DBName", "Endpoint", "NumberOfNodes",
                      'VpcId']
        x = [(k, v) for k, v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    @staticmethod
    def get_endopint(props):
        """
        Gets cluster endpoint and role ARN.
        """
        return({'endpoint': props['Endpoint']['Address'],
                'arn': props['IamRoles'][0]['IamRoleArn']})
