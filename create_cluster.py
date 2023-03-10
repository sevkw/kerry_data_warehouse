import pandas as pd
import boto3
import json
import configparser

aws_region = "us-west-2" 

#load params from cfg file

config = configparser.ConfigParser()
config.read_file(open('aws.cfg'))
# use config.get() to obtain Key from dictionary AWS section
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
# use config.get() to obtain other items from DWH section
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

tbl_param = pd.DataFrame({"Param":
									["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
							"Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
							})

print(tbl_param.head(10))

# create clients for EC2, S3, IAM, and Redshift
ec2 = boto3.resource('ec2',
                       region_name=aws_region,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name=aws_region,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name=aws_region
                  )

redshift = boto3.client('redshift',
                       region_name=aws_region,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

#check out sample data source on s3

print("Check out sample data source.")

sampleDbBucket =  s3.Bucket("awssampledbuswest2")
for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
    print(obj)

print("Check complete")

# create IAM role
## uncomment the following section to create a role
from botocore.exceptions import ClientError

# #1.1 Create the role
# try:
#     print("1.1 Creating a new IAM Role") 
#     dwhRole = iam.create_role(
#         Path='/',
#         RoleName=DWH_IAM_ROLE_NAME,
#         Description = "Allows Redshift clusters to call AWS services on your behalf.",
#         AssumeRolePolicyDocument=json.dumps(
#             {'Statement': [{'Action': 'sts:AssumeRole',
#                'Effect': 'Allow',
#                'Principal': {'Service': 'redshift.amazonaws.com'}}],
#              'Version': '2012-10-17'})
#     )    
# except Exception as e:
#     print(e)
    
    
# print("1.2 Attaching Policy")

# iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
#                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
#                       )['ResponseMetadata']['HTTPStatusCode']

# print("1.3 Get the IAM role ARN")
# roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

# print(roleArn)

# Create Redshift Cluster
config.read('dwh.cfg')
role_arn = config.get("IAM_ROLE", "ARN")

try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[role_arn]  
    )
except Exception as e:
    print(e)

print("Cluster is creating...")

# the following returns the status of the cluster
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(prettyRedshiftProps(myClusterProps).head(10))
