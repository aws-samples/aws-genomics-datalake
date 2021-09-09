from __future__ import print_function
import json
import subprocess
import boto3
import time
import os
import cfnresponse
import logging
import traceback

paginator = boto3.client('s3').get_paginator('list_objects_v2')
s3 = boto3.client('s3')

#Function for logger
def load_log_config():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root

#Logger initiation
logger = load_log_config()

def handler(event, context):
   s3_script_path = os.environ["S3_SCRIPT"]
   output_s3 = os.environ["OUTPUT_S3"]
   hail_bucket = os.environ["HAIL_BUCKET"]
   cluster_name = os.environ["CLUSTER_NAME"]
   emr_config = os.environ["EMR_CONFIG"]
   master_type = os.environ["MASTER_TYPE"]
   core_type = os.environ["CORE_TYPE"]
   core_count = os.environ["CORE_COUNT"]
   task_type = os.environ["TASK_TYPE"]
   task_count = os.environ["TASK_COUNT"]
   subnetid = os.environ["SUBNET_ID"]
   customAmiId = os.environ["CUSTOM_AMI"]
   service_role = os.environ["EMR_SERVICE"]
   emr_ec2 = os.environ["EMR_EC2_ROLE"]
   autoscaling_role = os.environ["EMR_AUTOSCALING_ROLE"]

#splitting the emr configurations s3 path into bucket and key to read the configurations json file
   bucket = emr_config.replace("s3://","",1).split("/",1)[0]
   key = emr_config.replace("s3://","",1).split("/",1)[1]
   try:

##reading the emr configuration into json object
       response = s3.get_object(Bucket = bucket, Key = key)
       content = response["Body"]
       jsonObject = json.loads(content.read())

##Getting the bucket and key information from the event details for further processing
       lambda_message = event['Records'][0]
       inpbucket = lambda_message['s3']['bucket']['name']
       inpkey = lambda_message['s3']['object']['key']
       p_full_path = inpkey
       p_base_file_name = os.path.basename(p_full_path)
       print(p_base_file_name)
       prefix = inpkey.split('/')[0]
       input_s3 = 's3://' + inpbucket + '/' + prefix + '/'
       maxkey=10
       if not key.endswith('/'):
          pages = paginator.paginate(Bucket=inpbucket,Prefix=prefix,MaxKeys=maxkey)
          params=''

# process each batch of files (determined from maxkey)
          for page in pages:
              for obj in page['Contents']:
                  params=params+str(obj['Key']).split('/')[1]+','
              params=params[:-1]
              print(params)

# Horizontal scaling: parallel processing via multiple clusters
              cluster_id=boto3.client('emr').run_job_flow(Name=cluster_name,LogUri=f's3://{hail_bucket}/elasticmapreduce/',ReleaseLabel='emr-5.29.0', BootstrapActions=[{'Name':'pythonDateutilReinstall','ScriptBootstrapAction':{'Path':'file:/usr/bin/sudo','Args':['python3','-m','pip','install','-I','python-dateutil']}}], Instances={'InstanceGroups':[{'Name':'Master Instance Group','Market':'ON_DEMAND','InstanceRole':'MASTER','InstanceType':master_type,'InstanceCount':1, 'EbsConfiguration':{'EbsBlockDeviceConfigs':[{  'VolumeSpecification':{  'VolumeType':'gp2', 'SizeInGB':32 }, 'VolumesPerInstance':2 }]}}, {'Name':'Core Instance Group','Market':'ON_DEMAND','InstanceRole':'CORE','InstanceType':core_type,'InstanceCount':int(core_count),'EbsConfiguration':{'EbsBlockDeviceConfigs':[{  'VolumeSpecification':{  'VolumeType':'gp2', 'SizeInGB':1500 }, 'VolumesPerInstance':1 }]}},{'Name':'Task Instance Group','Market':'ON_DEMAND','InstanceRole':'TASK','InstanceType':task_type,'InstanceCount':int(task_count),'EbsConfiguration':{'EbsBlockDeviceConfigs':[{  'VolumeSpecification':{  'VolumeType':'gp2', 'SizeInGB':1500 }, 'VolumesPerInstance':1 }]}}],'KeepJobFlowAliveWhenNoSteps':False,'Ec2SubnetId':subnetid}, Applications=[{'Name':'Ganglia'},{'Name':'Hadoop'},{'Name':'Hive'},{'Name':'Livy'},{'Name':'Spark'}],Configurations=jsonObject,ServiceRole=service_role,VisibleToAllUsers=True,JobFlowRole=emr_ec2, Tags=[{'Key':'owner','Value':''},{'Key':'environment','Value':'development'},{'Key':'allow-emr-ssm','Value':'false'},{'Key':'Name','Value':'hailtest'},], Steps=[ {'Name':'hailApachePlotDir','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','mkdir','-p','/var/www/html/plots']}},{'Name':'hailMainPlotDir','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','ln','-s','/var/www/html/plots','/plots']}}, {'Name':'hailLivyPlotOwnership','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','chown','livy:livy','/var/www/html/plots']}}, {'Name':'vepOwnership','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','bash','-c','if test -d /opt/vep/; then chown -R hadoop:hadoop /opt/vep; fi']}}, {'Name':'clusterManifestToS3','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['/usr/local/bin/cluster_manifest.sh']}}, {'Name':'Spark Application','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['spark-submit','--conf','spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2','--conf','spark.sql.parquet.filterPushdown=true','--conf','spark.sql.parquet.fs.optimized.committer.optimization-enabled=true','--conf','spark.sql.catalogImplementation=hive','--conf','spark.executor.cores=1','--conf','spark.executor.instances=1','--conf','spark.sql.shuffle.partitions=1000','--conf','spark.executor.memory=10g','--jars','/opt/hail/hail-all-spark.jar',s3_script_path,params,input_s3,output_s3]}}], AutoScalingRole=autoscaling_role,ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',CustomAmiId=customAmiId,EbsRootVolumeSize=100)
              print(cluster_id['JobFlowId'])
              params=''
              time.sleep(10)
       else:
           logger.info('Event was triggered by a folder')
           logger.info('bucket: '+bucket)
           logger.info('key: '+key)
   except Exception as e:
      print(e)
      track = traceback.format_exc()
      message = {
          "ErrorMessage" : str(e),
          "StackTrace" : track,
          "BaseFileName" : p_base_file_name
      }
   return
