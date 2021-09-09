from __future__ import print_function
import json
import subprocess
import boto3
import time
import os
import cfnresponse

paginator = boto3.client('s3').get_paginator('list_objects_v2')
s3 = boto3.client('s3')

def handler(event, context):
   input_s3 = os.environ["INPUT_S3"]
   s3_script_path = os.environ["S3_SCRIPT"]
   samples_per_cluster = os.environ["SAMPLES_PER_CLUSTER"]
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

#splitting the input s3 path into bucket and key to paginate over the sample files and group them based on number of samples per cluster
   inpbucket = input_s3.replace("s3://","",1).split("/",1)[0]
   inpkey = input_s3.replace("s3://","",1).split("/",1)[1].rsplit("/",1)[0]
   outputData = {}
   try:
      response = s3.get_object(Bucket = bucket, Key = key)
      content = response["Body"]
      jsonObject = json.loads(content.read())
      maxkey=int(samples_per_cluster)
      pages = paginator.paginate(Bucket=inpbucket,Prefix=inpkey,MaxKeys=maxkey)
      params=''
# process each batch of files (determined from maxkey)
      for page in pages:
         for obj in page['Contents']:
             params=params+str(obj['Key']).split("/")[1]+","
         params=params[:-1]
         print(params)

# Horizontal scaling: parallel processing via multiple clusters
         cluster_id=boto3.client('emr').run_job_flow(Name=cluster_name,LogUri=f's3://{hail_bucket}/elasticmapreduce/',ReleaseLabel='emr-5.29.0', BootstrapActions=[{'Name':'pythonDateutilReinstall','ScriptBootstrapAction':{'Path':'file:/usr/bin/sudo','Args':['python3','-m','pip','install','-I','python-dateutil']}}], Instances={'InstanceGroups':[{'Name':'Master Instance Group','Market':'ON_DEMAND','InstanceRole':'MASTER','InstanceType':master_type,'InstanceCount':1, 'EbsConfiguration':{'EbsBlockDeviceConfigs':[{  'VolumeSpecification':{  'VolumeType':'gp2', 'SizeInGB':32 }, 'VolumesPerInstance':2 }]}}, {'Name':'Core Instance Group','Market':'ON_DEMAND','InstanceRole':'CORE','InstanceType':core_type,'InstanceCount':int(core_count),'EbsConfiguration':{'EbsBlockDeviceConfigs':[{  'VolumeSpecification':{  'VolumeType':'gp2', 'SizeInGB':1500 }, 'VolumesPerInstance':1 }]}},{'Name':'Task Instance Group','Market':'ON_DEMAND','InstanceRole':'TASK','InstanceType':task_type,'InstanceCount':int(task_count),'EbsConfiguration':{'EbsBlockDeviceConfigs':[{  'VolumeSpecification':{  'VolumeType':'gp2', 'SizeInGB':1500 }, 'VolumesPerInstance':1 }]}}],'KeepJobFlowAliveWhenNoSteps':False,'Ec2SubnetId':subnetid}, Applications=[{'Name':'Ganglia'},{'Name':'Hadoop'},{'Name':'Hive'},{'Name':'Livy'},{'Name':'Spark'}],Configurations=jsonObject,ServiceRole=service_role,VisibleToAllUsers=True,JobFlowRole=emr_ec2, Tags=[{'Key':'owner','Value':''},{'Key':'environment','Value':'development'},{'Key':'allow-emr-ssm','Value':'false'},{'Key':'Name','Value':'hailtest'},], Steps=[ {'Name':'hailApachePlotDir','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','mkdir','-p','/var/www/html/plots']}},{'Name':'hailMainPlotDir','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','ln','-s','/var/www/html/plots','/plots']}}, {'Name':'hailLivyPlotOwnership','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','chown','livy:livy','/var/www/html/plots']}}, {'Name':'vepOwnership','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','bash','-c','if test -d /opt/vep/; then chown -R hadoop:hadoop /opt/vep; fi']}}, {'Name':'clusterManifestToS3','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['/usr/local/bin/cluster_manifest.sh']}}, {'Name':'Spark Application','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['spark-submit','--conf','spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2','--conf','spark.sql.parquet.filterPushdown=true','--conf','spark.sql.parquet.fs.optimized.committer.optimization-enabled=true','--conf','spark.sql.catalogImplementation=hive','--conf','spark.executor.cores=1','--conf','spark.executor.instances=1','--conf','spark.sql.shuffle.partitions=1000','--conf','spark.executor.memory=10g','--jars','/opt/hail/hail-all-spark.jar',s3_script_path,params,input_s3,output_s3]}}], AutoScalingRole=autoscaling_role,ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',CustomAmiId=customAmiId,EbsRootVolumeSize=100)
         print(cluster_id['JobFlowId'])
         outputData['Result'] = cluster_id['JobFlowId']
         cfnresponse.send(event, context, cfnresponse.SUCCESS, outputData, {})
         params=''
         time.sleep(10)
   except Exception as e:
      print(e)
      cfnresponse.send(event, context, cfnresponse.FAILED, { 'error': str(e) }, {})
   return
