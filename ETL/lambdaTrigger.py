import subprocess
import boto3
import time
import os
paginator = boto3.client('s3').get_paginator('list_objects_v2')
#s3 = boto3.client('s3')

def lambda_handler(event, context):
	key = event['Records'][0]['s3']['object']['key']
	if 'SUCCESS' in key:
		cntPages=paginator.paginate(Bucket='ab3',Prefix='dev_input_vcf/',MaxKeys=999)
		output=0
		for pagec in cntPages:
			for obj in pagec['Contents']:
				if 'bgz' in str(obj['Key']):
					output=output+1
		cluster_name='hailtest'
		emr_log_location='s3n://coviddatasalaunch/elasticmapreduce/'
		master_instance_type='m5.xlarge'
		master_instance_count=1
		core_instance_type='r5.4xlarge'
		core_instance_count=1
		ec2_key_name='gnome'
		subnet_id='subnet-0f879ae628c3afabf'
		master_sg='sg-036df76a9e89e6faa'
		slave_sg='sg-0475387164011d26f'
		service_access_sg='sg-0d58818a3c1d3b272'
		additional_master_sg='sg-0bb3a94d545296e87'
		additional_slave_sg='sg-0478dc8c6eac415de'
		autoscaling_role='emr-autoscaling-hailtest'
		scale_down_behavior='TERMINATE_AT_TASK_COMPLETION'
		custom_ami_id='ami-0f33e21674eed03c6'
		num_file_per_cluster=26

	# determine the number of files you want to process in one cluster / multiple clusters
		maxkey=int(int(output)/2)

	# script which converts vcf to parquet
		s3_script_path='s3://ab3/dev_artifacts/vcf_parquet_transform_dev.py'
		print(output,maxkey)
		pages = paginator.paginate(Bucket='ab3',Prefix='dev_input_vcf/',MaxKeys=maxkey)
		params=''
	# process each batch of files (determined from maxkey)
		for page in pages:
			for obj in page['Contents']:
				if 'bgz' in str(obj['Key']):
					params=params+str(obj['Key']).split("/")[1]+","
			#params='\"'+params[:-1]+'\"'
			params=params[:-1]
			print(params)

			# Horizontal scaling: parallel processing via multiple clusters

		cluster_id=boto3.client('emr',region_name='us-east-1').run_job_flow(Name=cluster_name,LogUri=emr_log_location,ReleaseLabel='emr-5.29.0',BootstrapActions=[{'Name':'pythonDateutilReinstall','ScriptBootstrapAction':{'Path':'file:/usr/bin/sudo','Args':['python3','-m','pip','install','-I','python-dateutil']}}],Instances={'InstanceGroups':[{'Name':'Master Instance Group','Market':'ON_DEMAND','InstanceRole':'MASTER','InstanceType':master_instance_type,'InstanceCount':master_instance_count},{'Name':'Core Instance Group','Market':'ON_DEMAND','InstanceRole':'CORE','InstanceType':core_instance_type,'InstanceCount':core_instance_count}],'Ec2KeyName':ec2_key_name,'KeepJobFlowAliveWhenNoSteps':True,'Ec2SubnetId':subnet_id,'EmrManagedMasterSecurityGroup':master_sg,'EmrManagedSlaveSecurityGroup':slave_sg,'ServiceAccessSecurityGroup':service_access_sg,'AdditionalMasterSecurityGroups':[additional_master_sg],'AdditionalSlaveSecurityGroups':[additional_slave_sg]},Applications=[{'Name':'Ganglia'},{'Name':'Hadoop'},{'Name':'Hive'},{'Name':'Livy'},{'Name':'Spark'}],Configurations=[{'Classification':'spark','Properties':{'maximizeResourceAllocation':'true'}},{'Classification':'spark-defaults','Properties':{'spark.executor.extraClassPath':'/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar:/opt/hail/hail-all-spark.jar','spark.kryo.registrator':'is.hail.kryo.HailKryoRegistrator','spark.driver.extraClassPath':'/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar:/opt/hail/hail-all-spark.jar','spark.serializer':'org.apache.spark.serializer.KryoSerializer'}},{'Classification':'livy-conf','Properties':{'livy.file.local-dir-whitelist':'/home/hadoop/','livy.server.session.timeout-check':'false','livy.server.session.timeout':'168h'}},{'Classification':'spark-env','Properties':{},'Configurations':[{'Classification':'export','Properties':{'PYSPARK_PYTHON':'/usr/bin/python3'}}]},{'Classification':'yarn-env','Properties':{},'Configurations':[{'Classification':'export','Properties':{'PYSPARK_PYTHON':'/usr/bin/python3'}}]},{'Classification':'yarn-site','Properties':{'yarn.log-aggregation.retain-seconds':'3600'}},{'Classification':'emrfs-site','Properties':{'fs.s3.maxConnections':'1000'}}],ServiceRole='emr-cluster-hailtest',VisibleToAllUsers=True,JobFlowRole='emr-ec2-hailtest',Tags=[{'Key':'owner','Value':''},{'Key':'environment','Value':'development'},{'Key':'allow-emr-ssm','Value':'false'},{'Key':'Name','Value':'hailtest'},],Steps=[{'Name':'Spark Application','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['spark-submit','--conf','spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2','--conf','spark.sql.parquet.filterPushdown=true','--conf','spark.sql.parquet.fs.optimized.committer.optimization-enabled=true','--conf','spark.sql.catalogImplementation=hive','--conf','spark.executor.cores=1','--conf','spark.executor.instances=1','--conf','spark.sql.shuffle.partitions=1000','--conf','spark.executor.memory=10g','--jars','/opt/hail/hail-all-spark.jar',s3_script_path,params]}},{'Name':'hailApachePlotDir','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo,mkdir','-p','/var/www/html/plots']}},{'Name':'hailMainPlotDir','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','ln','-s','/var/www/html/plots','/plots']}},{'Name':'hailLivyPlotOwnership','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','chown','livy:livy','/var/www/html/plots']}},{'Name':'vepOwnership','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['sudo','bash','-c','if test -d /opt/vep/; then chown -R hadoop:hadoop /opt/vep; fi']}},{'Name':'clusterManifestToS3','ActionOnFailure':'CONTINUE','HadoopJarStep':{'Jar':'command-runner.jar','Args':['/usr/local/bin/cluster_manifest.sh']}}],AutoScalingRole=autoscaling_role,ScaleDownBehavior=scale_down_behavior,CustomAmiId=custom_ami_id,EbsRootVolumeSize=100)

			#print(step_id['StepIds'][0])
		print(cluster_id['JobFlowId'])
		params=''
		time.sleep(10)
