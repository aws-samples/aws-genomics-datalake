import sys, os
from pyspark import SparkContext, SparkConf
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
#from hail import *
import hail as hl


conf = SparkConf()
conf.set('spark.app.name', u'Running Hail on Glue')
conf.set('spark.sql.files.maxPartitionBytes', '1099511627776')
conf.set('spark.sql.files.openCostInBytes', '1099511627776')
conf.set('spark.kryo.registrator', 'is.hail.kryo.HailKryoRegistrator')
conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')

sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
sc.getConf().getAll()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#job.init(args['JOB_NAME'], args)
#hc = HailContext(sc)
hl.init(sc)
filename='s3://coviddatasalaunch/data/dragen-3.5.7b/hg38_altaware_nohla-cnv-anchored/HG00096/HG00096.hard-filtered.vcf.bgz'
vds=hl.import_vcf(filename,reference_genome='GRCh38')

print("Hello World!!!")
