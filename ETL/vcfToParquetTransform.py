#pyspark script to convert VCF to parquet

import hail as hl
from bokeh.io import show, output_notebook
from bokeh.layouts import gridplot
from pprint import pprint
from  pyspark.sql.functions import col, lit
import os
import sys
from pyspark.sql import SparkSession
import subprocess

sample_id = str(sys.argv[1]).split(',')
input_path = sys.argv[2]
print (input_path)
output_path = sys.argv[3]
print (output_path)

hl.init()
spark = SparkSession.builder.getOrCreate()

for i in sample_id:
	#filename = input_path + i + '/' + i + '.hard-filtered.vcf.bgz'
	filename = input_path + '/' + i
	#if "additional_698_related" in filename:
	#	continue
	#else:
		#sample_id = os.path.basename(filename).replace(".hard-filtered.vcf.bgz","")
	#sample_id_=str(i.split('/')[-1].split('.')[0])
	vds=hl.import_vcf(filename,reference_genome='GRCh38')
	sample_id=vds.s.take(1)[0]
	v_spark = vds.make_table().to_spark()
	#v_spark_renamed=v_spark.toDF("locus.contig","locus.position","alleles","rsid","qual","filters",'info.AC','info.AF','info.AN','info.DB','info.DP','info.END','info.FS','info.FractionInformativeReads','info.LOD','info.MQ','info.MQRankSum','info.QD','info.R2_5P_bias','info.ReadPosRankSum','info.SOR',"AD","AF","DP","FIR2","F2R1","GP","GQ","GT.alleles","GT.phased","MB","PL","PRI","PS","SB","SQ")
	for name in v_spark.schema.names:
		if str(sample_id) in name:
			v_spark = v_spark.withColumnRenamed(name,name.split('.',1)[1])
	v_spark=v_spark.withColumn("sample_id",lit(sample_id)).withColumn('ref',v_spark.alleles[0]).withColumn('alt',v_spark.alleles[1])
	v_spark = v_spark.withColumnRenamed("locus.contig","chrom").withColumnRenamed("locus.position","pos")
	v_spark.createOrReplaceTempView("variant_v")
	df=spark.sql("SELECT concat(chrom, ':', ref, ':', alt, ':', cast(pos as string)) as variant_id,* FROM variant_v where length(chrom)<6")
	df.write.parquet(output_path + sample_id +'/')
