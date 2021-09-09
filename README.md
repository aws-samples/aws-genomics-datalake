# Build a Genomics Data Lake on AWS

This repo contains the code referenced in the AWS blog post "Build a Genomics data lake on AWS". 

#### ETL - contains the transformation scripts and the cloudformation template to spin up the EMR cluster

**EMRGenomics.py** - Lambda function that is triggered by the cloudFormation template to create EMR cluster to process VCFs.

**EventEMRGenomics.py** - Event trigger Lambda function

**emr_config.json** - JSON file with EMR configuration for this example. This file can be edited to change EMR configuration parameters.

**vcfToParquetTransform.py** - pySpark script that performs the VCF to parquet transformation using the Hail API. This can be customized to perform any specific transformation steps required.

**genomics_datalake_emr.template** - Cloudformation template that can be deployed in your account for the solution.

#### 1000Genomes.ipynb - Python notebook with sample queries

**This repo also has the CloudFormation Templates to create Glue Data Catalog entries for the transformed 1000 genomes and gnomAD data. For more details on this, check out the DataLakeAsCode repo at https://github.com/aws-samples/data-lake-as-code/tree/roda#readme
