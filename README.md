# Build a Genomics Data Lake on AWS

This repo contains the code referenced in the AWS blog post "Build a Genomics data lake on AWS". 

#### ETL - contains the transformation scripts and the cloudformation template to spin up the EMR cluster

**EMRGenomics.py** - Lambda function that is triggered by the cloudFormation template to create EMR cluster to process VCFs.

**EventEMRGenomics.py** - Event trigger Lambda function

**emr_config.json** - JSON file with EMR configuration for this example. This file can be edited to change EMR configuration parameters.

**vcfToParquetTransform.py** - pySpark script that performs the VCF to parquet transformation using the Hail API. This can be customized to perform any specific transformation steps required.

**genomics_datalake_emr.template** - Cloudformation template that can be deployed in your account for the solution.

#### 1000Genomes.ipynb - Python notebook with sample queries

**For instructions on how to create the Glue data catalog tables for 1000 Genomes on the Registry of Open Data, please check the DataLakeAsCode repo at https://github.com/aws-samples/data-lake-as-code/tree/roda#readme. The repo also has CloudFormation templates for ClinVar and gnomAD.**
