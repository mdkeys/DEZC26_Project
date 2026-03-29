**About** 

Terraform is used to provision the cloud infrastructure on GCP: creates a GCS bucket (the data lake) and two BigQuery datasets (nyc_311_raw, nyc_311_prod).

**Terraform Files**

1. 'main.tf' provides the infrastructure and defines 3 resources:
   1. one GCS bucket (the data lake)
   2. two BigQuery datasets ('nyc_311_raw' and 'nyc_311_prod')
      1. Note: The buckets have a 90-day auto-delete lifecylce rule as a cost-protection measure.
2. 'variables.tf' defines the input variables so that nothing is hardcoded. Sensitive values like credentials stay out of the code.
3. 'outputs.tf' print the created resource names and URLs to the terminal after running `terraform apply`. This confirms everything was created correctly.
4. 'terraform.tfvars.template' is a template used to create 'terraform.tfvars', which contains values that will NOT be committed to git ('*.tfvars' is in .gitignore).