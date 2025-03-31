variable "credentials" {
    description = "The JSON file for the authhenticaton the service accont"
    default = "/home/sinatavakoli2022/Retail-Data-Pipeline/Authentication/retail-data-pipeline-453817-5c7165d921e7.json"
}

variable "project_id" {
    description = "The id for the project 'retail_data_pipeline'"
    default = "retail-data-pipeline-453817"
}

variable "region" {
    description = "The region of the project"
    default = "europe-west1"
}

variable "bucket_name" {
    description = "The name of the bucket that should be universal"
    default = "retail-data-pipeline-453817-bucket"
}

variable "dataset_name" {
    description = "The name of the  dataset"
    default = "retail_data_pipeline_dataset"
}

variable "cluster_name" {
    description = "The name of the cluster for the pyspark operation"
    default = "retail-sparl-cluster"
}

variable "cluster_region" {
    description = "The valid region for activating the dataproc"
    default = "europe-west1"
}

variable "transformed_dataset_name" {
    description = "New dataset for the transformation process"
    default = "transformed_retail_data_pipeline_dataset"
}
