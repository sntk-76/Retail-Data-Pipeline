terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {

  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region
}


# create a google cloud storage 

resource "google_storage_bucket" "retail_data_bucket" {
  name = var.bucket_name
  location = var.region
  project = var.project_id
  force_destroy = true	
}

# create a google cloud bigquery

resource "google_bigquery_dataset" "retail_dataset" {
  dataset_id = var.dataset_name
  location = var.region
  project = var.project_id
 }


resource "google_bigquery_dataset" "transformed_retail_dataset" {

  dataset_id = var.transformed_dataset_name
  location = var.region
  project = var.project_id
}
#create a dataproc cluster for spark

resource "google_dataproc_cluster" "spark_cluster" {
  name   = var.cluster_name
  region = var.cluster_region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_size_gb = 40
      }
    }
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_size_gb = 40
      }
    }
  }
}

