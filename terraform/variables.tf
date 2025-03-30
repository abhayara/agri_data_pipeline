variable "location" {
  description = "My location"
  default = "asia-south1"
}

variable "bq_dataset_name"{
    description = "Bigquery Dataset name"
    default = "agri_bigquery"  # Change this
}

variable "gcs_storage_class" {
  description = "GCS storage class name"
  default = "raw_streaming"

}

variable "gcs_bucket_name" {
  description = "GCS storage bucket name"
  default = "agri_data_tf_bucket"  # Change this (use only lowercase and hyphens)
}

