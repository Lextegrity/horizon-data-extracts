CREATE storage integration export_s3_int
  type = external_stage
  storage_provider = 'S3'
  storage_aws_role_arn = 'arn:aws:iam::463717823351:role/snowfake-s3-cross-account-role'
  enabled = true
  storage_allowed_locations = ('s3://stagevpc-environments3bucketd212651c-1ie9g30pdj5d6/data/');
