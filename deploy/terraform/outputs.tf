output "cluster_name" {
  description = "EKS cluster hosting the pipeline"
  value       = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint"
  value       = aws_eks_cluster.this.endpoint
}

output "kafka_bootstrap_brokers_tls" {
  description = "MSK TLS bootstrap brokers for the ironclaw -kafka flag"
  value       = aws_msk_cluster.this.bootstrap_brokers_tls
}

output "redis_configuration_endpoint" {
  description = "Redis Cluster configuration endpoint for the ironclaw -redis flag"
  value       = aws_elasticache_replication_group.redis.configuration_endpoint_address
}

output "postgres_endpoint" {
  description = "Writer endpoint for the durable archive"
  value       = aws_db_instance.postgres.endpoint
}

output "postgres_replica_endpoint" {
  description = "Read replica endpoint for analytical queries"
  value       = aws_db_instance.postgres_replica.endpoint
}

output "device_ca_arn" {
  description = "Private CA issuing device, edge and ingest certificates"
  value       = aws_acmpca_certificate_authority.this.arn
}

output "crl_bucket" {
  description = "Bucket holding the certificate revocation list consumed by HAProxy"
  value       = aws_s3_bucket.crl.id
}

output "redis_auth_secret_arn" {
  description = "Secrets Manager entry holding the Redis AUTH token"
  value       = aws_secretsmanager_secret.redis_auth.arn
}

output "postgres_secret_arn" {
  description = "Secrets Manager entry holding the Postgres credentials"
  value       = aws_secretsmanager_secret.postgres.arn
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider backing IRSA for pipeline service accounts"
  value       = aws_iam_openid_connect_provider.this.arn
}
