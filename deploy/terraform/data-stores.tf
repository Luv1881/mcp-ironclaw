resource "aws_kms_key" "pipeline" {
  description             = "IronClaw pipeline encryption at rest"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "pipeline" {
  name          = "alias/${local.name}"
  target_key_id = aws_kms_key.pipeline.key_id
}

resource "aws_cloudwatch_log_group" "kafka" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.pipeline.arn
}

resource "aws_msk_configuration" "this" {
  name           = "${local.name}-config"
  kafka_versions = ["3.6.0"]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=${var.kafka_partitions}
    log.retention.hours=24
    unclean.leader.election.enable=false
  PROPERTIES
}

resource "aws_msk_cluster" "this" {
  cluster_name           = local.name
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.kafka_broker_count

  broker_node_group_info {
    instance_type   = var.kafka_instance_type
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.kafka.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.kafka_volume_size_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  client_authentication {
    tls {
      certificate_authority_arns = [aws_acmpca_certificate_authority.this.arn]
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.pipeline.arn

    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.kafka.name
      }
    }
  }

  tags = {
    Name = local.name
  }

  depends_on = [aws_acmpca_certificate_authority_certificate.root]
}

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${local.name}-redis"
  subnet_ids = aws_subnet.private[*].id
}

resource "random_password" "redis_auth" {
  length  = 48
  special = false
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${local.name}-redis"
  description          = "IronClaw hot state, sharded by user hash tag"

  engine         = "redis"
  engine_version = "7.1"
  node_type      = var.redis_node_type
  port           = 6379

  num_node_groups         = var.redis_shards
  replicas_per_node_group = var.redis_replicas_per_shard

  automatic_failover_enabled = true
  multi_az_enabled           = true

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = random_password.redis_auth.result
  kms_key_id                 = aws_kms_key.pipeline.arn

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [aws_security_group.redis.id]

  maintenance_window       = "sun:05:00-sun:06:00"
  snapshot_retention_limit = 7
  snapshot_window          = "03:00-04:00"

  tags = {
    Name = "${local.name}-redis"
  }
}

resource "aws_db_subnet_group" "postgres" {
  name       = "${local.name}-postgres"
  subnet_ids = aws_subnet.private[*].id
}

resource "random_password" "postgres" {
  length  = 48
  special = false
}

resource "aws_db_instance" "postgres" {
  identifier     = "${local.name}-postgres"
  engine         = "postgres"
  engine_version = "16.4"
  instance_class = var.postgres_instance_class

  allocated_storage     = var.postgres_allocated_storage_gb
  max_allocated_storage = var.postgres_allocated_storage_gb * 10
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.pipeline.arn

  db_name  = "ironclaw"
  username = "ironclaw"
  password = random_password.postgres.result

  multi_az               = true
  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.postgres.id]

  backup_retention_period = 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:05:00-sun:06:00"

  deletion_protection       = true
  delete_automated_backups  = false
  skip_final_snapshot       = false
  final_snapshot_identifier = "${local.name}-postgres-final"

  performance_insights_enabled = true
  auto_minor_version_upgrade   = true

  tags = {
    Name = "${local.name}-postgres"
  }
}

resource "aws_db_instance" "postgres_replica" {
  identifier          = "${local.name}-postgres-replica"
  replicate_source_db = aws_db_instance.postgres.identifier
  instance_class      = var.postgres_instance_class

  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  storage_encrypted      = true
  kms_key_id             = aws_kms_key.pipeline.arn

  publicly_accessible = false
  skip_final_snapshot = true

  tags = {
    Name = "${local.name}-postgres-replica"
  }
}
