variable "region" {
  description = "AWS region hosting the pipeline"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Environment name used for tagging and resource naming"
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  description = "CIDR block for the pipeline VPC"
  type        = string
  default     = "10.42.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR block."
  }
}

variable "availability_zone_count" {
  description = "Number of availability zones to spread every tier across"
  type        = number
  default     = 3

  validation {
    condition     = var.availability_zone_count >= 2 && var.availability_zone_count <= 6
    error_message = "availability_zone_count must be between 2 and 6 so no tier is single homed."
  }
}

variable "kafka_broker_count" {
  description = "MSK broker count; must be a multiple of the availability zone count"
  type        = number
  default     = 3
}

variable "kafka_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_volume_size_gb" {
  description = "EBS volume per MSK broker, sized for at least 24 hours of retention"
  type        = number
  default     = 1000
}

variable "kafka_partitions" {
  description = "Default partition count for device topics, the primary shard axis; see docs/capacity.md for the arithmetic behind this figure"
  type        = number
  default     = 192
}

variable "redis_node_type" {
  description = "ElastiCache node type for the hot state cluster"
  type        = string
  default     = "cache.r7g.large"
}

variable "redis_shards" {
  description = "Number of Redis Cluster shards; docs/capacity.md sizes 6 for one million devices"
  type        = number
  default     = 6
}

variable "redis_replicas_per_shard" {
  description = "Replicas per shard, required for automatic failover"
  type        = number
  default     = 1

  validation {
    condition     = var.redis_replicas_per_shard >= 1
    error_message = "At least one replica per shard is required for automatic failover."
  }
}

variable "postgres_instance_class" {
  description = "RDS instance class for the durable archive"
  type        = string
  default     = "db.m6g.large"
}

variable "postgres_allocated_storage_gb" {
  description = "Initial RDS storage in GiB"
  type        = number
  default     = 200
}

variable "eks_version" {
  description = "EKS control plane version"
  type        = string
  default     = "1.31"
}

variable "node_instance_types" {
  description = "Instance types for the managed node group"
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "node_desired_size" {
  description = "Desired managed node count"
  type        = number
  default     = 3
}

variable "node_max_size" {
  description = "Maximum managed node count for scale out"
  type        = number
  default     = 30
}
