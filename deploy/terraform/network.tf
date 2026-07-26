data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name = "ironclaw-${var.environment}"

  azs = slice(data.aws_availability_zones.available.names, 0, var.availability_zone_count)

  public_subnets  = [for index in range(var.availability_zone_count) : cidrsubnet(var.vpc_cidr, 8, index)]
  private_subnets = [for index in range(var.availability_zone_count) : cidrsubnet(var.vpc_cidr, 8, index + 100)]
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = local.name
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = local.name
  }
}

resource "aws_subnet" "public" {
  count = var.availability_zone_count

  vpc_id                  = aws_vpc.this.id
  cidr_block              = local.public_subnets[count.index]
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = false

  tags = {
    Name                     = "${local.name}-public-${local.azs[count.index]}"
    "kubernetes.io/role/elb" = "1"
  }
}

resource "aws_subnet" "private" {
  count = var.availability_zone_count

  vpc_id            = aws_vpc.this.id
  cidr_block        = local.private_subnets[count.index]
  availability_zone = local.azs[count.index]

  tags = {
    Name                              = "${local.name}-private-${local.azs[count.index]}"
    "kubernetes.io/role/internal-elb" = "1"
  }
}

resource "aws_eip" "nat" {
  count = var.availability_zone_count

  domain = "vpc"

  tags = {
    Name = "${local.name}-nat-${local.azs[count.index]}"
  }
}

resource "aws_nat_gateway" "this" {
  count = var.availability_zone_count

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = {
    Name = "${local.name}-${local.azs[count.index]}"
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "${local.name}-public"
  }
}

resource "aws_route_table_association" "public" {
  count = var.availability_zone_count

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  count = var.availability_zone_count

  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[count.index].id
  }

  tags = {
    Name = "${local.name}-private-${local.azs[count.index]}"
  }
}

resource "aws_route_table_association" "private" {
  count = var.availability_zone_count

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

resource "aws_security_group" "nodes" {
  name        = "${local.name}-nodes"
  description = "Pipeline workloads running on EKS"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "Node to node within the cluster"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  ingress {
    description = "Control plane to kubelet and webhooks"
    from_port   = 1025
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name}-nodes"
  }
}

resource "aws_security_group" "kafka" {
  name        = "${local.name}-kafka"
  description = "MSK brokers reachable only from pipeline workloads"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "TLS from workloads"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.nodes.id]
  }

  tags = {
    Name = "${local.name}-kafka"
  }
}

resource "aws_security_group" "redis" {
  name        = "${local.name}-redis"
  description = "ElastiCache reachable only from pipeline workloads"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "Redis from workloads"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.nodes.id]
  }

  tags = {
    Name = "${local.name}-redis"
  }
}

resource "aws_security_group" "postgres" {
  name        = "${local.name}-postgres"
  description = "RDS reachable only from pipeline workloads"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "Postgres from workloads"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.nodes.id]
  }

  tags = {
    Name = "${local.name}-postgres"
  }
}
