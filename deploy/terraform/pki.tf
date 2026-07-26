resource "aws_acmpca_certificate_authority" "this" {
  type = "ROOT"

  certificate_authority_configuration {
    key_algorithm     = "RSA_4096"
    signing_algorithm = "SHA256WITHRSA"

    subject {
      organization = "IronClaw"
      common_name  = "IronClaw Device CA"
    }
  }

  revocation_configuration {
    crl_configuration {
      enabled            = true
      expiration_in_days = 7
      s3_bucket_name     = aws_s3_bucket.crl.id
      s3_object_acl      = "BUCKET_OWNER_FULL_CONTROL"
    }
  }

  tags = {
    Name = "${local.name}-device-ca"
  }

  depends_on = [aws_s3_bucket_policy.crl]

  lifecycle {
    ignore_changes = [revocation_configuration]
  }
}

resource "aws_s3_bucket" "crl" {
  bucket_prefix = "${local.name}-crl-"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "crl" {
  bucket = aws_s3_bucket.crl.id

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

resource "aws_s3_bucket_server_side_encryption_configuration" "crl" {
  bucket = aws_s3_bucket.crl.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "crl" {
  bucket = aws_s3_bucket.crl.id

  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "crl" {
  statement {
    sid    = "AllowPrivateCACrlWrite"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["acm-pca.amazonaws.com"]
    }

    actions = [
      "s3:PutObject",
      "s3:PutObjectAcl",
      "s3:GetBucketAcl",
      "s3:GetBucketLocation",
    ]

    resources = [
      aws_s3_bucket.crl.arn,
      "${aws_s3_bucket.crl.arn}/*",
    ]
  }
}

resource "aws_s3_bucket_policy" "crl" {
  bucket = aws_s3_bucket.crl.id
  policy = data.aws_iam_policy_document.crl.json
}

resource "aws_secretsmanager_secret" "redis_auth" {
  name       = "${local.name}/redis-auth"
  kms_key_id = aws_kms_key.pipeline.arn
}

resource "aws_secretsmanager_secret_version" "redis_auth" {
  secret_id     = aws_secretsmanager_secret.redis_auth.id
  secret_string = random_password.redis_auth.result
}

resource "aws_secretsmanager_secret" "postgres" {
  name       = "${local.name}/postgres"
  kms_key_id = aws_kms_key.pipeline.arn
}

resource "aws_secretsmanager_secret_version" "postgres" {
  secret_id = aws_secretsmanager_secret.postgres.id

  secret_string = jsonencode({
    username = aws_db_instance.postgres.username
    password = random_password.postgres.result
    host     = aws_db_instance.postgres.address
    port     = aws_db_instance.postgres.port
    dbname   = aws_db_instance.postgres.db_name
  })
}

resource "aws_acmpca_certificate" "root" {
  certificate_authority_arn   = aws_acmpca_certificate_authority.this.arn
  certificate_signing_request = aws_acmpca_certificate_authority.this.certificate_signing_request
  signing_algorithm           = "SHA256WITHRSA"

  template_arn = "arn:aws:acm-pca:::template/RootCACertificate/V1"

  validity {
    type  = "YEARS"
    value = 10
  }
}

resource "aws_acmpca_certificate_authority_certificate" "root" {
  certificate_authority_arn = aws_acmpca_certificate_authority.this.arn
  certificate               = aws_acmpca_certificate.root.certificate
  certificate_chain         = aws_acmpca_certificate.root.certificate_chain
}
