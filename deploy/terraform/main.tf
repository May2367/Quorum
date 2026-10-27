provider "aws" {
  region = var.aws_region
}

locals {
  tags = merge(var.extra_tags, {
    Project = "quorum"
  })
}

data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_vpc" "default" {
  default = true
}

locals {
  selected_azs = slice(data.aws_availability_zones.available.names, 0, 3)
}

data "aws_subnet" "default_by_az" {
  for_each = toset(local.selected_azs)

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "availability-zone"
    values = [each.key]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  cluster_name = "${var.name_prefix}-${random_id.suffix.hex}"
  nodes = {
    for idx, az in local.selected_azs : tostring(idx + 1) => {
      id         = idx + 1
      az         = az
      subnet_id  = data.aws_subnet.default_by_az[az].id
      private_ip = cidrhost(data.aws_subnet.default_by_az[az].cidr_block, var.private_ip_host_offsets[idx])
      election   = idx == 0 ? 1 : 50 + (idx * 10)
    }
  }
  peer_map = join(",", [for _, node in local.nodes : "${node.id}=${node.private_ip}:9000"])
}

data "archive_file" "source" {
  type        = "zip"
  source_dir  = "${path.module}/../.."
  output_path = "${path.module}/.terraform/${local.cluster_name}-source.zip"

  excludes = [
    ".git/**",
    ".terraform/**",
    "deploy/terraform/.terraform/**",
    "data/**",
    "bin/**",
    "*.log",
    "*.test",
    "coverage.out",
    "plan.txt",
    "progress.txt",
  ]
}

resource "aws_s3_bucket" "source" {
  bucket        = "${local.cluster_name}-source-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "source" {
  bucket = aws_s3_bucket.source.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "source" {
  bucket = aws_s3_bucket.source.id
  key    = "quorum-source.zip"
  source = data.archive_file.source.output_path
  etag   = data.archive_file.source.output_md5
}

resource "aws_iam_role" "node" {
  name = "${local.cluster_name}-node"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "source_read" {
  name = "${local.cluster_name}-source-read"
  role = aws_iam_role.node.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = aws_s3_object.source.arn
      }
    ]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "${local.cluster_name}-node"
  role = aws_iam_role.node.name
}

resource "aws_security_group" "quorum" {
  name        = "${local.cluster_name}-sg"
  description = "Quorum peer/client access"
  vpc_id      = data.aws_vpc.default.id
  tags        = local.tags
}

resource "aws_vpc_security_group_ingress_rule" "peer" {
  security_group_id            = aws_security_group.quorum.id
  referenced_security_group_id = aws_security_group.quorum.id
  ip_protocol                  = "tcp"
  from_port                    = 9000
  to_port                      = 9000
  description                  = "Raft peer traffic within the cluster"
}

resource "aws_vpc_security_group_ingress_rule" "client_self" {
  security_group_id            = aws_security_group.quorum.id
  referenced_security_group_id = aws_security_group.quorum.id
  ip_protocol                  = "tcp"
  from_port                    = 9090
  to_port                      = 9090
  description                  = "Client gRPC traffic within the cluster"
}

resource "aws_vpc_security_group_ingress_rule" "client_external" {
  for_each = toset(var.client_cidr_blocks)

  security_group_id = aws_security_group.quorum.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 9090
  to_port           = 9090
  description       = "Optional external client gRPC access"
}

resource "aws_vpc_security_group_ingress_rule" "health_external" {
  for_each = toset(var.health_cidr_blocks)

  security_group_id = aws_security_group.quorum.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  description       = "Optional external health endpoint access"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.quorum.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Outbound package, SSM, S3, and Docker image access"
}

resource "aws_instance" "node" {
  for_each = local.nodes

  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = each.value.subnet_id
  private_ip                  = each.value.private_ip
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.quorum.id]
  iam_instance_profile        = aws_iam_instance_profile.node.name
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    aws_region       = var.aws_region
    node_id          = each.value.id
    peer_map         = local.peer_map
    election_timeout = each.value.election
    source_bucket    = aws_s3_bucket.source.id
    source_key       = aws_s3_object.source.key
  })

  root_block_device {
    volume_size = var.root_volume_size_gb
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  tags = merge(local.tags, {
    Name = "${local.cluster_name}-node-${each.value.id}"
  })

  depends_on = [
    aws_iam_role_policy_attachment.ssm,
    aws_iam_role_policy.source_read,
    aws_s3_object.source,
  ]
}

resource "aws_ebs_volume" "data" {
  for_each = local.nodes

  availability_zone = each.value.az
  size              = var.data_volume_size_gb
  type              = var.data_volume_type
  encrypted         = true
  tags = merge(local.tags, {
    Name = "${local.cluster_name}-data-${each.value.id}"
  })
}

resource "aws_volume_attachment" "data" {
  for_each = local.nodes

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data[each.key].id
  instance_id = aws_instance.node[each.key].id
}
