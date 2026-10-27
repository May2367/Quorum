variable "aws_region" {
  description = "AWS region for the three-node Quorum cluster."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix used for AWS resource names."
  type        = string
  default     = "quorum"
}

variable "instance_type" {
  description = "EC2 instance type for each Quorum node."
  type        = string
  default     = "t3.small"
}

variable "root_volume_size_gb" {
  description = "Root volume size for each EC2 node."
  type        = number
  default     = 20
}

variable "data_volume_size_gb" {
  description = "EBS data volume size for each node's Raft WAL/data directory."
  type        = number
  default     = 20
}

variable "data_volume_type" {
  description = "EBS data volume type."
  type        = string
  default     = "gp3"
}

variable "private_ip_host_offsets" {
  description = "Host offsets inside each default subnet CIDR used for stable private IPs. Override if these addresses are already in use."
  type        = list(number)
  default     = [101, 102, 103]

  validation {
    condition     = length(var.private_ip_host_offsets) == 3
    error_message = "private_ip_host_offsets must contain exactly three offsets."
  }
}

variable "client_cidr_blocks" {
  description = "Optional CIDR blocks allowed to reach the client gRPC port 9090. Leave empty and use SSM port forwarding for tighter access."
  type        = list(string)
  default     = []
}

variable "health_cidr_blocks" {
  description = "Optional CIDR blocks allowed to reach the HTTP health port 8080. Leave empty and use SSM port forwarding for tighter access."
  type        = list(string)
  default     = []
}

variable "extra_tags" {
  description = "Additional tags applied to supported AWS resources."
  type        = map(string)
  default     = {}
}
