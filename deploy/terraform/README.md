# AWS Terraform Scaffold

This directory contains Terraform for a three-node AWS topology: EC2 instances across availability zones, EBS-backed Raft data directories, SSM access instead of SSH, and security groups scoped to cluster peer/client ports.

The configuration is scaffold-only. It has not been applied or validated against real AWS resources.

## Configuration

- Three Ubuntu 22.04 EC2 instances in the default VPC, spread across the first three available AZs.
- One encrypted EBS data volume per node, mounted at `/var/lib/quorum` and passed to `quorumd` as `/data`.
- An IAM role/profile with `AmazonSSMManagedInstanceCore` and read access to a private S3 source bundle.
- A private S3 bucket containing a zipped copy of this repository for instance bootstrap.
- A security group that allows peer traffic within the cluster security group, optional client/health ingress, and no inbound SSH.
- A systemd-managed Docker container running `quorumd` on each node with a static private-IP peer map.

## Files

- `main.tf`: AWS resources and bootstrap wiring.
- `variables.tf`: region, naming, instance, and ingress inputs.
- `outputs.tf`: generated instance and SSM command outputs.
- `user_data.sh.tftpl`: instance bootstrap script.
- `terraform.tfvars.example`: example input values.
