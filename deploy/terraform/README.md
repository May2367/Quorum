# AWS Terraform Deployment

This is the minimal AWS deployment for Checkpoint D: three EC2 nodes across availability zones, EBS-backed Raft data directories, SSM access instead of SSH, and security groups scoped to cluster peer/client ports.

## What It Creates

- Three Ubuntu 22.04 EC2 instances in the default VPC, spread across the first three available AZs.
- One encrypted EBS data volume per node, mounted at `/var/lib/quorum` and passed to `quorumd` as `/data`.
- An IAM role/profile with `AmazonSSMManagedInstanceCore` and read access to a private S3 source bundle.
- A private S3 bucket containing a zipped copy of this repository for instance bootstrap.
- A security group that allows:
  - Raft peer traffic on `9000` only within the cluster security group.
  - client gRPC on `9090` only within the cluster by default, plus optional `client_cidr_blocks`.
  - health HTTP on `8080` only for optional `health_cidr_blocks`.
  - no inbound SSH.
- A systemd-managed Docker container running `quorumd` on each node with a static private-IP peer map.

## Prerequisites

- Terraform `>= 1.6`.
- AWS credentials for the target account and region.
- The AWS CLI with Session Manager support for operational checks.
- A default VPC with default subnets in at least three AZs.

## Deploy

```sh
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

By default, external client and health ingress are closed. Use SSM port forwarding to reach node 1 locally:

```sh
terraform output -raw ssm_client_port_forward_node1
```

Run the printed command in one shell, then use the local endpoint from another shell:

```sh
go run ../../cmd/quorumctl -endpoints=127.0.0.1:19090 put aws/smoke ok
go run ../../cmd/quorumctl -endpoints=127.0.0.1:19090 get aws/smoke
```

For in-instance checks, use the generated SSM start-session commands:

```sh
terraform output ssm_start_session_commands
```

On a node, useful checks are:

```sh
sudo systemctl status quorumd --no-pager
sudo journalctl -u quorumd -n 100 --no-pager
sudo docker ps
curl -fsS http://127.0.0.1:8080/readyz
```

## Measurements To Record

After deployment, record a local/AWS row for the project benchmark table:

- settlement write throughput,
- p50/p99 write latency,
- leader failover time,
- restart recovery time.

The local scripts under `test/chaos` are process/Docker-oriented. For AWS, use SSM sessions or Systems Manager Run Command to stop/restart Docker or the `quorumd` service on one node, then time the first successful write through a surviving endpoint or an SSM tunnel.

## Tear Down

```sh
terraform destroy
```

The source bucket has `force_destroy = true`, and the EBS data volumes are Terraform-managed so teardown removes them with the cluster.
