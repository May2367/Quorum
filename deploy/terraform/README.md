# Terraform Deployment

This directory is reserved for the AWS deployment described in `plan.txt`:
three EC2 instances across availability zones, EBS volumes for Raft data,
security groups scoped to peer/client access, and SSM instead of public SSH.

The implementation should wait until the networked server is functional so the
user data and systemd unit can launch a real cluster.

