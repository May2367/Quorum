output "cluster_name" {
  value = local.cluster_name
}

output "node_private_ips" {
  value = { for id, instance in aws_instance.node : id => instance.private_ip }
}

output "node_public_ips" {
  value = { for id, instance in aws_instance.node : id => instance.public_ip }
}

output "instance_ids" {
  value = { for id, instance in aws_instance.node : id => instance.id }
}

output "peer_map" {
  value = local.peer_map
}

output "ssm_start_session_commands" {
  value = { for id, instance in aws_instance.node : id => "aws ssm start-session --region ${var.aws_region} --target ${instance.id}" }
}

output "ssm_client_port_forward_node1" {
  value = format(
    "aws ssm start-session --region %s --target %s --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"9090\"],\"localPortNumber\":[\"19090\"]}'",
    var.aws_region,
    aws_instance.node["1"].id,
  )
}

output "quorumctl_endpoint_after_port_forward" {
  value = "127.0.0.1:19090"
}

output "source_bucket" {
  value = aws_s3_bucket.source.id
}
