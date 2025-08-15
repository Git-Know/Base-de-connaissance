variable "ami_id" {
  description = "AMI ID to use for the EC2 instances"
  type        = string
  default     = "ami-002e8d23bd2652396" # Ubuntu 20.04 (update if needed)

}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.micro"
}


variable "aws_public_subnet_id" {
  description = "Subnet ID for the bastion host (public subnet)"
  type        = string
}


variable "aws_security_group_bastion_sg_id" {
  description = "Security group ID for the bastion host"
  type        = string
}

variable "key_name" {
  description = "Name of the existing EC2 key pair"
  type        = string
  default     = "test-key"
}
