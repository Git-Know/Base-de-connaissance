
variable "ami_id" {
  type    = string
  default = "ami-002e8d23bd2652396" # Ubuntu 20.04 (update if needed)
}

variable "instance_type" {
  type    = string
  default = "t2.xlarge"
}

variable "key_name" {
  type        = string
  description = "The name of your AWS key pair"
  default     = "test-key"
}
variable "aws_subnet_id" {
  type = string

}
variable "aws_security_group_backend_sg_id" {
  type = string
}
