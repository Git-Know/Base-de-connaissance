
variable "ami_id" {
  type    = string
  default = "ami-0c02fb55956c7d316" # Ubuntu 20.04 (update if needed)
}

variable "instance_type" {
  type    = string
  default = "t2.micro"
}

variable "key_name" {
  type        = string
  description = "The name of your AWS key pair"
}
