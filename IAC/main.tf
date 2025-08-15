provider "aws" {
  region = "eu-west-3"
}

module "network" {
  source = "./network"
}

module "ec2" {
  source                           = "./ec2"
  aws_subnet_id                    = module.network.aws_private_subnet_id
  aws_security_group_backend_sg_id = module.network.aws_security_group_backend_sg_id
}

module "s3" {
  source = "./s3"
}
module "bastion" {
  source                           = "./bastion"
  aws_public_subnet_id             = module.network.aws_public_subnet_id
  aws_security_group_bastion_sg_id = module.network.aws_security_group_bastion_sg_id
}
