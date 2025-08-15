# Bastion Host EC2
resource "aws_instance" "bastion" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = data.aws_key_pair.test-key.key_name
  subnet_id              = var.aws_public_subnet_id # Bastion must be in a public subnet
  vpc_security_group_ids = [var.aws_security_group_bastion_sg_id]

  associate_public_ip_address = true # Bastion needs public IP to be accessible

  tags = {
    Name = "bastion-host"
  }
}

# Same key pair used for bastion
data "aws_key_pair" "test-key" {
  key_name = "test-key"
}
