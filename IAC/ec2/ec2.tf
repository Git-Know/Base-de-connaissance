resource "aws_instance" "backend" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = data.aws_key_pair.test-key.key_name
  subnet_id              = var.aws_subnet_id
  vpc_security_group_ids = [var.aws_security_group_backend_sg_id]
  root_block_device {
    volume_size           = 50    # 50 GB
    volume_type           = "gp3" # General purpose SSD
    delete_on_termination = true
  }
  tags = {
    Name = "backend-instance"
  }
}

data "aws_key_pair" "test-key" {
  key_name = "test-key"
}

