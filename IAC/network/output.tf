output "aws_private_subnet_id" {
  value = aws_subnet.private.id
}
output "aws_security_group_backend_sg_id" {
  value = aws_security_group.backend_sg.id
}
output "aws_security_group_frontend_sg_id" {
  value = aws_security_group.frontend_sg.id
}
output "aws_public_subnet_id" {
  value = aws_subnet.public.id
}
output "aws_security_group_bastion_sg_id" {
  value = aws_security_group.bastion_sg.id
}
