# Terraform untuk infrastruktur Fleet Telemetry (AWS)
# PERINGATAN: Resource di bawah mungkin dikenakan biaya AWS.
# Untuk belajar, sebaiknya jalankan dulu pakai Docker Compose di lokal.

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "Region AWS"
  type        = string
  default     = "ap-southeast-1" # Jakarta
}

variable "instance_type" {
  description = "Tipe EC2 untuk host Kafka manual (opsional)"
  type        = string
  default     = "t3.micro"
}

# VPC untuk armada EV
resource "aws_vpc" "ev_vpc" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags = {
    Name = "ev-fleet-vpc"
  }
}

# Subnet
resource "aws_subnet" "ev_subnet" {
  vpc_id     = aws_vpc.ev_vpc.id
  cidr_block = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"
  tags = {
    Name = "ev-fleet-subnet"
  }
}

# Security Group (buka port Kafka & Postgres)
resource "aws_security_group" "ev_sg" {
  name        = "ev-fleet-sg"
  description = "Allow Kafka, SSH, and PostgreSQL"
  vpc_id      = aws_vpc.ev_vpc.id

  ingress {
    description = "Kafka plaintext"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Untuk production, ganti dengan IP kamu
  }

  ingress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Untuk production, batasi IP
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Untuk production, batasi IP
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ev-fleet-sg"
  }
}

# EC2 contoh (bisa dipasang Kafka manual atau pakai MSK terpisah)
resource "aws_instance" "ev_kafka_host" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type
  subnet_id     = aws_subnet.ev_subnet.id
  vpc_security_group_ids = [aws_security_group.ev_sg.id]

  tags = {
    Name = "ev-kafka-host"
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

output "kafka_broker_endpoint" {
  value = aws_instance.ev_kafka_host.public_ip
  description = "Public IP EC2 (pasang Kafka secara manual di sini)"
}

output "vpc_id" {
  value = aws_vpc.ev_vpc.id
}