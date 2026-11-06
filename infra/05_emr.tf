###############################################################################
# Clúster EMR para Spark (streaming y batch)
#
# Clúster PEQUEÑO (1 master + 1 worker) para no gastar creditos de AWS Academy.
# El documento de riesgos lo exige: "un solo worker de Spark".
###############################################################################

resource "aws_emr_cluster" "spark" {
  name          = "${var.proyecto}-emr"
  release_label = var.emr_release
  applications  = ["Spark"]

  # En AWS Academy se usan los roles predefinidos (no se crean roles nuevos).
  service_role = var.lab_role_arn

  ec2_attributes {
    instance_profile = var.lab_instance_profile
  }

  master_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = 1 # un solo worker (ahorro de creditos)
  }

  # Mantener el clúster vivo entre jobs (lo apagas manualmente al terminar).
  keep_job_flow_alive_when_no_steps = true

  # Loguear a S3 para depurar los jobs.
  log_uri = "s3://${aws_s3_bucket.datalake.bucket}/emr-logs/"

  tags = {
    Proyecto = var.proyecto
  }
}
