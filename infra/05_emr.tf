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

  # EMR necesita un rol de servicio con permisos EC2. El LabRole no los tiene,
  # asi que usamos el rol de servicio estandar de EMR (configurable por variable).
  service_role = var.emr_service_role

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
  log_uri = "s3://${var.data_bucket}/emr-logs/"

  tags = {
    Proyecto = var.proyecto
  }
}
