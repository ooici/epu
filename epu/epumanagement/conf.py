# Copyright 2013 University of Chicago

# Confs for the Neediness API constraints

CONF_IAAS_SITE = "iaas_site"
CONF_IAAS_ALLOCATION = "iaas_allocation"


# Initial confs, see doc for epu.epumanagement.EPUManagement.__init__()

EPUM_INITIALCONF_PROC_NAME = "proc_name"
EPUM_INITIALCONF_SERVICE_NAME = "service_name"
EPUM_INITIALCONF_EXTERNAL_DECIDE = "_external_decide_invocations"

EPUM_INITIALCONF_DEFAULT_NEEDY_IAAS = "needy_default_iaas_site"
EPUM_INITIALCONF_DEFAULT_NEEDY_IAAS_ALLOC = "needy_default_iaas_allocation"

# EPU confs, see doc for epu.epumanagement.EPUManagement.msg_reconfigure_epu()

EPUM_CONF_GENERAL = "general"
EPUM_CONF_ENGINE_CLASS = "engine_class"
EPUM_CONF_ENGINE = "engine_conf"
EPUM_CONF_CHEF_CREDENTIAL = "chef_credential"
EPUM_CONF_HEALTH = "health"
EPUM_CONF_HEALTH_MONITOR = "monitor_health"
EPUM_CONF_HEALTH_BOOT = "boot_timeout"
EPUM_CONF_HEALTH_MISSING = "missing_timeout"
EPUM_CONF_HEALTH_REALLY_MISSING = "really_missing_timeout"
EPUM_CONF_HEALTH_ZOMBIE = "zombie_seconds"
EPUM_CONF_SENSOR_TYPE = "sensor_type"

# Default time before removing node records in terminal state
EPUM_RECORD_REAPING_DEFAULT_MAX_AGE = 7200

# Other
EPUM_DEFAULT_SERVICE_NAME = "epu_management_service"
PROVISIONER_VARS_KEY = "provisioner_vars"

EPUM_CONF_DECIDER_LOOP_INTERVAL = "decider_loop_interval"
EPUM_DECIDER_DEFAULT_LOOP_INTERVAL = 5.0
