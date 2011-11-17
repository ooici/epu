class EPUMDoctor(object):
    """The doctor handles critical sections related to 'pronouncing' a VM instance unhealthy.

    In the future it may farm out subtasks to the EPUM workers (EPUMReactor) but currently all
    health-check activity happens directly via the doctor role.

    The instance of the EPUManagementService process that hosts a particular EPUMDoctor instance
    might not be the elected doctor.  When it is the elected doctor, this EPUMDoctor instance
    handles that functionality.  When it is not the elected doctor, this EPUMDoctor instance
    handles being available in the election.

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    See: https://confluence.oceanobservatories.org/display/CIDev/EPUManagement+Refactor
    """
    
    def __init__(self, epum_store, notifier, provisioner_client, epum_client, ouagent_client):
        """
        @param epum_store State abstraction for all EPUs
        @param notifier A way to signal state changes
        @param provisioner_client A way to destroy VMs
        @param epum_client A way to launch subtasks to EPUM workers (reactor roles) (TODO: not sure if needed)
        @param ouagent_client See OUAgent dump_state() in architecture documentation
        """
        self.epum_store = epum_store
        self.notifier = notifier
        self.provisioner_client = provisioner_client
        self.epum_client = epum_client
        self.ouagent_client = ouagent_client

    def recover(self):
        """Called whenever the whole EPUManagement instance is instantiated.
        """
        # For callbacks: "now_leader()" and "not_leader()"
        self.epum_store.register_doctor(self)


    def now_leader(self):
        """Called when this instance becomes the doctor leader.
        """
        pass

    def not_leader(self):
        """Called when this instance is known not to be the doctor leader.
        """
        pass
