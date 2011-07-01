"""Decision engine states
"""

PENDING = 'PENDING_DE' # EPU is waiting on something

STABLE = 'STABLE_DE' # EPU is in a stable state (with respect to its policy)

UNKNOWN = 'UNKNOWN' # DE does not implement the contract

DEVMODE_FAILED = 'DEVMODE_FAILED_DE' # EPU is in development mode and received a node failure notification
