"""Decision engine states
"""

PENDING = 'PENDING_DE' # EPU is waiting on something

STABLE = 'STABLE_DE' # EPU is in a stable state (with respect to its policy)

UNKNOWN = 'UNKNOWN' # DE does not implement the contract
