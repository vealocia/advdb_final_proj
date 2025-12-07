from typing import Dict, Set, Optional, List
from enum import Enum
from tabulate import tabulate


class Version:
    """
    Represents a committed version of a variable in the database.
    
    Each version tracks the value, the transaction that wrote it, and when
    it was committed. This supports multiversion concurrency control.
    """

    def __init__(self, value: int, transaction_id: str, commit_time: float):
        """
        Create a new version of a variable.
        
        Args:
            value: The integer value of this version
            transaction_id: ID of the transaction that created this version
            commit_time: Timestamp when this version was committed
        
        Side effects:
            - Initializes instance variables for the version
        """
        self.value = value
        self.transaction_id = transaction_id
        self.commit_time = commit_time


class Site:
    """
    Represents a database site in a distributed replicated database system.
    
    Each site stores variables and their version histories, tracks its
    operational status (up/down), and manages recovery after failures.
    Sites store:
    - Even-numbered variables (x2, x4, ..., x20): replicated across all sites
    - Odd-numbered variables (x1, x3, ..., x19): one per site based on var_num % 10
    """

    def __init__(self, site_id: int):
        """
        Initialize a database site with all variables and version histories.
        
        Args:
            site_id: Unique identifier for this site (1-10)
        
        Side effects:
            - Sets site_id and initial operational status (up)
            - Initializes empty data structures for variables and versions
            - Calls _initialize_variables() to populate initial variable state
            - Sets last_fail_time and last_recover_time to -1.0
        """
        self.site_id = site_id
        self.is_up = True
        self.variables = {}
        self.version_history: Dict[str, List[Version]] = {}
        self.readable_after_recovery: Dict[str, bool] = {}
        self.last_fail_time = -1.0
        self.last_recover_time = -1.0
        self._initialize_variables()

    def _initialize_variables(self):
        """
        Initialize all variables at this site with their initial values.
        
        Creates variables x1 through x20, each with initial value 10*i.
        Sets up version history with initial version from transaction T0 at time 0.
        All variables are initially marked as readable.
        
        Returns:
            None
        
        Side effects:
            - Populates self.variables with initial values
            - Creates initial version history for each variable
            - Marks all variables as readable in self.readable_after_recovery
        """
        for i in range(1, 21):
            var = f"x{i}"
            initial_value = 10 * i
            self.variables[var] = initial_value
            init_version = Version(initial_value, "T0", 0)
            self.version_history[var] = [init_version]
            self.readable_after_recovery[var] = True

    def fail(self, global_time: float):
        """
        Mark this site as failed at the given time.
        
        Args:
            global_time: Timestamp when the site fails
        
        Returns:
            None
        
        Side effects:
            - Sets self.is_up to False
            - Records failure time in self.last_fail_time
        """
        self.is_up = False
        self.last_fail_time = global_time

    def recover(self, global_time: float):
        """
        Recover this site from a failed state.
        
        After recovery, replicated variables (even-numbered) become temporarily
        unreadable until a new write is committed. This prevents reading stale
        data that may have been updated on other sites during the failure.
        
        Args:
            global_time: Timestamp when the site recovers
        
        Returns:
            None
        
        Side effects:
            - Sets self.is_up to True
            - Records recovery time in self.last_recover_time
            - Marks all replicated variables (x2, x4, ..., x20) as unreadable
            - Restores variable values to last committed version before failure
        """
        self.is_up = True
        self.last_recover_time = global_time

        # Only replicated variables need special handling
        for i in range(2, 21, 2):
            var = f"x{i}"
            if var in self.version_history:
                # Mark as unreadable until a new write is committed
                self.readable_after_recovery[var] = False

                # Keep the last committed value but don't allow reads until new write
                if self.last_fail_time > -1:
                    last_commit = next(
                        (
                            v
                            for v in reversed(self.version_history[var])
                            if v.commit_time < self.last_fail_time
                        ),
                        None,
                    )
                    if last_commit:
                        self.variables[var] = last_commit.value

    def get_committed_version_at(self, var: str, start_time: float) -> Optional[int]:
        """
        Get the committed value of a variable as of a specific timestamp.
        
        This method supports multiversion concurrency control by retrieving the
        appropriate version for a read-only transaction that started at start_time.
        For replicated variables, ensures the site was up continuously between
        the commit and the transaction start to avoid reading stale data.
        
        Args:
            var: Variable name (e.g., "x1", "x2")
            start_time: Timestamp of the transaction's start
        
        Returns:
            The integer value of the variable at start_time, or None if:
            - The site is down
            - The variable doesn't exist at this site
            - The site failed between the last commit and start_time (for replicated vars)
            - No version exists before start_time
        
        Side effects:
            None
        """
        if not self.is_up or var not in self.version_history:
            return None

        var_num = int(var[1:])
        versions = self.version_history[var]

        # For replicated variables (even numbered)
        if var_num % 2 == 0:
            # For read-only transactions, we need the last committed value before start_time
            last_commit_before_start = next(
                (v for v in reversed(versions) if v.commit_time <= start_time), None
            )

            # Site must have been up continuously from last commit to transaction start
            if not last_commit_before_start:
                return (
                    versions[0].value
                    if (
                        self.last_fail_time < 0
                        or (
                            self.last_fail_time < start_time
                            and self.last_recover_time > self.last_fail_time
                        )
                    )
                    else None
                )

            # Check if site failed between commit and transaction start
            if (
                self.last_fail_time > last_commit_before_start.commit_time
                and self.last_fail_time < start_time
            ):
                return None

            return last_commit_before_start.value

        # For non-replicated variables (odd numbered)
        return next(
            (v.value for v in reversed(versions) if v.commit_time <= start_time),
            versions[0].value,
        )

    def commit_write(self, var: str, value: int, tid: str, commit_time: float):
        """
        Commit a write operation by adding a new version to the variable's history.
        
        Args:
            var: Variable name to write to
            value: New value to commit
            tid: Transaction ID performing the write
            commit_time: Timestamp of the commit
        
        Returns:
            None
        
        Side effects:
            - Adds new Version to self.version_history[var]
            - Sorts version history by commit time
            - Updates self.variables[var] to the new value
            - For replicated variables, marks as readable after recovery
        """
        versions = self.version_history[var]
        versions.append(Version(value, tid, commit_time))
        versions.sort(key=lambda x: x.commit_time)
        self.variables[var] = value

        var_num = int(var[1:])
        if var_num % 2 == 0:
            self.readable_after_recovery[var] = True

    def dump(self) -> List[str]:
        """
        Generate a snapshot of current variable states at this site.
        
        Returns a formatted list of variable-value pairs for all variables
        stored at this site. Even variables are stored at all sites, while
        odd variables are stored at site (var_num % 10) + 1.
        
        Returns:
            List of strings in format "x1: 10", "x2: 20", etc., sorted by
            variable number. Returns empty list if site is down.
        
        Side effects:
            None
        """
        # Return current site variable states
        if not self.is_up:
            return []
        result = []
        # even vars
        for i in range(2, 21, 2):
            var = f"x{i}"
            if var in self.variables:
                result.append(f"{var}: {self.variables[var]}")
        # odd vars
        for i in range(1, 21, 2):
            correct_site = 1 + (i % 10)
            if self.site_id == correct_site:
                var = f"x{i}"
                if var in self.variables:
                    result.append(f"{var}: {self.variables[var]}")

        return sorted(result, key=lambda x: int(x.split(":")[0][1:]))

    def reset(self):
        """
        Reset site to initial state with all variables reinitialized.
        
        Returns:
            None
        
        Side effects:
            - Sets is_up to True
            - Clears all variables and version history
            - Clears readable_after_recovery tracking
            - Calls _initialize_variables() to restore initial state
        """
        self.is_up = True
        self.variables = {}
        self.version_history = {}
        self.readable_after_recovery = {}
        self._initialize_variables()


class TransactionType(Enum):
    """
    Enumeration of transaction types in the system.
    
    READ_WRITE: Transaction can read and write variables
    READ_ONLY: Transaction can only read variables using multiversion snapshots
    """
    READ_WRITE = "READ_WRITE"
    READ_ONLY = "READ_ONLY"


class TransactionStatus(Enum):
    """
    Enumeration of transaction lifecycle states.
    
    ACTIVE: Transaction is currently executing
    COMMITTED: Transaction has successfully committed
    ABORTED: Transaction has been aborted/rolled back
    """
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class Transaction:
    """
    Represents a database transaction with concurrency control metadata.
    
    Tracks reads, writes, dependencies, and status for implementing
    optimistic concurrency control with first-committer-wins semantics.
    """

    def __init__(self, tid: str, transaction_type: TransactionType, start_time: float):
        """
        Create a new transaction.
        
        Args:
            tid: Unique transaction identifier (e.g., "T1", "T2")
            transaction_type: READ_WRITE or READ_ONLY transaction type
            start_time: Global timestamp when transaction began
        
        Side effects:
            - Initializes transaction with ACTIVE status
            - Creates empty write cache, read set, and write set
            - Sets should_abort flag to False
            - Initializes empty dependency set for serialization graph
        """
        self.tid = tid
        self.type = transaction_type
        self.status = TransactionStatus.ACTIVE
        self.write_cache: Dict[str, int] = {}
        self.read_set: Dict[str, float] = {}
        self.write_set: Set[str] = set()
        self.start_time = start_time
        self.should_abort = False
        self.dependencies: Set[str] = set()


class TransactionManager:
    """
    Manages distributed transactions across multiple replicated database sites.
    
    Implements optimistic concurrency control with:
    - Multiversion concurrency control for read-only transactions
    - First-committer-wins conflict resolution
    - Serialization graph testing for serializability
    - Site failure and recovery handling
    - Available copies replication protocol
    """

    def __init__(self):
        """
        Initialize the transaction manager with 10 database sites.
        
        Creates sites numbered 1-10, each initially up and containing
        appropriate variables based on replication rules.
        
        Side effects:
            - Creates 10 Site objects in self.sites dictionary
            - Initializes empty transaction tracking dictionary
            - Sets global_time to 0.0
            - Initializes empty serialization graph
        """
        self.sites: Dict[int, Site] = {i: Site(i) for i in range(1, 11)}
        self.transactions: Dict[str, Transaction] = {}
        self.global_time = 0.0
        self.serial_graph: Dict[str, Set[str]] = {}

    def begin_transaction(self, tid: str):
        """
        Start a new read-write transaction.
        
        Args:
            tid: Unique transaction identifier
        
        Returns:
            None
        
        Side effects:
            - Increments global_time
            - Creates new Transaction object in self.transactions
            - Prints "begin {tid}" to stdout
        
        Raises:
            ValueError: If transaction with this ID already exists
        """
        if tid in self.transactions:
            raise ValueError(f"Transaction {tid} already exists")
        self.global_time += 1
        self.transactions[tid] = Transaction(
            tid, TransactionType.READ_WRITE, self.global_time
        )
        print(f"begin {tid}")

    def begin_read_only_transaction(self, tid: str):
        """
        Start a new read-only transaction.
        
        Read-only transactions use multiversion concurrency control to read
        a consistent snapshot of the database as of their start time.
        
        Args:
            tid: Unique transaction identifier
        
        Returns:
            None
        
        Side effects:
            - Increments global_time
            - Creates new READ_ONLY Transaction object in self.transactions
            - Prints "beginRO {tid}" to stdout
        
        Raises:
            ValueError: If transaction with this ID already exists
        """
        if tid in self.transactions:
            raise ValueError(f"Transaction {tid} already exists")
        self.global_time += 1
        self.transactions[tid] = Transaction(
            tid, TransactionType.READ_ONLY, self.global_time
        )
        print(f"beginRO {tid}")

    def read(self, tid: str, var: str):
        """
        Execute a read operation for a transaction.
        
        For read-write transactions, reads from the write cache if available,
        otherwise reads the most recent committed version from an available site.
        For read-only transactions, reads the appropriate snapshot version.
        
        Args:
            tid: Transaction ID performing the read
            var: Variable name to read (e.g., "x1", "x2")
        
        Returns:
            None
        
        Side effects:
            - Updates transaction's read_set with variable and commit time
            - Prints read result or wait message to stdout
            - Updates serialization graph with read dependencies
            - May abort transaction if cycle detected
        """
        transaction = self.transactions.get(tid)
        if not transaction or transaction.status != TransactionStatus.ACTIVE:
            return

        # Return cached value if transaction has written to this variable
        if var in transaction.write_cache:
            val = transaction.write_cache[var]
            print(f"{tid} reads {var}: {val} [from write cache]")
            transaction.read_set[var] = transaction.start_time
            return

        var_num = int(var[1:])
        # For non-replicated variables, check if home site is up
        if var_num % 2 == 1:
            home_site = 1 + (var_num % 10)
            if not self.sites[home_site].is_up:
                print(f"{tid} waits for site {home_site} to recover (contains {var})")
                return

        # Get available versions from all up sites
        available_versions = [
            (val, site_id)
            for site_id, site in self.sites.items()
            if (val := site.get_committed_version_at(var, transaction.start_time))
            is not None
        ]

        if available_versions:
            val, site_id = max(available_versions, key=lambda x: x[1])
            print(f"{tid} reads {var}: {val} [from site {site_id}]")
            commit_time = self._find_commit_time_of_value(
                var, val, transaction.start_time
            )
            transaction.read_set[var] = commit_time or 0
            self._update_serial_graph_on_read(tid, var)
        else:
            print(f"{tid} waits - no available version of {var} at any site")

    def _find_commit_time_of_value(
        self, var: str, val: int, start_time: float
    ) -> Optional[float]:
        """
        Find when a specific value of a variable was committed.
        
        Searches all sites to find the most recent commit time for the given
        value that occurred before or at start_time.
        
        Args:
            var: Variable name
            val: Value to search for
            start_time: Upper bound on commit time to search
        
        Returns:
            The commit time when this value was written, or None if not found
        
        Side effects:
            None
        """
        return max(
            (
                version.commit_time
                for site in self.sites.values()
                if var in site.version_history
                for version in site.version_history[var]
                if version.value == val and version.commit_time <= start_time
            ),
            default=None,
        )

    def write(self, tid: str, var: str, val: int):
        """
        Execute a write operation for a transaction.
        
        Writes are buffered in the transaction's write cache until commit.
        For replicated variables, writes go to all available sites. For
        non-replicated variables, writes go to the home site if available.
        
        Args:
            tid: Transaction ID performing the write
            var: Variable name to write
            val: Value to write
        
        Returns:
            None
        
        Side effects:
            - Adds value to transaction's write_cache
            - Adds variable to transaction's write_set
            - Prints write operation and target sites to stdout
            - Prints wait message if no sites available
        
        Raises:
            ValueError: If a read-only transaction attempts to write
        """
        transaction = self.transactions.get(tid)
        if not transaction or transaction.status != TransactionStatus.ACTIVE:
            return
        if transaction.type == TransactionType.READ_ONLY:
            raise ValueError(f"Read-only transaction {tid} cannot write")

        # Track which sites will receive this write
        var_num = int(var[1:])
        target_sites = []
        if var_num % 2 == 0:  # Replicated variable
            target_sites = [
                site_id for site_id, site in self.sites.items() if site.is_up
            ]
        else:  # Non-replicated variable
            home_site = 1 + (var_num % 10)
            if self.sites[home_site].is_up:
                target_sites = [home_site]

        if not target_sites:
            print(f"{tid} waits - no available sites for writing {var}")
            return

        transaction.write_cache[var] = val
        transaction.write_set.add(var)
        print(
            f"{tid} writes {var}: {val} [to sites {', '.join(map(str, target_sites))}]"
        )

    def end_transaction(self, tid: str):
        """
        End a transaction by committing or aborting it.
        
        Read-only transactions and transactions with empty write sets commit
        immediately. Transactions with writes undergo validation and conflict
        detection before committing.
        
        Args:
            tid: Transaction ID to end
        
        Returns:
            None
        
        Side effects:
            - Calls _commit_transaction or _abort_transaction
            - Prints commit or abort message to stdout
            - May increment global_time
            - May propagate writes to all appropriate sites
        """
        transaction = self.transactions.get(tid)
        if not transaction:
            return
        if transaction.status == TransactionStatus.ABORTED:
            print(f"{tid} aborts")
            return

        if transaction.should_abort or any(
            ct is None for ct in transaction.read_set.values()
        ):
            self._abort_transaction(transaction)
            print(f"{tid} aborts")
            return

        if not transaction.write_set:
            transaction.status = TransactionStatus.COMMITTED
            self.global_time += 1
            print(f"{tid} commits")
            self._update_serial_graph_on_commit(tid)
            return

        self._commit_transaction(transaction)

    def _commit_transaction(self, transaction: Transaction):
        """
        Attempt to commit a transaction with write operations.
        
        Validates the transaction by checking:
        1. All written-to sites are still up
        2. No first-committer-wins conflicts exist
        3. No serialization cycles would be created
        
        If validation succeeds, writes are propagated to all appropriate sites.
        
        Args:
            transaction: Transaction object to commit
        
        Returns:
            None
        
        Side effects:
            - Updates serialization graph
            - May detect cycle and abort transaction
            - On success: increments global_time, propagates writes to sites,
              marks transaction as COMMITTED, prints commit message
            - On failure: aborts transaction and prints abort message
        """
        tid = transaction.tid

        def should_abort() -> bool:
            # Check if any site failed after write
            for var in transaction.write_set:
                var_num = int(var[1:])
                if var_num % 2 == 1:  # Non-replicated variable
                    home = 1 + (var_num % 10)
                    if not self.sites[home].is_up:
                        return True

            # Check for read consistency
            if any(ct is None for ct in transaction.read_set.values()):
                return True

            # Check for first-committer-wins conflicts
            for var in transaction.write_set:
                for other_tid, other_txn in self.transactions.items():
                    if (
                        other_tid != tid
                        and var in other_txn.write_set
                        and other_txn.status == TransactionStatus.COMMITTED
                        and other_txn.commit_time > transaction.start_time
                    ):
                        return True
            return False

        def get_target_sites(var: str) -> List[Site]:
            var_num = int(var[1:])
            if var_num % 2 == 0:
                return [
                    site
                    for site in self.sites.values()
                    if site.is_up
                    and (
                        site.last_fail_time == -1.0
                        or (
                            site.last_fail_time < transaction.start_time
                            and site.last_recover_time > site.last_fail_time
                        )
                    )
                ]
            else:
                home = 1 + (var_num % 10)
                return [self.sites[home]] if self.sites[home].is_up else []

        if should_abort():
            self._abort_transaction(transaction)
            print(f"{tid} aborts")
            return

        self._update_serial_graph_on_commit(tid)
        self._update_serial_graph_for_ww_conflicts(tid)

        if self._detect_cycle():
            self._abort_transaction(transaction)
            print(f"{tid} aborts")
            return

        commit_time = self.global_time + 1
        for var, val in transaction.write_cache.items():
            for site in get_target_sites(var):
                site.commit_write(var, val, tid, commit_time)

        transaction.status = TransactionStatus.COMMITTED
        transaction.commit_time = commit_time
        self.global_time = commit_time
        print(f"{tid} commits")

    def _latest_commit_time_before_commit(
        self, var: str, commit_time: float
    ) -> Optional[float]:
        """
        Find the most recent commit time for a variable before a given time.
        
        Args:
            var: Variable name
            commit_time: Upper bound (exclusive) on commit times to search
        
        Returns:
            The latest commit time before commit_time, or None if no such commit exists
        
        Side effects:
            None
        """
        return max(
            (
                version.commit_time
                for site in self.sites.values()
                if var in site.version_history
                for version in site.version_history[var]
                if version.commit_time < commit_time
            ),
            default=None,
        )

    def _abort_transaction(self, transaction: Transaction):
        """
        Abort a transaction and discard its uncommitted changes.
        
        Args:
            transaction: Transaction object to abort
        
        Returns:
            None
        
        Side effects:
            - Sets transaction status to ABORTED
            - Clears transaction's write_cache (discards buffered writes)
            - Clears transaction's write_set
        """
        transaction.status = TransactionStatus.ABORTED
        # Clear write cache to discard uncommitted writes
        transaction.write_cache.clear()
        transaction.write_set.clear()

    def fail_site(self, site_id: int):
        """
        Simulate a site failure.
        
        Marks the site as down and flags all transactions that wrote to
        variables at that site for abortion.
        
        Args:
            site_id: ID of the site to fail (1-10)
        
        Returns:
            None
        
        Side effects:
            - Increments global_time
            - Calls site.fail() to mark site as down
            - Sets should_abort flag for affected transactions
            - Prints failure message to stdout
        """
        if site_id not in self.sites:
            return
        self.global_time += 1
        self.sites[site_id].fail(self.global_time)
        print(f"Site {site_id} fails")

        for tid, transaction in self.transactions.items():
            if transaction.status != TransactionStatus.ACTIVE:
                continue
            for var in transaction.write_set:
                var_num = int(var[1:])
                if var_num % 2 == 0 or (
                    var_num % 2 == 1 and (1 + (var_num % 10)) == site_id
                ):
                    transaction.should_abort = True

    def recover_site(self, site_id: int):
        """
        Recover a failed site.
        
        Brings the site back online. Replicated variables at the site
        become unreadable until new writes are committed to ensure
        data consistency.
        
        Args:
            site_id: ID of the site to recover (1-10)
        
        Returns:
            None
        
        Side effects:
            - Increments global_time
            - Calls site.recover() to mark site as up
            - Prints recovery message to stdout
        """
        if site_id not in self.sites:
            return
        self.global_time += 1
        self.sites[site_id].recover(self.global_time)
        print(f"Site {site_id} recovers")

    def dump(self) -> None:
        """
        Print a formatted table showing the current state of all sites.
        
        Displays all variables and their current values across all sites.
        Shows which sites are UP or DOWN and the value of each variable
        at each site.
        
        Returns:
            None
        
        Side effects:
            - Prints formatted table to stdout using tabulate library
        """
        active_sites = [site for site in self.sites.values() if site.is_up]
        all_vars = sorted(
            {var.split(":")[0] for site in active_sites for var in site.dump()},
            key=lambda x: int(x[1:]),
        )

        headers = ["Site", "Status"] + all_vars
        table_data = []

        for site_id in sorted(self.sites.keys()):
            site = self.sites[site_id]
            if not site.is_up:
                table_data.append([site_id, "DOWN"] + ["" for _ in all_vars])
                continue

            var_dict = {
                var.split(":")[0]: var.split(":")[1].strip() for var in site.dump()
            }
            row = [site_id, "UP"]
            row.extend(var_dict.get(var, "") for var in all_vars)
            table_data.append(row)

        print(tabulate(table_data, headers=headers, tablefmt="grid"))

    def reset_state(self):
        """
        Reset the entire system to initial state.
        
        Clears all transactions and resets all sites to their initial
        configuration with default variable values.
        
        Returns:
            None
        
        Side effects:
            - Clears self.transactions dictionary
            - Resets global_time to 0.0
            - Clears serialization graph
            - Calls reset() on all sites
        """
        self.transactions.clear()
        self.global_time = 0.0
        self.serial_graph.clear()
        for site in self.sites.values():
            site.reset()

    def process_operation(self, operation: str):
        """
        Parse and execute a single operation command.
        
        Supported operations:
        - begin(T1): Start read-write transaction
        - beginRO(T1): Start read-only transaction
        - R(T1,x2): Read variable
        - W(T1,x2,100): Write variable
        - end(T1): End transaction
        - fail(1): Fail site
        - recover(1): Recover site
        - dump(): Display current state
        
        Args:
            operation: String containing the operation command
        
        Returns:
            None
        
        Side effects:
            - Strips comments (text after //)
            - Calls appropriate transaction manager method
            - May modify transaction state, site state, or global time
        """
        operation = operation.split("//")[0].strip()
        if not operation:
            return

        def parse_args(op_str: str) -> List[str]:
            return [arg.strip() for arg in op_str.rstrip(")").split(",")]

        if "(" in operation:
            op_type, args_str = operation.strip().split("(", 1)
            op_type = op_type.strip().lower()
            args = parse_args(args_str)

            operations = {
                "begin": lambda: self.begin_transaction(args[0]),
                "beginro": lambda: self.begin_read_only_transaction(args[0]),
                "w": lambda: self.write(args[0], args[1], int(args[2])),
                "r": lambda: self.read(args[0], args[1]),
                "end": lambda: self.end_transaction(args[0]),
                "dump": self.dump,
                "fail": lambda: self.fail_site(int(args[0])),
                "recover": lambda: self.recover_site(int(args[0])),
            }

            if op_type in operations:
                operations[op_type]()
        else:
            parts = operation.strip().split()
            if len(parts) == 2:
                op_type = parts[0].lower()
                if op_type == "begin":
                    self.begin_transaction(parts[1])
                elif op_type == "beginro":
                    self.begin_read_only_transaction(parts[1])

    def _update_serial_graph_on_read(self, tid: str, var: str):
        """
        Update the serialization graph when a transaction reads a variable.
        
        Creates a read-after-write dependency edge from the writing transaction
        to the reading transaction. Checks for cycles after adding the edge.
        
        Args:
            tid: Transaction ID that performed the read
            var: Variable that was read
        
        Returns:
            None
        
        Side effects:
            - Adds edge to self.serial_graph
            - May abort transaction if cycle is detected
            - Prints abort message if cycle detected
        """
        commit_time = self.transactions[tid].read_set.get(var)
        if commit_time is None:
            return

        writer_tid = next(
            (
                t_id
                for t_id, txn in self.transactions.items()
                if txn.start_time <= commit_time
                and txn.status == TransactionStatus.COMMITTED
                and var in txn.write_set
            ),
            None,
        )

        if writer_tid and writer_tid != tid:
            if writer_tid not in self.serial_graph:
                self.serial_graph[writer_tid] = set()
            self.serial_graph[writer_tid].add(tid)

            if self._detect_cycle():
                self._abort_transaction(self.transactions[tid])
                print(f"{tid} aborts due to serialization cycle")

    def _update_serial_graph_on_commit(self, tid: str):
        """
        Update the serialization graph when a transaction commits.
        
        Adds edges for:
        1. Write-after-read dependencies (this transaction read, another wrote)
        2. Read-after-write dependencies (this transaction wrote, another read)
        
        Args:
            tid: Transaction ID that is committing
        
        Returns:
            None
        
        Side effects:
            - Adds edges to self.serial_graph for all relevant dependencies
        """
        transaction = self.transactions[tid]

        for var in transaction.read_set:
            for other_tid, other_txn in self.transactions.items():
                if (
                    other_tid != tid
                    and var in other_txn.write_set
                    and other_txn.status == TransactionStatus.COMMITTED
                ):
                    if tid not in self.serial_graph:
                        self.serial_graph[tid] = set()
                    self.serial_graph[tid].add(other_tid)

        for var in transaction.write_set:
            for other_tid, other_txn in self.transactions.items():
                if (
                    other_tid != tid
                    and var in other_txn.read_set
                    and other_txn.status != TransactionStatus.ABORTED
                ):
                    if other_tid not in self.serial_graph:
                        self.serial_graph[other_tid] = set()
                    self.serial_graph[other_tid].add(tid)

    def _detect_cycle(self) -> bool:
        """
        Detect cycles in the serialization graph using depth-first search.
        
        A cycle in the serialization graph indicates that the transaction
        schedule is not serializable, requiring abortion of one or more
        transactions.
        
        Returns:
            True if a cycle is detected, False otherwise
        
        Side effects:
            None (uses local visited and recursion stack sets)
        """
        visited = set()
        rec_stack = set()

        def dfs(v: str) -> bool:
            visited.add(v)
            rec_stack.add(v)
            for neighbour in self.serial_graph.get(v, []):
                if neighbour not in visited:
                    if dfs(neighbour):
                        return True
                elif neighbour in rec_stack:
                    return True
            rec_stack.remove(v)
            return False

        for node in self.serial_graph:
            if node not in visited and dfs(node):
                return True
        return False

    def _update_serial_graph_for_ww_conflicts(self, tid: str):
        """
        Update serialization graph for write-write conflicts.
        
        Adds edges from transactions that previously wrote to the same
        variables to the current transaction, enforcing write ordering.
        
        Args:
            tid: Transaction ID that is committing with writes
        
        Returns:
            None
        
        Side effects:
            - Adds edges to self.serial_graph for write-write dependencies
        """
        transaction = self.transactions[tid]
        for var in transaction.write_set:
            for other_tid, other_txn in self.transactions.items():
                if (
                    other_tid != tid
                    and var in other_txn.write_set
                    and other_txn.status == TransactionStatus.COMMITTED
                    and other_txn.commit_time < transaction.start_time
                ):
                    if other_tid not in self.serial_graph:
                        self.serial_graph[other_tid] = set()
                    self.serial_graph[other_tid].add(tid)