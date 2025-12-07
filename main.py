import sys
import argparse
import re
from typing import TextIO, Optional, Dict, Any
from utils import TransactionManager


class RepCRec:
    """
    Replicated Concurrency Control and Recovery System.

    This class provides a command-line interface for managing distributed
    database transactions with support for replication, concurrency control,
    and failure recovery.
    """

    def __init__(self):
        """
        Initialize the RepCRec system.

        Creates a new transaction manager and initializes the set of active
        transactions.

        Side effects:
            - Creates a new TransactionManager instance
            - Initializes an empty set for tracking active transactions
        """
        self.tm = TransactionManager()
        self.active_transactions = set()

    def parse_command(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Parse a command line into a structured command dictionary.

        Supported commands:
            - begin(T1): Start a read-write transaction
            - beginRO(T1): Start a read-only transaction
            - R(T1,x2): Read variable x2 in transaction T1
            - W(T1,x2,100): Write value 100 to variable x2 in transaction T1
            - end(T1): End/commit transaction T1
            - fail(1): Fail site 1
            - recover(1): Recover site 1
            - dump(): Dump current state of all sites

        Args:
            line: A string containing a single command

        Returns:
            A dictionary containing the parsed command with keys like:
                - 'type': Command type (e.g., 'begin', 'read', 'write')
                - 'tid': Transaction ID (for transaction operations)
                - 'var': Variable name (for read/write operations)
                - 'val': Value to write (for write operations)
                - 'site': Site ID (for fail/recover operations)
            Returns None if the line is empty, a comment, or invalid.

        Side effects:
            - Prints error message to stdout if parsing fails
        """
        line = line.strip()
        if not line or line.startswith("//") or line.startswith("==="):
            return None

        try:
            # Match begin/beginRO
            if match := re.match(r"begin\(([T\d]+)\)", line):
                return {"type": "begin", "tid": match.group(1)}
            if match := re.match(r"beginRO\(([T\d]+)\)", line):
                return {"type": "beginRO", "tid": match.group(1)}

            # Match read operation: R(T1,x2)
            if match := re.match(r"R\(([T\d]+),\s*x(\d+)\)", line):
                return {
                    "type": "read",
                    "tid": match.group(1),
                    "var": f"x{match.group(2)}",
                }

            # Match write operation: W(T1,x2,100)
            if match := re.match(r"W\(([T\d]+),\s*x(\d+),\s*(-?\d+)\)", line):
                return {
                    "type": "write",
                    "tid": match.group(1),
                    "var": f"x{match.group(2)}",
                    "val": int(match.group(3)),
                }

            # Match end operation
            if match := re.match(r"end\(([T\d]+)\)", line):
                return {"type": "end", "tid": match.group(1)}

            # Match fail operation
            if match := re.match(r"fail\((\d+)\)", line):
                return {"type": "fail", "site": int(match.group(1))}

            # Match recover operation
            if match := re.match(r"recover\((\d+)\)", line):
                return {"type": "recover", "site": int(match.group(1))}

            # Match dump operation
            if line.strip() == "dump()":
                return {"type": "dump"}

        except Exception as e:
            print(f"Error parsing command '{line}': {str(e)}")
            return None

        return None

    def process_input(self, input_source: TextIO) -> None:
        """
        Process commands from an input source, handling multiple test cases.

        Reads the input line by line, grouping commands into tests separated
        by "// Test" markers. Each test is executed independently with a fresh
        transaction manager state.

        Args:
            input_source: A file-like object (file or stdin) containing commands

        Returns:
            None

        Side effects:
            - Prints test markers to stdout
            - Executes all commands in each test
            - Resets transaction manager state between tests
            - Modifies self.tm and self.active_transactions for each test
        """
        current_test = 0
        test_lines = []

        for line in input_source:
            line = line.strip()

            # Start of new test
            if line.startswith("// Test"):
                # Execute previous test if exists
                if test_lines:
                    self._execute_test(current_test, test_lines)
                current_test += 1
                test_lines = []
                print(f"\n=== Test {current_test} ===")
                continue

            # Skip empty lines at start of file
            if not test_lines and not line:
                continue

            test_lines.append(line)

        # Execute final test
        if test_lines:
            self._execute_test(current_test, test_lines)

    def _execute_test(self, test_num: int, test_lines: list) -> None:
        """
        Execute a single test case with the given commands.

        Resets the transaction manager and active transactions, then processes
        each command line in sequence. This ensures test isolation.

        Args:
            test_num: The test number (for reference, not used in execution)
            test_lines: List of command strings to execute in this test

        Returns:
            None

        Side effects:
            - Resets self.tm to a new TransactionManager instance
            - Clears self.active_transactions
            - Executes all commands, which may modify database state
            - Prints error messages to stdout if command execution fails
        """
        self.tm = TransactionManager()
        self.active_transactions = set()

        for line in test_lines:
            # Skip empty lines and comments within test
            if not line or line.startswith("//") or line.startswith("==="):
                continue

            command = self.parse_command(line)
            if command:
                try:
                    self.execute_command(command)
                except Exception as e:
                    print(f"Error executing command: {str(e)}")

    def execute_command(self, command: dict) -> None:
        """
        Execute a parsed command on the transaction manager.

        Dispatches the command to the appropriate transaction manager method
        based on the command type. Handles transaction lifecycle (begin, end)
        and delegates read/write operations to the transaction manager.

        Args:
            command: Dictionary containing parsed command with keys:
                - 'type': Command type (required)
                - 'tid': Transaction ID (for transaction operations)
                - 'var': Variable name (for read/write)
                - 'val': Value (for write)
                - 'site': Site ID (for fail/recover)

        Returns:
            None

        Side effects:
            - Modifies self.active_transactions (adds/removes transaction IDs)
            - Calls transaction manager methods that modify database state
            - Prints status messages and errors to stdout
            - May abort existing transactions with duplicate IDs

        Raises:
            Exception: If command execution fails (caught and printed)
        """
        cmd_type = command["type"]
        tid = command.get("tid")

        try:
            if cmd_type == "begin" or cmd_type == "beginRO":
                # Clear any existing transaction with this ID
                if tid in self.active_transactions:
                    self.tm._abort_transaction(self.tm.transactions.get(tid))
                    self.active_transactions.remove(tid)

                self.active_transactions.add(tid)
                if cmd_type == "begin":
                    self.tm.begin_transaction(tid)
                else:
                    self.tm.begin_read_only_transaction(tid)

            elif cmd_type == "end":
                if tid not in self.active_transactions:
                    print(f"Transaction {tid} does not exist")
                    return
                transaction = self.tm.transactions.get(tid)
                if transaction:
                    if transaction.write_set:
                        self.tm._commit_transaction(transaction)
                    else:
                        print(f"{tid} commits")
                self.active_transactions.remove(tid)
                if tid in self.tm.transactions:
                    del self.tm.transactions[tid]

            elif cmd_type == "fail":
                site_id = command["site"]
                self.tm.fail_site(site_id)

            elif cmd_type == "recover":
                site_id = command["site"]
                self.tm.recover_site(site_id)

            elif cmd_type == "dump":
                self.tm.dump()

            else:
                if tid not in self.active_transactions:
                    print(f"Transaction {tid} does not exist")
                    return
                if cmd_type == "read":
                    self.tm.read(tid, command["var"])
                elif cmd_type == "write":
                    self.tm.write(tid, command["var"], command["val"])

        except Exception as e:
            print(f"Error executing {cmd_type} command: {str(e)}")


def parse_args():
    """
    Parse command-line arguments for the RepCRec system.

    Configures argument parser to accept an optional input file. If no file
    is provided, the system reads from stdin.

    Returns:
        argparse.Namespace: Parsed arguments containing:
            - input_file: File object for reading commands, or None for stdin

    Side effects:
        - May exit the program if invalid arguments are provided (handled by argparse)
        - Opens the specified file for reading if provided
    """
    parser = argparse.ArgumentParser(
        description="Replicated Concurrency Control and Recovery System"
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        type=argparse.FileType("r"),
        default=None,
        help="Input file containing commands (default: stdin)",
    )
    return parser.parse_args()


def main():
    """
    Main entry point for the RepCRec system.

    Processes commands from either a file or stdin, executing them through
    a TransactionManager. Handles multiple test cases and automatically dumps
    the final state if no explicit dump() command was issued in a test.

    Args:
        None (uses command-line arguments via sys.argv)

    Returns:
        None

    Side effects:
        - Reads from file or stdin
        - Creates and manages TransactionManager instances
        - Prints test markers, command results, and state dumps to stdout
        - Closes input file if one was opened
        - Processes all database operations with full side effects
    """
    args = parse_args()
    input_source = args.input_file if args.input_file else sys.stdin

    tm = TransactionManager()
    has_dump = False
    in_test = False

    for line in input_source:
        line = line.strip()

        # Start of new test
        if line.startswith("// Test"):
            # If we were in a test and had no dump, dump the final state
            if in_test and not has_dump:
                print("\nFinal state:")
                tm.dump()

            # Reset for new test
            print(f"\n{line}")
            tm = TransactionManager()
            has_dump = False
            in_test = True
            continue

        # Process the current line if it's not empty or a comment
        if line and not line.startswith("//"):
            if line.lower() == "dump()":
                has_dump = True
            tm.process_operation(line)

    # Final dump only at the very end if needed
    if in_test and not has_dump:
        print("\nFinal state:")
        tm.dump()

    # Close file if we opened one
    if args.input_file:
        args.input_file.close()


if __name__ == "__main__":
    main()
