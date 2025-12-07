# Replicated Concurrency Control and Recovery

## Team Members

- Shusheng Yang (sy4280)
- Qiutong Men (qm2017)

## Usage

### Environment Setup

```bash
pip install tabulate
```

### Run

```bash
python main.py < data.txt > out.txt
```

## Reprozip

### Environment Setup

```bash
pip install reprozip
pip install reprounzip
pip install reprounzip-docker
```

### Packing Project

```bash
reprozip trace python main.py < data.txt
reprozip pack submission.rpz
```

#### Unpack & Run

```bash
reprounzip docker setup submission.rpz ./submission
reprounzip docker run submission < data.txt
```

## Overview

### System Overview

Our project implements a distributed database system featuring:

- Serializable snapshot isolation
- Available copies replication
- Site failure and recovery handling
- Variable versioning and consistency management

### Code Organization

#### Module Structure

**main.py**

Command processing and execution control:
- Command parsing and validation
- Test case management
- Error handling

**utils.py**

Core components:
- TransactionManager: Central coordination
- Site: Data storage and versioning
- Transaction: Transaction state management
- Version: Variable version tracking

## Key Components

### Transaction Manager

**Major Functions**
- `begin_transaction(tid)`: Initiates new transaction
- `read(tid, var)`: Coordinates read operations
- `write(tid, var, val)`: Manages write operations
- `end_transaction(tid)`: Handles commit/abort

**State Management**
- Active transactions tracking
- Site status monitoring
- Serialization graph maintenance

### Site Manager

**Key Operations**
- Version history management
- Failure and recovery handling
- Data consistency maintenance

**Data Distribution**
- Even-indexed variables: All sites
- Odd-indexed variables: Single site

### Core Algorithms

#### Transaction Processing

**Read Operations**
- Version selection based on transaction start time
- Site availability checking
- Consistency verification

**Write Operations**
- Write caching until commit
- Available copies protocol
- First-committer-wins rule

**Commit Protocol**
- Serialization graph validation
- Write set propagation
- Version history updates

#### Recovery Management

**Failure Handling**
- Transaction abort decisions
- State cleanup

**Recovery Protocol**
- Non-replicated variable restoration
- Replicated variable accessibility control
- Version consistency maintenance

## Test Cases

We demonstrate system functionality through comprehensive test scenarios verifying concurrency control, conflict detection, and serialization requirements.

### Write-Write Conflicts and First-Committer-Wins

```
begin(T1)
begin(T2)
W(T1,x1,101)
W(T2,x2,202)
W(T1,x2,102)
W(T2,x1,201)
end(T2)
end(T1)
dump()
```

**Expected behavior:** T1 aborts while T2 commits, demonstrating first-committer-wins rule. Final state: x1=201, x2=202.

### Read-Write Ordering Without Conflicts

```
begin(T1)
begin(T2)
R(T1,x2)
R(T2,x2)
end(T1)
W(T2,x2,10)
end(T2)
```

**Expected behavior:** Both transactions commit successfully. Final state: x2=10.

### Read-Write Conflict Cycle Detection

```
begin(T1)
begin(T2)
R(T1,x2)
R(T2,x4)
W(T1,x4,30)
W(T2,x2,90)
end(T1)
end(T2)
```

**Expected behavior:** T1 commits, T2 aborts due to serialization cycle. Final state: x4=30, x2 unchanged.
