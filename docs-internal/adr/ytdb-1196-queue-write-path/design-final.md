# YTDB-1196 high-level design

YTDB-1196 is the identifier of this change in the JetBrains issue tracker, inside the project named
YTDB. YouTrackDB is the graph database that this change modifies. A graph database stores records
and stores links between records. The name "the YTDB-1196 change" refers to this change everywhere
in this document.

The current document stays at a high level. The current document names no method, no data field,
and no step of code. The design record holds every detail below that level. The design record is
the file `research-log.md` at the repository root.

## Words used in this document

The table below lists every term of this document that is not ordinary technical English. Each
concept has exactly one chosen name. The third column lists other names for the same concept. The
current document never uses a name from the third column.

| Chosen name | Plain meaning | Names not used in the current document |
|---|---|---|
| the YTDB-1196 change | The change that the current document describes | this change, the change |
| design record | The file `research-log.md` at the repository root, which holds every design detail | the record, the design log |
| storage layer | The part of YouTrackDB that owns the database files and performs every commit | storage |
| frontend transaction | The in-memory object that collects the changes of one client before a commit | live transaction, in-memory transaction object |
| caller scope | The client-side owner of one write and of one later read | client, session, logical client scope |
| direct write path | The write path that YouTrackDB uses today, in which a caller hands a frontend transaction to the storage layer | direct path, current path |
| queue write path | The new write path that sends a portable envelope through a queue | queued path, queue path |
| portable envelope | The self-contained description of one complete write, free of any thread and any session | envelope, detached write description, portable mutation envelope, admitted work |
| logical value graph | The set of new records, changed records, and links inside one portable envelope, stated without storage addresses | value graph, record graph |
| symbolic identifier | A temporary name for a new record that has no storage identifier yet | symbol, temporary name |
| storage identifier | The permanent address that the storage layer assigns to a record | physical identifier, real identifier, record identity |
| preparation stage | The caller-thread step that builds one portable envelope | preparation, prepare step |
| database mutation coordinator | The component that admits one authorized portable envelope at a time into a bounded queue | coordinator, queue owner |
| apply consumer | The single background worker that takes admitted portable envelopes in admission order | consumer, background worker, apply worker |
| resolver | The role of the apply consumer that replaces every symbolic identifier with a storage identifier | resolution step |
| applier | The role of the apply consumer that writes admitted work into storage | apply step, writer |
| atomic operation | A group of storage changes in which every change succeeds together or every change rolls back together | one operation, transaction boundary |
| write-ahead log | The storage engine file that records a change before the data files change | the log, WAL |
| commit timestamp | The ordered number that the storage layer assigns to every commit | sequence number, ordered number |
| storage committed state | The state of a commit that a reader can observe and that no rollback undoes | committed |
| storage error state | The existing failure state of YouTrackDB, which stops writes until an operator restarts the server | failure state, error state |
| result UNKNOWN | The commit result that the server returns when the server cannot determine the outcome | in-doubt result, undetermined result |
| data snapshot | One fixed view of the record data that serves one whole read | snapshot, fixed view |
| metadata publication bundle | The immutable set of schema state, index state, collection state, and configuration state that a reader may observe | bundle, reader state, published metadata |
| publication pairing rule | The rule that makes a record and the metadata that describes the record visible together, never separately | data and metadata publication pairing rule, pairing rule |
| server run | The period between one start of the server and the next stop of the server | run, server lifetime |
| run identifier | The in-memory value that names one server run | run name, generation |
| visibility token | The small value that a successful commit on the queue write path returns to a caller | token |
| causal context | The client-side memory that holds the newest visibility token of one caller scope | session token store |
| read barrier | The read-path step that validates a visibility token and then waits for the named commit | barrier |
| equivalence test | The test that sends one input through both write paths and compares both results | comparison test |
| canonical binary manifest | One byte sequence that describes a write result in a fixed order, so two results compare exactly | manifest, comparison format |
| benchmark module | The separate measurement module that drives both write paths and reports the cost of each write path | benchmark, harness |
| drift control | Drift control is a check that detects unwanted changes in measurement conditions over time. | drift check |
| confidence interval | A confidence interval is a statistical range that states how uncertain a measurement is. | statistical interval |
| test-only direct adapter | The test-code entry point that calls the direct write path | test adapter, adapter |
| track | One unit of implementation work with its own deliverables, its own dependencies, and its own verification | work package, phase |

## Intention

The YTDB-1196 change prepares the write path of YouTrackDB for a queue. A write changes one record.
A write may also change several records inside one transaction. A transaction becomes permanent
through a commit. The storage layer performs every commit. The storage layer also owns the database
files.

The main purpose of the YTDB-1196 change is architectural preparation. A consensus layer is a
component that keeps several machines in agreement on one ordered list of writes. Raft is one
published consensus algorithm of that kind. Every consensus layer needs one ordered queue of
detached write descriptions. A portable envelope is one such detached write description.

The direct write path of YouTrackDB offers no queue of portable envelopes. The YTDB-1196 change
builds the missing queue. A later change can then add a consensus layer above the queue.

Three properties of the direct write path prevent a queue. The first property is the handover of
the frontend transaction. The direct write path passes the frontend transaction into the storage
layer. The frontend transaction belongs to one caller thread. No queue can therefore hold the
frontend transaction.

The second property is late identifier assignment. The storage commit chooses the storage
identifier of every new record. No complete description of one write exists before the commit.

The third property is the silent commit result. The commit returns no commit timestamp. No later
read can therefore name a finished commit.

The YTDB-1196 change removes the direct handover of the frontend transaction. The YTDB-1196 change
adds one ordered queue between the caller thread and the storage layer. The YTDB-1196 change also
returns a visibility token to the caller. A visibility token is one small ordered value that names
one finished commit.

The visibility token gives a secondary benefit. A client may send the visibility token back with a
later read or write. The token requires that the later operation observes the finished commit. The
secondary benefit is not the main requirement of the YTDB-1196 change.

The YTDB-1196 change builds the queue. The YTDB-1196 change also measures the cost of the queue.
Production write traffic keeps the direct write path. A later change moves production write traffic
onto the queue, under a separate issue. Raft integration also belongs to that later work.

## Goals

Every goal below states one measurable outcome.

1. The queue write path carries a detached portable envelope. No live frontend transaction reaches
   the storage layer.
2. The database mutation coordinator reorders no admitted portable envelope.
3. Every admitted portable envelope reaches exactly one recorded outcome.
4. A later change can extend the encoding of a portable envelope. An older reader of the encoding
   detects an encoding that the older reader cannot read.
5. The equivalence test sends one input through both write paths. Both write paths produce one
   identical canonical binary manifest.
6. Every existing public commit method keeps the current signature. Every existing public query
   method also keeps the current signature.
7. Every successful commit on the queue write path returns one visibility token.
8. A read that carries a visibility token of the current server run observes a data snapshot that
   contains the named commit.
9. A read that carries a visibility token of an earlier server run fails at once. The server returns
   one named rejection error.
10. A caller that sends no visibility token performs no wait before a read.
11. No caller that sends a visibility token of the current server run receives data older than the
    named commit.
12. The benchmark module reports the cost of the direct write path against the cost of the queue
    write path. The benchmark module uses drift control and reports paired confidence intervals.
13. The benchmark module covers one writer, several writers, bursts of offered work, one writer
    together with several readers, and several writers together with several readers.
14. No write path that the YTDB-1196 change touches holds a lock while a commit runs. An automated
    check enforces the rule at build time.
15. Every approved exception to the rule of goal 14 is recorded in one reviewed place.
16. A database shutdown discards no write that the system already accepted. The apply consumer
    finishes every portable envelope that entered the queue before the shutdown closed admission.

## Components

The two diagrams below name every component of the YTDB-1196 change. The first diagram shows the
write path. The second diagram shows the read path. A third block lists the supporting components.

```text
LEGEND
  NEW      the YTDB-1196 change adds the component
  CHANGED  the YTDB-1196 change modifies an existing component
  EXISTING the YTDB-1196 change leaves the component unchanged


WRITE PATH

  +--------------------------------+
  | Caller scope           CHANGED |  gains one causal context
  +--------------------------------+
                 |
                 |  one frontend transaction and an optional visibility token
                 v
  +--------------------------------+
  | Preparation stage          NEW |  runs on the caller thread
  +--------------------------------+
                 |
                 |  one portable envelope, detached from the caller thread
                 v
  +--------------------------------+
  | Database mutation          NEW |  bounded queue, insertion order only
  | coordinator                    |
  +--------------------------------+
                 |
                 |  one admitted portable envelope at a time
                 v
  +--------------------------------+
  | Apply consumer             NEW |  the resolver runs first
  |   resolver, then applier       |  the applier runs second
  +--------------------------------+
                 |
                 |  one atomic operation
                 v
  +--------------------------------+
  | Storage layer          CHANGED |  data files, write-ahead log, commit timestamp
  +--------------------------------+
                 |
                 |  reaches the storage committed state
                 v
  +--------------------------------+
  | Commit result plus         NEW |  returns to the caller scope
  | visibility token               |
  +--------------------------------+
                 |
                 v
  +--------------------------------+
  | Causal context             NEW |  keeps the newest visibility token
  +--------------------------------+


READ PATH

  +--------------------------------+
  | Caller scope           CHANGED |  reads the visibility token from the causal context
  +--------------------------------+
                 |
                 |  one visibility token, or no visibility token at all
                 v
  +--------------------------------+
  | Read barrier               NEW |  validates the visibility token
  +--------------------------------+
                 |
                 |  waits until the named commit is observable
                 v
  +--------------------------------+
  | Storage layer          CHANGED |  hands out one data snapshot
  +--------------------------------+
                 |
                 v
             read result


SUPPORTING COMPONENTS

  Run identifier            NEW   in memory only, names the current server run,
                                  and appears inside every visibility token
  Benchmark module          NEW   drives both write paths and reports the cost of each
  Test-only direct adapter  NEW   test code only, absent from production code
  Equivalence test          NEW   compares the result of both write paths
```

### How the components relate to each other

One caller scope is an embedded database connection, a pooled graph object, or a remote client
connection. The YTDB-1196 change adds one causal context to every caller scope. The causal context
belongs to the caller scope and to no other component.

The preparation stage runs on the caller thread. The preparation stage checks permissions. The
preparation stage checks size limits. The preparation stage then builds one portable envelope from
the frontend transaction. Every portable envelope holds one logical value graph. Each new record
inside the portable envelope carries one symbolic identifier.

A caller may hand a visibility token to the preparation stage together with a write submission. The
visibility token then requires the write transaction to observe the results of the named earlier
write transaction. The preparation stage hands the portable envelope to the database mutation
coordinator. The database mutation coordinator hands one admitted portable envelope at a time to
the apply consumer.

The apply consumer is the only component that calls the storage layer on the queue write path. The
storage layer publishes the result. The result then travels back to the caller scope as a commit
result plus a visibility token.

On the read path, a caller scope may hand a visibility token to the read barrier. The read barrier
compares the visibility token against the run identifier and against the data snapshot. The read
barrier waits until the named commit is observable. The read then observes a data snapshot that
contains the named commit. The read barrier returns a failure when the requirement cannot be met.

### Ownership of the queue and the path of a result

The database mutation coordinator belongs to one storage. The database mutation coordinator owns
two things. The database mutation coordinator owns admission of new work. The database mutation
coordinator owns the capacity of the queue. The database mutation coordinator owns no lock of the
storage layer.

Admission is the step that places one portable envelope into the queue. Admission checks the
authorization of the portable envelope. A failed admission leaves no queue entry and no recorded
work. The admission order is the insertion order into the queue. The database mutation coordinator
never reorders admitted work.

The queue has a bounded number of entries. The queue also has a bounded total size in bytes. A
caller that finds no free capacity waits for capacity. A waiting caller holds no lock of the storage
layer while the caller waits. Waiting for capacity is the only mechanism that slows a fast writer
down.

Every queue entry reaches exactly one recorded outcome. The apply consumer normally produces that
outcome.

A commit method that uses the queue write path waits until the entry of that commit reaches an
outcome. The commit method keeps the current signature. The commit method also keeps the current
synchronous behaviour. Cancellation stops no entry after admission.

A shutdown of the server closes admission first. The database mutation coordinator then collects
every entry that is already admitted. The apply consumer finishes every collected entry. The
storage layer shuts down only after the last collected entry reaches an outcome. A shutdown therefore
discards no admitted work.

### The apply consumer

The apply consumer contains two roles. The first role is the resolver. The second role is the
applier.

The resolver runs first. The resolver replaces every symbolic identifier with a storage identifier.
The applier then writes records, schema, indexes, and configuration in one atomic operation. The
publication pairing rule is deferred work. Non-goal 15 explains that limit.

### The storage layer and the reader state

The storage layer owns the data files. The storage layer owns the write-ahead log. The storage layer
also owns the commit timestamp. The YTDB-1196 change uses the commit timestamp inside the visibility
token. The YTDB-1196 change adds no durable state. The publication pairing rule is outside the
YTDB-1196 change, as non-goal 15 states.

The server holds the run identifier in memory only. The server writes no run identifier to disk.
Every visibility token carries the run identifier of the server run that produced the visibility
token. The read barrier rejects a visibility token of a different server run. The read barrier
otherwise waits until the named commit belongs to the data snapshot.

### The causal context

The causal context stores the newest visibility token of one caller scope. The commit path writes
into the causal context after every successful commit on the queue write path. The read barrier
reads the causal context. The read barrier writes no value into the causal context.

Two visibility tokens can exist inside one caller scope at the same time. The causal context then
keeps the visibility token with the higher commit timestamp. A later commit inside one caller scope
therefore replaces an older visibility token.

### The benchmark module and the acceptance model

The benchmark module drives both write paths. The benchmark module reports the cost of each write
path. No production caller takes part in a benchmark run. An operator dispatches every benchmark run
by hand.

The benchmark module runs two mixed workloads. Workload one uses one writer together with several
readers. Workload two uses several writers together with several readers. Both mixed workloads
measure the cost of the read barrier. Every reader of both mixed workloads carries a visibility
token of the current server run.

The benchmark module reports four kinds of number for every workload. The first kind is successful
throughput. The second kind is median latency. The third kind is ninety-ninth percentile latency.
The fourth kind is full outcome accounting.

Full outcome accounting means that every offered operation lands in exactly one bucket. The four
buckets are successful, rejected, failed, and timed out. Full outcome accounting prevents a report
that hides lost demand.

The benchmark module compares the two write paths in pairs. One pair contains one measurement of
the direct write path and one measurement of the queue write path. The two measurements of one pair
run close to each other in time. The benchmark module balances the order of the two measurements
across runs. The benchmark module reports the paired difference together with a confidence interval.
The confidence interval states how uncertain the reported difference is.

The benchmark module uses drift control before and after each block of work. The two control
measurements use the same fixed input. A large difference between the two control measurements means
that measurement conditions changed during the block. The benchmark module then discards the block.

The first run of the benchmark module establishes the baseline. No numeric acceptance threshold
gates the first run. The acceptance thresholds are set after the first run, by a decision of the
project owner. The benchmark report therefore states measurements and intervals, and the benchmark
report states no verdict.

The direct write path stays in place because changing that path is too risky for the YTDB-1196
change. Production writes continue to use the direct write path during the YTDB-1196 change. The
long-term aim is that every write travels through the queue write path. That long-term aim is work
outside the YTDB-1196 change. After that later move, tests remain the only user of the direct write
path. The test-only direct adapter calls that path for those tests.

No production code references the test-only direct adapter.

## Approach

### The normal path of one write and one following read

```text
STEP                            WHAT HAPPENS

caller scope                    collects the changes into a frontend transaction
        |
        v
preparation stage               checks permissions and size limits
                                builds one portable envelope
        |
        v
database mutation coordinator   waits for free capacity
                                admits the portable envelope in insertion order
        |
        v
apply consumer, resolver        replaces every symbolic identifier
                                with a storage identifier
        |
        v
apply consumer, applier         writes records, schema, and indexes
                                inside one atomic operation
        |
        v
storage layer                   reaches the storage committed state
        |
        v
visibility token                returns to the caller scope with the commit result
        |
        v
causal context                  keeps the newest visibility token of the caller scope
        |
        v
read barrier                    waits until the named commit belongs to the
                                pinned data snapshot
        |
        v
read                            observes data that already contains the write
```

The storage committed state promises no durability. A restart of the server can therefore lose a
commit that reached the storage committed state. Non-goal 7 states the same limit. A read without a
visibility token skips the read barrier completely.

### The failing paths, at a high level

A write can fail at four points of the path. A failure before admission returns a rejection error,
and no commit timestamp exists. A failure after admission and before the write starts also returns a
rejection error, and nothing needs undoing. A failure during the write returns a conflict error, and
the storage layer rolls the whole write back. A failure after the storage committed state returns
the result UNKNOWN, and no visibility token exists.

The design record holds the exact list of failure conditions. The file
`.pi/slate/design-ytdb-1196-lowlevel-notes.md` holds the detailed failure table of the earlier
version of the current document.

### The results of a read barrier

A read that carries a visibility token reaches exactly one of four results. The first result reports
that the named commit belongs to the pinned data snapshot. The second result reports that the bounded
wait expired before the named commit became observable. The third result reports that the visibility
token names an earlier server run. The fourth result reports that the storage layer is in the
storage error state.

No result of a read barrier returns data older than the named commit. A read without a visibility
token skips the read barrier and therefore reaches no result of a read barrier.

### Evidence rules and state-transition rules

The failing paths follow three rules. The three rules protect one property. The property is that the
server never reports more than the server can prove.

Rule one covers the count of outcomes. The server records exactly one outcome for every admitted
portable envelope.

The mechanism behind rule one is one small state value per queue entry. Every move to a final state
is one atomic replacement of that state value. Exactly one replacement can succeed. A second
producer of an outcome therefore always loses. The single winner also releases the memory that the
entry reserved.

Rule two covers the strength of a claim. No result claims more than the evidence supports. The
server calls a commit observable only after the storage layer reports the commit as committed. The
server calls a commit durable only after the write-ahead log reports the write as persisted.

A metadata change follows the same principle. The applier reads the current metadata state under the
locks of the apply step. The applier compares that state against the state that the caller observed
during preparation. A mismatch makes the write fail with a conflict error. The applier never guesses
the intent of the caller from a name alone.

Rule three covers an outcome that the server cannot determine. Such an outcome stops further writes.
The caller receives the result UNKNOWN. The result UNKNOWN describes the knowledge of the caller.
The result UNKNOWN does not describe an undecided state of the database.

Restart recovery later derives the real outcome from the write-ahead log. Writes resume after the
restart.

Each queue entry passes through a fixed sequence of states. The entry is first admitted. The apply
consumer then claims the entry and starts the write. The write-ahead log then holds the end record
of the write. The storage layer then reports the commit. The publication step then makes the result
observable.

Each of those points corresponds to exactly one class of failure result. A failure before the log
end record can roll back. A failure after the log end record cannot roll back, so the result is
UNKNOWN.

### The effect of a restart of the server

A restart of the server changes the treatment of every visibility token. The new server run holds a
new run identifier in memory. A visibility token of an earlier server run names a different run
identifier. The read barrier rejects such a visibility token at once. The client then clears the
visibility token. The client then repeats the read without a visibility token.

## Non-goals

Every non-goal below carries one reason.

1. The YTDB-1196 change integrates no Raft library. The project owner wants the cost of the queue
   measured before a consensus layer is added.
2. The YTDB-1196 change implements no leader election and no leader lease. No consensus layer exists
   in the YTDB-1196 change.
3. The YTDB-1196 change supports no partition ownership and no transaction across several machines.
   The queue admits work inside one server process only.
4. No production write passes through the queue in the YTDB-1196 change. The move of production
   traffic belongs to a later change with a separate design.
5. No visibility token works after a restart of the server. The server holds the run identifier in
   memory only.
6. The YTDB-1196 change adds no new durable state to a database directory. The storage layer already
   recovers the commit timestamp during replay of the write-ahead log.
7. The YTDB-1196 change gives no durability promise for a returned visibility token. The default
   commit path returns before the write reaches the disk.
8. The YTDB-1196 change offers no fallback to older data after a failed wait. A caller that accepts
   older data can send no visibility token.
9. The YTDB-1196 change routes no lifecycle work and no maintenance work through the queue. Database
   creation, backup, recovery, checkpoint, and flush change no user data.
10. The YTDB-1196 change accepts no externally supplied record version and no externally supplied
    commit timestamp. The local storage layer owns both values.
11. The YTDB-1196 change reduces no operator work in general, because the YTDB-1196 change adds no
    operator tooling. Rare operator work stays a general aim of the project owner, as the design
    record states.
12. The YTDB-1196 change proves nothing about code outside the touched write paths. The automated
    check of goal 14 covers the touched write paths only.
13. The YTDB-1196 change carries record writes and schema writes only. The scheduler family, the
    sequence family, the function family, and the security family pass through no queue in the
    YTDB-1196 change. Those four write families move to a later change.
14. The YTDB-1196 change carries no network transport of a visibility token. The network agreement
    that hands a visibility token to a server belongs to a later change.
15. The YTDB-1196 change does not deliver the publication pairing rule. A later change will make a
    record and the metadata that describes the record visible to readers together, never separately.
    The publication pairing rule belongs to later work because adding the rule would make the
    YTDB-1196 change too large.
16. The YTDB-1196 change does not deliver an optional caller deadline before admission. A later
    change may add that deadline to limit how long a caller waits for queue capacity.
17. The read barrier does not reject a visibility token because the commit timestamp looks
    unreachable. A client may read from a server that has not yet received the named commit, so the
    server must not reject the visibility token for that reason.
18. The YTDB-1196 change does not carry a visibility token from one server to another server. That
    transport belongs to later work because decision D47 in the design record defers the case.

An earlier version of the current document carried a non-goal about the durability-order gap. The
durability-order gap is the difference between the disk order of writes and the order of commit
timestamps. The current document removes that non-goal. The reduced scope rejects every visibility
token of an earlier server run, so the gap cannot be observed. Consensus work must revisit
durability order, as the design record states.

## Open items

The current document lists no open item of its own. The design record holds the approved split of
the work into nine tracks. The first benchmark run sets the baseline, so no numeric threshold gates
the first run.

### The nine-track split

The work of the YTDB-1196 change is split into nine tracks. A track is one unit of implementation
work with its own deliverables, its own dependencies, and its own verification. The first track
carries the preparatory rules for single-process ownership of a storage directory.

The later tracks build the encoding, the queue, and the apply step. The last tracks build the
visibility token, the read barrier, the equivalence test, and the benchmark. Each track depends on
earlier tracks, so the tracks run mostly in order. The design record holds the full track table with
the dependencies and the verification of each track.

| Track | Short name | Deliverables in brief |
|---|---|---|
| T1 | Single-process ownership foundation | Ownership declarations, lock-order assertions, and focused rule tests |
| T2 | Lock-across-transaction compiler check | The automated build-time check, its fixtures, and the allow-list of approved exceptions |
| T3 | Sequence lock separation | Refactoring of four confirmed sequence sites, and removal of their temporary exceptions |
| T4 | Portable model and deterministic codecs | Portable envelope types, encoders, size limits, and fixed-byte fixtures |
| T5 | Session-free apply and publication | Resolution, authorization revalidation, one-operation apply, rollback, and publication |
| T6 | Run-local token and read barrier | Run-local visibility tokens, data snapshots, bounded waiting, and unavailable storage |
| T7 | Test-only direct adapter | Queue lifecycle states, memory reservations, shutdown, and the test-only direct adapter |
| T8 | Direct-versus-queued equivalence | Canonical binary manifests, deterministic inputs, and the comparison matrix |
| T9 | Paired queue benchmark | Deterministic datasets, balanced schedules, accounting, estimators, and manifests |
