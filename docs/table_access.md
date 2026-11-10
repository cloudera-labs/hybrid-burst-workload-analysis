# Table access and table sizes

`make table-access` answers: **which tables does each application read and
write, and how much of each?** A placement question needs it - if a set of
applications ran in another site, which tables would have to be there, and how
big are they. Moirai (Qiu et al., SOSP 2025) records the same two things, per
(job, table) bytes read and written plus a dated snapshot of every table's
directory size.

## What it writes

Under `processed/spark/`:

| File | Row | Fields |
|---|---|---|
| `table_access.json` | (app, table, direction) | `app_id, table, inventory_table, direction (read\|write), storage, format, bytes, bytes_source, files, partitions, operations, full_scan_bytes_max, location_root, location_truncated`, and for Kudu `rows, rows_source, columns, bytes_guess, guessed, guess_basis` |
| `table_access_apps.json` | app | `input_bytes, input_bytes_attributed, output_bytes, tables_read, tables_written, unresolved_scans, unresolved_writes, unmeasured` |
| `table_inventory.json` | distinct table | `table, storage, formats, location_root, location_truncated, apps_read, apps_written, bytes_read, bytes_written, full_scan_bytes_max` |

All bytes are integer, decimal bytes. Table names are normalised to `db.table`
in lower case; `spark_catalog.` and Kudu's `impala::` prefixes are dropped, so a
table has one name whichever engine wrote the plan.

`inventory_table` is the row's key into `table_inventory.json`: the table name,
or for a bare-path scan the table whose location contains it, or `path:<root>`.

## Where each number comes from

| Source in the plan | `storage`/`format` | `bytes` |
|---|---|---|
| `Scan parquet\|orc\|json\|csv\|avro\|text db.t` (file scan) | hdfs / its format | `size of files read`: the files touched after pruning (`bytes_source = files`) |
| `Scan hive db.t` (`HiveTableRelation`) | hdfs / hive | its stages' input, split by output rows (`stage`) |
| `BatchScan db.t` (Iceberg) | hdfs / iceberg | as Hive |
| `Scan Kudu impala::db.t` | kudu | as Hive |
| `Scan JDBCRelation(x)` | jdbc, table `jdbc:x` | as Hive |
| `InsertIntoHiveTable`, CTAS, `InsertIntoHadoopFsRelationCommand` | hdfs | `written output` (`metric`) |
| Iceberg `AppendData`, `ReplaceData`, `WriteDelta` | hdfs / iceberg | its execution's output bytes (`stage`) |
| Kudu through `SaveIntoDataSourceCommand` | kudu | as Iceberg |

- A **file scan of a bare path** has no table name. It keeps its location, and
  the inventory folds it into the table whose location contains it; one no
  table claims stays a `path:<root>` entry, which is still a measurable
  directory.
- **`WriteDelta`** (an Iceberg MERGE or UPDATE) never names its target. When its
  execution scans exactly one Iceberg table, that table is the target;
  otherwise the write is counted in `unresolved_writes`.
- **`location_root`** is the scan's `Location` with trailing `key=value`
  partition directories removed. A listing Spark cut short (`...`, or fewer
  paths shown than counted) is flagged `location_truncated` and is never
  measured as it stands.
- **Files touched, not bytes read.** A file scan's `bytes` is the size of the
  files it opened. Parquet and ORC read only the columns a query needs, so the
  sum over tables is larger than the tasks' `input_bytes` - often much larger.
  For a placement question that is the right figure: a replica copies files,
  not columns.
- **Coverage.** `input_bytes` equals `measurement_summary`'s
  `spark_total_input_read_gb` in bytes. `input_bytes_attributed` is the input of
  the stages that ran a recognised scan; the rest is RDD input, streams and
  caches, which no table accounts for.

## Kudu and Kafka: rows, not bytes

Spark counts bytes only for file sources and sinks. A Kudu scan, a Kudu write
and a Kafka write leave `bytes` at null, and `measurement_summary` shows 0 in
and 0 out for an application that does only these. Such an application is not
free to move: its data still crosses the network.

The Kudu connector counts rows in its own stage accumulators. This pass keeps
them:

| Field | What | Source |
|---|---|---|
| `rows` (read) | rows Kudu returned | `kudu.rows_read`, per stage |
| `rows` (write) | rows inserted, upserted, updated and deleted | `kudu.num_*`, a `{table=rows}` map |
| `rows_source` | `kudu.rows_read` or `kudu.num_*` | |
| `columns` | columns the scan projects | `Scan Kudu db.t [a#1,b#2]` |
| `bytes_guess` | `rows x columns x 16 B` | a guess |
| `guessed` | always true on these rows | |
| `guess_basis` | how the row and bytes were found, in words | |

- **A guess is never written into `bytes`.** `bytes` stays what Spark measured.
- **Bytes only where the columns are known.** A physical `Scan Kudu` shows the
  columns it projects. A Kudu read through a view (`CreateViewCommand`, run later
  by `Dataset.javaRDD`) shows only the relation, with all its columns, in the
  plan text. The same view can be scanned once for one column and once for all.
  So for a view, `rows` is set and `bytes_guess` and `columns` are null. On the
  first prod week, a guess from the relation's width gave 116 TB against 53 TB
  measured, which is not credible.
- **16 B per column** is `GUESS_BYTES_PER_COLUMN` in `table_access.py`: between an
  8-byte number and a short string. Column types are not in the event log.
- **A stage with no physical scan** (a view) splits its rows across the
  application's Kudu tables in proportion to the columns each view uses.
  `guess_basis` then says so.
- **A table seen only in a view's plan** gets a row with `operations` 0.
- **Kafka** is not a table. An application with a Kafka stage (`KafkaOutput`,
  `KafkaWriter`, ...) gets a Kafka target in `unmeasured`. The event log carries
  neither its topic nor its rows, so both are null.

`unmeasured` in `table_access_apps.json` is null for an application with no Kudu
or Kafka I/O. Otherwise:

    {"guessed": true, "bytes_per_column": 16,
     "rows_read", "rows_written", "rows_read_unattributed",
     "bytes_read_guess", "bytes_written_guess",
     "kafka_read", "kafka_write",
     "sources": [{"storage", "name", "rows", "bytes_guess"}],
     "targets": [{"storage", "name", "rows", "bytes_guess"}]}

## What it cannot give: a table's total size

That is a property of the storage, not of a query, and no event log carries it.
`full_scan_bytes_max` - the largest scan of a table with no partition filter -
is a lower bound and only that.

`scripts/hdfs_table_sizes.py` measures it. It is standalone (Python 3.8+, the
standard library, `hdfs`, optionally `beeline`) so it can be handed to the
cluster's administrators, and read-only: `hdfs dfs -count` per location, and
with `--beeline-url`, `DESCRIBE FORMATTED` (100 tables per beeline session) for tables whose location the plans
did not show (Hive scans, Iceberg, truncated listings). `--dry-run` prints every
command. It writes

    {"script_version", "collected_at", "host",
     "tables": [{"table", "location", "bytes", "files", "dirs", "status", "error"}]}

with `status` one of `ok`, `missing`, `no-location`, `not-hdfs`, `error`.
`bytes` is HDFS's CONTENT_SIZE, before replication. For an Iceberg table it is
everything under the table directory, metadata and not-yet-expired snapshots
included.
