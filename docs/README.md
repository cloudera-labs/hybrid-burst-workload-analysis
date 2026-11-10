# Documentation

These pages explain what each command prints and how to read it.

Read the [main README](../README.md) first. It explains the setup and the commands.

## Data pipeline

- [Data pipeline](pipeline.md): how the download and process commands create the
  files in `data/`.

## Analysis

- [Ranking](ranking.md): rank the workloads by disaggregation sensitivity.
- [Summary](summary.md): the data volume, the network rate, the risk mix, and
  the compute you can move to the cloud.
- [Spark model features](spark_model_features.md): read the Spark event logs into
  the stage and job features that the transport-aware Spark model needs. Also
  explains `--split`, which is what lets one benchmark application become one row
  for each of its queries.
- [Baseline comparison](baseline_comparison.md): check a freshly collected TPC-DS
  baseline against the published one, structure first and timing second.
- [Stage matching](stage_matching.md): pair the stages of two runs so the model is
  calibrated on stages that really correspond. Explains why the pairing is a model
  input rather than a label.
- [Plan divergence](plan_divergence.md): measure the gap between the plan a query
  was submitted with and the plan that ran. The gap is a ceiling on any prediction
  made before the query starts.

## Terms

- **Disaggregation**: you separate the compute from the storage. Each data read
  then goes across the network, not to a local disk.
- **Workload**: one Impala query, or one Spark application.
- **Unit**: the slice of a Spark application that becomes one row. The whole
  application in production; one SQL execution when a benchmark harness runs many
  queries inside one application. See `--split`.
- **Intensity**: the number of bytes moved for each second that the workload
  runs. A high intensity fills the network link.
