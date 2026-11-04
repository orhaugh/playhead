# playhead

A video service's playback analytics on [clink](https://github.com/orhaugh/clink),
on Kubernetes, with every third-party system it talks to running beside it in
Docker. Four SQL jobs read the players' events and the CDN's edge logs from Kafka,
and keep a quality-of-experience table in Iceberg on S3, publish alerts to Kafka,
send the CDN vendor per-location minutes in its GCS bucket, and write finance's
billing ledger to Azure. A half hour of viewing, generated deterministically, is
played through them at fifteen times real time, with three faults on the way, and
every output is then checked against what the tape says it must hold. Then the
CDN's job scales to zero: with the half hour over it parks itself, holding no
slots, until the vendor's next batch of logs wakes it.

## What runs

| Job | Reads | Writes | Delivery |
| --- | --- | --- | --- |
| `qoe` | `playback.events`: Avro, two schema versions, through the Schema Registry | `qoe.title_minute`, an Iceberg table on S3 behind a REST catalog: starts, plays, failures, rebuffers and stall time per title per minute | exactly once: each checkpoint commits one Iceberg snapshot |
| `alerts` | `playback.events` | `qoe.alerts` on Kafka, in Avro: a title whose sessions fail to start, five failures or more in a minute and more failures than successes | exactly once: a transaction per checkpoint, read `read_committed` |
| `edges` | `cdn.edge`: Protobuf, through the Schema Registry | `gs://cdn-vendor/pop-minute`: requests, bytes, server errors and time to first byte per edge location per minute, as typed Parquet | exactly once: each file staged, then published when its checkpoint commits |
| `ledger` | `playback.events` | `azure://finance/billable-sessions`: one row for every session that started, as typed Parquet | exactly once, with the storage account's key from a Kubernetes Secret (`env://`), never in the SQL |

The jobs are the four files in [`pipeline/`](pipeline). Each is one self-contained
script: the tables it reads and writes, and one `INSERT INTO`.

The cluster is a kind cluster of one node. clink runs there with two
Coordinators, leader elected over a shared directory, and two Workers of ten
slots each. Every pod requires the HTTP API token, the Workers reach the
Coordinators over mutual TLS, and the Secrets for both are made by
[`scripts/secrets.sh`](scripts/secrets.sh). The clink operator runs it: a
`ClinkCluster` ([`k8s/cluster.yaml`](k8s/cluster.yaml)) and one `ClinkJob` per
job, which the operator submits over the Coordinator's API with the token and
checkpoints every two seconds. `DEPLOY=chart` runs the same cluster from clink's
Helm chart ([`k8s/values.yaml`](k8s/values.yaml)) instead, with each job submitted
by the `clink` CLI.

The cloud is [`cloud/compose.yml`](cloud/compose.yml): Redpanda for Kafka and the
Schema Registry, LocalStack for S3, the Iceberg REST catalog, fake-gcs-server,
Azurite, Marquez for lineage, and an OpenTelemetry Collector in front of Jaeger and
Prometheus. Each container gets a Service in the cluster's `cloud` namespace, so a
pod names `redpanda.cloud.svc.cluster.local` as it would a managed service. The
Coordinators export lifecycle spans and metrics over OTLP, the Workers their
metrics, and every job's lineage reaches Marquez as OpenLineage run events.

## The tape

[`tools/gen.py`](tools/gen.py) writes a deterministic half hour of viewing,
then five minutes of ordinary traffic so that every checked window closes.
Each session starts, shows its first frame or fails, stalls, switches bitrate and
stops, and fetches a segment from a CDN edge every few seconds while it plays.
Two incidents are planted: from minute 12 to 16 one title's new encode fails to
start on most devices, and from minute 20 to 23 one edge location answers slowly
and sometimes with a 503. App 5.0 rolls out at minute 15; its events carry the
network the player was on, in a second Avro schema registered as it ships.

[`tools/feed.py`](tools/feed.py) plays the tape into Kafka at `SPEED` times real
time, and on the way:

1. at minute 8, the edges job is upgraded to a new version of its script, which
   waits twice as long before it sets a quiet partition aside. Applied as a change
   to its `ClinkJob` while the tape plays on, it goes out through a savepoint: the
   operator ends the job with a savepoint that cancels it, so no checkpoint commits
   after it, and submits the new script restored from it;
2. at minute 14, inside the failing-title incident, a Worker is killed with
   SIGKILL;
3. at minute 20, the leading Coordinator's pod is killed. The standby takes the
   leadership and recovers the jobs, and the killed pod comes back as the new
   standby.

The generator also writes the CDN's next six minutes of edge logs, from a random
stream of its own so the tape is the same either way, for the scale-to-zero
scene once the half hour is over and checked.

## Run it

You need Docker with about 8 GB for it, kind, kubectl, Python 3 and OpenSSL, Helm
for `DEPLOY=chart`, and a clink checkout beside this one (`../clink`, or
`CLINK_SRC`). The run builds the operator from that checkout, at `CLINK_REF`.

playhead needs clink after 0.15.0: typed SQL tables on object-store Parquet, the
chart's and the operator's auth, TLS and Secrets, SQL `ClinkJob`s, and the fixes
its upgrade, failover and scale to zero turned up. Until a release carries them, run it against
a runtime image built from clink at that commit:

```sh
CLINK_IMAGE=clink-runtime:<tag> CLINK_REF=<commit> ./run.sh
```

| Variable | Default | Effect |
| --- | --- | --- |
| `DEPLOY` | `operator` | `chart` runs clink from the Helm chart, its jobs submitted by the CLI |
| `SPEED` | `15` | the tape's speed over real time |
| `UPGRADE` | `1` | `0` skips the edges upgrade (operator only) |
| `PARK` | `1` | `0` skips the scale-to-zero scene (operator only) |
| `KILL` | `1` | `0` skips the Worker kill |
| `FAILOVER` | `1` | `0` skips the Coordinator failover |
| `CLEANUP` | `0` | `1` removes the cluster and the cloud at the end |

Every run starts from empty: the cloud is recreated, clink is reinstalled with an
empty HA directory, and the topics, subjects, buckets and namespace are made again
([`scripts/provision.sh`](scripts/provision.sh)).

## What the checks hold it to

[`tools/check.py`](tools/check.py) works out from the tape what every output must
hold, then reads each one back:

| Output | Read | Must hold |
| --- | --- | --- |
| `qoe.title_minute` | through the REST catalog, with PyIceberg | one row per title and minute, with the counts the tape gives, none twice |
| `qoe.alerts` | `read_committed`, decoded through the Schema Registry | exactly the failing title-minutes, once each |
| `gs://cdn-vendor/pop-minute` | every object the sink published | one row per edge location and minute, none twice |
| `azure://finance/billable-sessions` | every object the sink published | one row per session that started, none twice |

Every window that ends by minute 33 is checked: the tape's last five minutes
carry the watermark past them. Then the cluster itself: the API refuses a request
without the token, the leader accepts a Worker's certificate and refuses a client
without one, every `ClinkJob` runs, the upgraded job started from the savepoint
its predecessor ended at, the standby took the leadership, and the traces,
metrics and lineage reached Jaeger, Prometheus and Marquez.

A run through the operator, with the three faults, on a runtime image built from
clink at `fe982e37`:

```text
check: the tape says 396 title-minutes, 4 alerts (['t07']), 198 edge-location minutes up to minute 33, 6000 billable sessions
  qoe.title_minute (Iceberg on S3): 396 windows for 396 expected, 396 as the tape says, 0 repeated, 0 unexpected  OK
  qoe.alerts (Kafka, read_committed): 4 windows for 4 expected, 4 as the tape says, 0 repeated, 0 unexpected  OK
  gs://cdn-vendor/pop-minute (GCS, exactly once): 198 windows for 198 expected, 198 as the tape says, 0 repeated, 0 unexpected  OK
  azure://finance/billable-sessions (Azure, exactly once): 6000 sessions for 6000 expected, 6000 as the tape says, 0 repeated, 0 unexpected  OK
  security: the API refuses a request without the token: 401  OK
  security: and answers the cluster's token: 200  OK
  security: the readiness probe answers without it: standby  OK
  security: a Worker's certificate is accepted by the leader at clink-coordinator-1  OK
  security: a client without a certificate is refused  OK
  operator: ClinkJob qoe is Running, job 1  OK
  operator: ClinkJob alerts is Running, job 2  OK
  operator: ClinkJob edges is Running, job 5  OK
  operator: ClinkJob ledger is Running, job 4  OK
  operator: the edges upgrade replaced job 3 with job 5, restored from its savepoint /var/lib/clink/checkpoints/edges/_jobs/3@18  OK
  failover: leadership at epoch 2 under clink-coordinator-1, running ['alerts', 'edges', 'ledger', 'qoe']  OK
  traces (Jaeger): ['clink-coordinator'], 9 clink.submit and 280 clink.checkpoint spans  OK
  metrics (Prometheus): series from ['clink-coordinator', 'clink-worker']  OK
  lineage (Marquez) qoe: RUNNING, ['kafka://redpanda.cloud.svc.cluster.local:9093/playback.events'] -> ['http://iceberg-rest.cloud.svc.cluster.local:8181/qoe.title_minute']  OK
  lineage (Marquez) alerts: RUNNING, ['kafka://redpanda.cloud.svc.cluster.local:9093/playback.events'] -> ['kafka://redpanda.cloud.svc.cluster.local:9093/qoe.alerts']  OK
  lineage (Marquez) edges: RUNNING, ['kafka://redpanda.cloud.svc.cluster.local:9093/cdn.edge'] -> ['gs://cdn-vendor/pop-minute']  OK
  lineage (Marquez) ledger: RUNNING, ['kafka://redpanda.cloud.svc.cluster.local:9093/playback.events'] -> ['azure://finance/billable-sessions']  OK
check: PASS
```

`DEPLOY=chart` passes the same checks, less the operator's.

## Scale to zero

A job that only has work now and then should not hold Worker slots all night.
The edges job declares when it parks and what wakes it, in
[`pipeline/edges.policy.json`](pipeline/edges.policy.json), which
[`scripts/job.sh`](scripts/job.sh) puts into its `ClinkJob`:

```json
"suspendPolicy": {"idleAfterSeconds": 60},
"wakePolicy": {"kafkaLag": {"brokers": "redpanda.cloud.svc.cluster.local:9093", "topics": ["cdn.edge"],
                            "groupId": "playhead-edges", "lagThreshold": 100, "pollIntervalSeconds": 5}}
```

Through the half hour, with its upgrade, the Worker kill and the failover, the job
never goes a minute without a record. Once the tape is over and checked,
[`scripts/park.sh`](scripts/park.sh) runs the scene:

1. A minute after its last record, the operator parks the job: it records where
   `cdn.edge` ends, ends the job at a savepoint that cancels it, and frees its
   slots. The window for minute 34 is still open, its state waiting in the
   savepoint.
2. The half hour's edge requests are still on `cdn.edge`, and clink's Kafka source
   commits no offsets to its group, so the operator counts the backlog from where
   the topic ended when the job parked: none, and the job stays parked.
3. The CDN delivers its next six minutes of edge logs in one batch
   ([`tools/batch.py`](tools/batch.py)), starting ten seconds before minute 34
   ends, each edge location's logs on a partition of its own. The backlog wakes
   the job, restored from the savepoint it parked at.
4. The job writes minutes 34 to 39, and parks again a minute after the batch.

`tools/check.py --scale-to-zero` holds the scene to what the operator and the
Coordinator recorded, and the windows to the tape and the batch: minute 34 must
hold the requests from before the park and after it in one row per edge
location, and every edge-location minute in the bucket must be there once. The
scene in a run with clink at `e6210048`:

```text
  scale to zero: edges parked 75 s after the tape ended, the job the upgrade started, job 5, at its savepoint /var/lib/clink/checkpoints/edges/_jobs/5@111  OK
  scale to zero: parked, it held no slots: 9 of 20 in use, by alerts,ledger,qoe; 12 once it ran again  OK
  scale to zero: the half hour's 269520 edge requests on cdn.edge kept it parked: parked (idle); lag 0 < threshold 100  OK
  scale to zero: the batch of 8015 woke it 5 s after it was delivered, as job 6, restored from /var/lib/clink/checkpoints/edges/_jobs/5@111  OK
  scale to zero: and it parked again 66 s after the batch, job 6 at savepoint /var/lib/clink/checkpoints/edges/_jobs/6@143  OK
  gs://cdn-vendor/pop-minute, minutes 33 to 39 (GCS, exactly once): 42 windows for 42 expected, 42 as the tape says, 0 repeated, 0 unexpected  OK
  scale to zero: minute 34, open when the job parked, holds requests from both sides of the park: ams 1229+19=1248, fra 1296+20=1316, iad 986+17=1003, lhr 1367+20=1387, nrt 1250+21=1271, sfo 1424+21=1445  OK
  scale to zero: every one of the bucket's 240 edge-location minutes once  OK
check: PASS
```

Step 2 is a fix this scene turned up in clink's operator. It measured a parked
job's backlog against the group's committed offsets and counted a partition the
group never committed from its start, and a clink job commits none, so the job
woke at the first poll for everything it had already read, then went idle and
parked again, over and over. The operator now records where the topics end
before it takes the park's savepoint, and counts from there.

## What it costs

[`tools/footprint.py`](tools/footprint.py) samples every five seconds from the
start of the tape to the end of the checks: each clink pod's CPU time and working
set, from the container runtime inside the kind node, and the memory of the node
and of the cloud's containers, from `docker stats`. The same run:

```text
footprint: 30 samples over 147 s of the tape playing
  clink-coordinator-0        0.8 CPU s     120 MiB at its peak
  clink-coordinator-1        0.7 CPU s     122 MiB at its peak
  clink-operator             0.3 CPU s      16 MiB at its peak
  clink-worker-0             6.5 CPU s     399 MiB at its peak
  clink-worker-1             2.7 CPU s     242 MiB at its peak
  clink: 10.9 CPU s (0.07 cores on average), 768 MiB together at the peak
  the kind node, Kubernetes and every pod: 2138 MiB at the peak
  the cloud's 11 containers: 1725 MiB together at the peak
  the node and the cloud: 3858 MiB together at the peak
```

The four jobs carried the tape's 305,943 records through an upgrade, a Worker
kill and a failover, checkpointing every two seconds, on 11 CPU-seconds and
under 800 MiB between clink's five pods. The cloud beside them takes more than
twice that memory, and the whole run peaks below 4 GiB, which leaves room to
spare in the 8 GB suggested above.

## Layout

| Path | What it is |
| --- | --- |
| `pipeline/` | the four jobs, and the edges job's scale-to-zero policy |
| `schemas/` | the playback events' Avro schemas, v1 and v2, and the edge request's Protobuf |
| `k8s/` | the kind cluster, the `ClinkCluster`, and the chart's values |
| `cloud/` | the cloud's containers, the collector's pipeline and Prometheus's scrape |
| `scripts/` | naming the cloud in the cluster, provisioning it, the Secrets, a job's `ClinkJob`, a CLI submission, the leader kill, the scale-to-zero scene |
| `tools/` | the tape, the feed, the CDN's later batch, the checks and the footprint |

`data/` (the tape) and `state/` (keys, certificates, logs) are made by a run and
not tracked.

## Licence

Apache 2.0, see [LICENSE](LICENSE).
