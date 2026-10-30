# Hands-on L10: Report

**Name:** Pablo Garces
**Student ID:** 801377963
**Email:** <pgarces@charlotte.edu>

---

## Seed and commands

Seed used for `datagen.py`: `801377963`

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
# Stopped the containers from the previous assignment first.
cd ../ITCS6190-A3-PabloG && docker compose down
cd ../ITCS6190-H10-PabloG

python3 datagen.py 801377963

docker compose up -d

docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared

# Ran task1 again without deleting the checkpoint for question 3.
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared update

cat shared-folder/output/task1/*.csv | grep -vc trip_id
cat shared-folder/output/task2/*.csv | grep -v window_start | sort
ls shared-folder/checkpoint/task1
ls shared-folder/checkpoint/task2

docker compose down
```

---

## Results

### Task 1: ingest and parse

Paste the micro-batch summary printed at the end of the run, and the number of rows in
`shared-folder/output/task1` (`cat shared-folder/output/task1/*.csv | grep -vc trip_id`).

```text
=== task1: micro-batch summary ===
batch input rows            watermark state rows
    0         29                    -          -
    1         29                    -          -
    2         30                    -          -
    3         21                    -          -
    4         29                    -          -
    5         21                    -          -
    6         27                    -          -
    7         22                    -          -
    8         22                    -          -
    9         20                    -          -
   10         22                    -          -
   11         21                    -          -

$ cat shared-folder/output/task1/*.csv | grep -vc trip_id
293
```

How many micro-batches ran, and why that number?

12 micro-batches ran because `datagen.py` wrote 12 files and `read_rides()` uses
`maxFilesPerTrigger = 1`, so each file is one batch. The input rows add up to 293, which is the
same number of events `datagen.py` printed and the same number of rows in the output. There is
no watermark and no state because task 1 does not aggregate anything; each row is just parsed
and written.

### Task 2: trips and fares per window

Paste the windows written by the stream (`cat shared-folder/output/task2/*.csv | grep -v
window_start | sort`), the summary, and the batch table that `main.py` prints at the end.

```text
$ cat shared-folder/output/task2/*.csv | grep -v window_start | sort
2026-10-27 07:58:00,2026-10-27 08:00:00,4,199.57
2026-10-27 08:00:00,2026-10-27 08:02:00,65,2468.61
2026-10-27 08:02:00,2026-10-27 08:04:00,49,2120.14
2026-10-27 08:04:00,2026-10-27 08:06:00,48,2071.39
2026-10-27 08:06:00,2026-10-27 08:08:00,45,1909.71

=== task2: micro-batch summary ===
batch input rows            watermark state rows
    0         29                    -          2
    1         29  2026-10-27 07:58:57          2
    2         30  2026-10-27 07:59:50          3
    3         21  2026-10-27 08:00:48          2
    4         29  2026-10-27 08:01:36          3
    5         21  2026-10-27 08:02:54          2
    6         27  2026-10-27 08:03:57          3
    7         22  2026-10-27 08:04:52          2
    8         22  2026-10-27 08:05:54          3
    9         20  2026-10-27 08:06:49          2
   10         22  2026-10-27 08:07:47          3
   11         21  2026-10-27 08:08:46          2
   12          0  2026-10-27 08:09:54          2

=== task2: the same windows computed in batch over all files ===
+-------------------+-------------------+-----+----------+
|window_start       |window_end         |trips|total_fare|
+-------------------+-------------------+-----+----------+
|2026-10-27 07:58:00|2026-10-27 08:00:00|5    |245.28    |
|2026-10-27 08:00:00|2026-10-27 08:02:00|66   |2547.76   |
|2026-10-27 08:02:00|2026-10-27 08:04:00|53   |2292.86   |
|2026-10-27 08:04:00|2026-10-27 08:06:00|48   |2071.39   |
|2026-10-27 08:06:00|2026-10-27 08:08:00|45   |1909.71   |
|2026-10-27 08:08:00|2026-10-27 08:10:00|44   |1877.3    |
|2026-10-27 08:10:00|2026-10-27 08:12:00|32   |1311.56   |
+-------------------+-------------------+-----+----------+
```

---

## Questions

**1. Append versus update.** Run task 2 a second time with `update` (it prints to the
console). In which batch does each window first appear in update mode, and in which batch
is it written in append mode? Explain the difference with the watermark values from the
summary.

In update mode, 07:58–08:00 and 08:00–08:02 first appear in batch 0, 08:02–08:04 in batch 2,
08:04–08:06 in batch 4, 08:06–08:08 in batch 6, 08:08–08:10 in batch 8 and 08:10–08:12 in
batch 10. In append mode, 07:58–08:00 is written in batch 3, 08:00–08:02 in batch 5,
08:02–08:04 in batch 7, 08:04–08:06 in batch 9 and 08:06–08:08 in batch 11. The last two
windows, 08:08–08:10 and 08:10–08:12, are never written in append mode.

In update mode a window is printed as soon as it gets its first trip, and then printed again
every time its count changes. For example, 08:00–08:02 was printed in batches 0, 1, 2 and 3
with 25, 54, 64 and 65 trips. In append mode a window is only written once, when Spark is sure
it will not change anymore, and that is when the watermark passes the end of the window. The
watermark is the latest `event_time` seen minus 2 minutes. For example, 07:58–08:00 ends at
08:00, and the first batch with a watermark after 08:00 is batch 3, where the watermark was
08:00:48, so that is when append writes it. In the same way, 08:00–08:02 is written in batch 5
and 08:06–08:08 in batch 11.

**2. Where did the rows go?** Compare the stream's windows with the batch table. Which
windows have fewer trips in the stream, by how many, and why? Which windows are missing
from the stream's output altogether, and what would make Spark write them? (Look at
`datagen.py` for how the late events were made.)

Three windows have fewer trips in the stream than in the batch table: 07:58–08:00 has 4
instead of 5, 08:00–08:02 has 65 instead of 66, and 08:02–08:04 has 49 instead of 53. That is
6 trips in total, which is exactly the number of "very late" events `datagen.py` made. From
file 9 on, some events get an `event_time` 8 to 10 minutes earlier than when they arrive. When
they arrive, the watermark is already around 08:06, so the windows they belong to are already
closed and Spark drops them. The 13 events that were only 1 to 2 minutes late are still inside
the 2-minute watermark, so none of them were lost. Update mode lost the same 6 trips, since the
watermark applies to both modes.

Two windows are missing from the stream's output: 08:08–08:10 and 08:10–08:12. The last
watermark is 08:09:54, which is before the end of both windows, so Spark never closed them and
append never wrote them. To write them, the stream would need new events that move the
watermark past their end. An event at 08:12 or later would close 08:08–08:10, and one at 08:14
or later would close 08:10–08:12.

**3. Checkpoint.** Run task 1 again without deleting `shared-folder/checkpoint/task1`. What
happens, and why? List the subfolders of the checkpoint and say in one line what each one
holds.

```text
=== task1: micro-batch summary ===
batch input rows            watermark state rows
   12          0                    -          -
```

The second run only had one batch, batch 12, with 0 input rows, and nothing new was written.
This is because the checkpoint already says that batches 0 to 11 were processed and which files
they read, so Spark restarts where it stopped and finds no new files. So the rows are not
written again.

```text
$ ls shared-folder/checkpoint/task1
commits  metadata  offsets  sources

$ ls shared-folder/checkpoint/task2
commits  metadata  offsets  sources  state
```

- `offsets/`: one file per batch with what that batch was going to read.
- `commits/`: one file per batch that finished, so Spark knows which batches are complete.
- `sources/`: the list of already processed input files.
- `metadata`: the id of the query, so a restart knows it is the same query.

The checkpoint of task 2 also has a `state/` folder, with the open windows and their counts,
which task 1 does not need because it does not aggregate.

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

Nothing went wrong this time. Like in the last assignment, before starting this cluster I
stopped the containers from the previous activity with `docker compose down`, so
`docker compose up -d` worked on the first try. Both tasks ran correctly, and the stream and
the batch table match except for the late events explained in question 2.
