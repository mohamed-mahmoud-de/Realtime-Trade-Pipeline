---
name: architecture
description: Draw the current state of the streaming pipeline architecture in ASCII. Shows what is built, what is in progress, and what is not yet built. Use at the start of any session or when adding a new component.
---

Draw the current state of the streaming pipeline architecture.

Use this format:

CURRENT ARCHITECTURE
====================

[Data Source]
     |
     | (Python producer)
     v
[Kafka Broker]
  Topic: <topic-name>
  Partitions: <number>
     |
     | (Spark Structured Streaming)
     v
[Spark Processing]
  Window: <if applicable>
  Transformations: <what we do to the data>
     |
     +---> [Cassandra] (low-latency reads)
     |       Keyspace: <name>
     |       Table: <name>
     |
     +---> [Elasticsearch] (search and analytics)
             Index: <name>

LEGEND:
[X] = Built and working
[X*] = Built but not fully tested
[ ] = Not yet built

Then answer:
1. What is the weakest part of the current architecture?
2. What would break first under high load?
3. What is missing before this is production-ready?

Keep it honest. If something is not built yet, say so.
Do not draw components that do not exist yet.
