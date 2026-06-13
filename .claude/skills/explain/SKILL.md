---
name: explain
description: Deep-dive a streaming concept before writing any code. Mandatory before touching any new tool or idea. Use for Kafka topics, partitions, offsets, consumer groups, Spark windowing, Cassandra data models, Elasticsearch indexing, or anything else that feels unclear.
---

I want to understand a concept deeply before writing any code.

Follow this exact sequence — do not skip steps:

1. ONE SENTENCE ELI5
   Explain it like I am five years old. One sentence only.

2. REAL EXPLANATION WITH ANALOGY
   The actual technical explanation. Include an analogy that
   connects to something I already know:
   - My experience with Airflow DAGs and tasks
   - PostgreSQL tables and queries
   - Docker containers and networking
   - My Discord bot experience (events, state, persistence)
   Do NOT say "it's just like X" — explain the similarities
   AND the important differences.

3. IN THIS PROJECT SPECIFICALLY
   How does this concept apply to what we are building?
   Where will I see it in our actual code?

4. THE FAILURE MODE
   What breaks if I misunderstand this concept?
   What is the most common mistake beginners make with it?

5. TEST ME
   Ask me to explain it back in my own words.
   Wait for my answer before moving on.
   If I get it wrong, correct me clearly and ask again.

Do NOT show any code during this explanation.
Do NOT move to the next step until I confirm I understand.
