---
name: quiz
description: Knowledge check on streaming concepts covered so far. Ask 4-5 questions including scenario-based and interview-style questions. Use after completing a session or learning a new component.
---

Quiz me on what I have learned so far in this project.

Ask 4-5 questions covering a mix of:

CONCEPT QUESTIONS (can I explain the tool):
- "Explain in one sentence why Kafka retains messages after consumption"
- "What is a consumer group and why does it exist?"
- "Why do we write to Cassandra AND Elasticsearch instead of just one?"

SCENARIO QUESTIONS (can I reason about the system):
- "Our Spark consumer crashes mid-stream. What happens to the data?
   Will we lose events? Will we process any twice?"
- "We need to add a second team consuming the same events independently.
   What do we change in Kafka to make this work?"
- "Our Cassandra writes are getting slow. What would you check first?"

DESIGN QUESTIONS (can I make engineering decisions):
- "Why did we choose this data source for streaming over batch?"
- "What would break if we removed Kafka and had the producer write
   directly to Spark?"

Ask one question at a time. Wait for my answer before continuing.
After each answer: correct me if wrong, explain why, then continue.

At the end rate my understanding for each component:
Kafka: solid / shaky / needs review
Spark Streaming: solid / shaky / needs review
Cassandra: solid / shaky / needs review
Elasticsearch: solid / shaky / needs review

Tell me what to revisit before the next session.
