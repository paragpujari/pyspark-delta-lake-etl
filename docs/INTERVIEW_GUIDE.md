# Interview Guide — Retail Orders PySpark + Delta Lake

## 90-second project explanation

“I built a batch retail-orders lakehouse using PySpark and Delta Lake with Bronze, Silver and Gold layers. The pipeline reads raw CSV order data, stores it unchanged in Bronze with batch and file lineage, then applies schema casting, standardization, validation and deduplication in Silver. Invalid rows are not discarded; they are sent to a quarantine Delta table with explicit rejection reasons. I then enrich the trusted orders by joining customer and product dimensions and create Gold aggregates for daily sales, category performance and customer lifetime value. I partition the daily sales table by year and month, enable Spark adaptive query execution, and produce a quality report covering record counts, duplicates and unmatched dimensions. The sample is local and uses generated data; in production I would use cloud storage, orchestration, incremental Delta MERGE, monitoring and governance.”

## Questions you should be ready to answer

1. Why use a medallion architecture instead of one transformation table?
2. What belongs in Bronze, and why preserve raw values?
3. How do you handle schema drift and corrupt records?
4. Why cast types in Silver rather than infer them in Bronze?
5. How does the pipeline distinguish invalid rows from duplicates?
6. Why use a window function for deduplication?
7. What makes a deduplication rule deterministic?
8. What is the difference between `repartition` and `coalesce`?
9. What is a shuffle, and why is it expensive?
10. When is a broadcast join useful? What can go wrong?
11. What is data skew, and how would you diagnose it?
12. Why partition daily sales by year/month instead of customer ID?
13. What are small files, and how can they affect query performance?
14. Explain predicate pushdown and column pruning.
15. What does Spark AQE do?
16. What is the difference between cache and persist?
17. What does Delta Lake add over Parquet?
18. Explain Delta transaction log and ACID guarantees.
19. How would you implement incremental processing with Delta `MERGE`?
20. How would you make the pipeline idempotent after a failed run?
21. How would you implement a watermark for late-arriving data?
22. What is the difference between `OPTIMIZE`, `ZORDER` and `VACUUM` in Databricks?
23. How would you implement SCD Type 2 for customer dimensions?
24. How would you monitor freshness, volume and quality in production?
25. How would you secure data and secrets in Azure Databricks?

## Trade-offs and caveats

- The example uses overwrite writes to make the demo rerunnable. A production incremental pipeline should use Delta `MERGE`, a source watermark, and a retry-safe batch identifier.
- Broadcast joins are used because the generated dimension tables are tiny. For larger dimensions, inspect the physical plan and remove/adjust the hint.
- Year/month partitioning is demonstrated on a date-oriented fact table. Partition only when query patterns and data volume justify it; too many tiny partitions hurt performance.
- `OPTIMIZE` and `ZORDER` are Databricks capabilities, not portable commands for every local open-source Delta setup.
- The pipeline currently treats unmatched dimension keys as a quality-report metric; decide explicitly whether they should be quarantined or mapped to an Unknown member in a production contract.

## Suggested demo sequence

1. Show the source CSV and point out intentional dirty records and duplicate key.
2. Explain Bronze lineage columns.
3. Show Silver schema and derived amount columns.
4. Inspect quarantine and explain rejection reasons.
5. Explain dimension joins and why broadcast is appropriate for the tiny sample.
6. Show Gold outputs and partition folders.
7. Open `quality_report.json` and discuss the checks.
8. Explain how you would replace local storage with ADLS Gen2 and orchestrate with ADF/Databricks Workflows.
