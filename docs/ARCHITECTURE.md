# Architecture and operational flow

```mermaid
flowchart TD
    A[Orders CSV] --> B[Bronze Delta: raw + lineage]
    C[Customers CSV] --> D[Customers Delta]
    E[Products CSV] --> F[Products Delta]
    B --> G[Silver: cast, standardize, validate]
    G -->|valid| H[Deduplicate + derive amounts]
    G -->|invalid| I[Quarantine Delta + reason]
    H --> J[Join customer dimension]
    D --> J
    J --> K[Join product dimension]
    F --> K
    K --> L[Gold daily sales]
    K --> M[Gold category performance]
    K --> N[Gold customer lifetime value]
    H --> O[Data quality checks]
    I --> O
    K --> O
    O --> P[quality_report.json]
```

## Layer contracts

| Layer | Contract | Consumers |
|---|---|---|
| Bronze | Source-aligned strings, immutable lineage fields | Replay/debugging |
| Silver | Typed, validated, deduplicated order facts | Gold transformations |
| Quarantine | Rejected source rows and reason codes | Data stewards / remediation |
| Gold | Business aggregates and dimensional context | BI / analytics |

## Production Azure mapping

- Landing: ADLS Gen2 raw container
- Ingestion/orchestration: Azure Data Factory
- Processing: Azure Databricks (PySpark + Delta Lake)
- Curated storage: ADLS Gen2 Delta tables
- Governance: Unity Catalog / managed identity / Key Vault
- Consumption: Synapse, Databricks SQL, or Power BI
