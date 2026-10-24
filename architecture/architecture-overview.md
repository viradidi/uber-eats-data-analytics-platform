# Uber Eats Data Platform — Architecture Overview

## Business purpose

Bring operational data from different source systems into a structured data workflow so it can be prepared for consistent analytics and reporting.

## Logical architecture

```mermaid
flowchart TB
    subgraph SOURCES["1. Operational Data Sources"]
        API["REST API<br/>Payments, deliveries,<br/>ratings and order status"]
        CSV["CSV Files<br/>Drivers, promotions<br/>and delivery zones"]
        SQL["Relational Source<br/>Incremental extraction"]
    end

    subgraph INGESTION["2. Ingestion and Orchestration"]
        ADF["Azure Data Factory<br/>Pipelines and orchestration"]
        APIING["API ingestion"]
        FILEING["File ingestion"]
        SQLING["SQL incremental ingestion"]
    end

    subgraph STORAGE["3. Cloud Data Storage"]
        LAKE["Azure Data Lake Storage<br/>Landing and data storage"]
    end

    subgraph TRANSFORM["4. Transformation"]
        DBX["Azure Databricks<br/>Notebook-based processing"]
        SILVER["Silver Layer<br/>Cleansed and structured data"]
    end

    subgraph CONSUMPTION["5. Analytics Consumption"]
        ANALYTICS["Analytics-ready datasets"]
        REPORTS["Reporting and<br/>business analysis"]
    end

    API --> APIING
    CSV --> FILEING
    SQL --> SQLING

    APIING --> ADF
    FILEING --> ADF
    SQLING --> ADF

    ADF --> LAKE
    LAKE --> DBX
    DBX --> SILVER
    SILVER --> ANALYTICS
    ANALYTICS --> REPORTS

    classDef source fill:#e8f1ff,stroke:#3973ac,color:#142d4e
    classDef orchestration fill:#e9e7ff,stroke:#7164c8,color:#282052
    classDef storage fill:#e1f5ee,stroke:#24866b,color:#124d3d
    classDef transformation fill:#fff0dc,stroke:#c78227,color:#68400d
    classDef consumption fill:#f0e8f9,stroke:#8b62ad,color:#44275b

    class API,CSV,SQL source
    class ADF,APIING,FILEING,SQLING orchestration
    class LAKE storage
    class DBX,SILVER transformation
    class ANALYTICS,REPORTS consumption
```

## How to read the architecture

1. **Sources:** Operational records arrive through APIs, files, and relational data sources.
2. **Ingestion:** Azure Data Factory organizes the configured ingestion workflows.
3. **Storage:** Azure Data Lake Storage provides the cloud storage layer.
4. **Transformation:** Databricks notebooks process data into a structured Silver layer.
5. **Consumption:** The resulting datasets provide a foundation for downstream analytics and reporting.

## Engineering considerations

- Incremental extraction must define reliable extraction boundaries.
- Reprocessing should avoid unintended duplicate records.
- Data-quality checks and reconciliation help establish output correctness.
- Failures need a documented recovery approach.
- Monitoring and security depend on the deployed configuration and available execution evidence.

## Architecture boundary

This diagram represents the **logical architecture** of the project, not proof that every component is deployed and connected in a live production environment. Refer to the repository's pipeline definitions, notebooks, and execution evidence for implementation details.