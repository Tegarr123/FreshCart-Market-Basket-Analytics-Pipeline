# FreshCart: Automated Market Basket Analytics Pipeline
FreshCart is a growing grocery retailer that needs to increase its Average Order Value (AOV). Currently, their marketing team creates bundles and discounts based on intuition rather than data, leading to low conversion rates on promotions.

## Problem Statement
1. **Analytical Complexity**: Standard Transactional (OLTP) databases store sales linearly. Identifying product affinities (e.g., "Customers who bought X also bought Y") requires expensive self-joins that can degrade production database performance.
2. **Stale Data**: Traditional Batch ETL processes load entire datasets weekly, causing a "data lag" that prevents the marketing team from reacting to fast-moving trends.
3. **High Infrastructure Costs**: Full-table reloads in a Cloud Data Warehouse like Snowflake are computationally expensive and inefficient for large-scale retail data.

## Objectives
1. **Implement Change Data Capture (CDC)**: Use dlthub to perform incremental loads from PostgreSQL to Snowflake, ensuring only new or updated transaction records are processed to minimize latency and cost.
2. **Architect a Role-Playing Dimensional Model**: Design and implement a Star Schema in Snowflake where a single Product Dimension "plays two roles" (Product A and Product B) to support affinity analysis without data redundancy.
3. **Automate Complex Transformations**: Leverage dbt (Data Build Tool) to engineer a POS_Market_Basket_Fact table, transforming raw line items into a paired-product matrix.
4. **Orchestrate Containerized Workflows**: Use Dockerized Airflow to manage the end-to-end dependency between data ingestion and transformation, ensuring the pipeline is portable and robust.
5. **Enable Data-Driven Decisions**: Provide a Looker dashboard that calculates key association metrics such as Support, Confidence, and Lift to guide retail bundling strategies.
