# Databricks Certified Data Engineer Associate

<img align="left" role="left" src="https://img-c.udemycdn.com/course/240x135/4956262_2022_2.jpg" width="180" alt="Databricks Certified Data Engineer Associate - Preparation" />
This repository contains the resources of the preparation course for Databricks Data Engineer Associate certification exam on Udemy:
<br/>
<a href="https://www.udemy.com/course/databricks-certified-data-engineer-associate/?referralCode=F0FA48E9A0546C975F14" target="_blank">https://www.udemy.com/course/databricks-certified-data-engineer-associate/?referralCode=F0FA48E9A0546C975F14</a>.
<br/>
<br/>


## Practice Exams

<img align="left" role="left" src="https://img-c.udemycdn.com/course/240x135/5005556_5c54.jpg" width="180" alt="Practice Exams: Databricks Certified Data Engineer Associate" />
Practice exams for this certification are available in the following Udemy course:
<br/>
<a href="https://www.udemy.com/course/practice-exams-databricks-certified-data-engineer-associate/?referralCode=9AA679C03D1F51B2C956" target="_blank">https://www.udemy.com/course/practice-exams-databricks-certified-data-engineer-associate/?referralCode=9AA679C03D1F51B2C956</a>.<br/>

## Unity Catalog Serverless Compatibility Fixes

This repository was originally written for workspaces using the `hive_metastore` catalog with DBFS access. The following fixes have been applied to make the notebooks compatible with **Unity Catalog-only serverless** workspaces:

### 1. Catalog Replacement (hive_metastore -> workspace)

All `USE CATALOG hive_metastore` statements were replaced with `USE CATALOG workspace` across 14 notebooks. The `workspace` catalog uses Unity Catalog with a `default` schema, so unqualified table references resolve to `workspace.default.<table>`.

Notebooks fixed:
- 1.2 - Understanding Delta Tables
- 1.3 - Advanced Delta Lake Features
- 1.4 - Databases and Tables on Databricks
- 1.5A - Views
- 1.5B - Views (Session 2)
- 1.2L - Delta Lake (Lab)
- 1.3L - Databases and Tables on Databricks (Lab)
- 1.2L Solution - Delta Lake
- 1.3L Solution - Databases and Tables on Databricks
- 4.3L - Databricks SQL (Lab)
- 4.3L Solution - Databricks SQL
- Labs/Includes/Setup-Lab
- Labs/Solutions/Includes/Setup-Lab
- Includes/Copy-Datasets

### 2. %fs Commands Replaced with SQL Equivalents

On serverless compute with Unity Catalog, DBFS root is disabled and managed table storage is isolated. The following %fs commands were replaced with SQL equivalents:

| Original %fs command | SQL replacement | Purpose |
| --- | --- | --- |
| %fs ls 'dbfs:/.../table' | DESCRIBE EXTENDED table | Explore table directory / metadata |
| %fs ls 'dbfs:/.../table' (after UPDATE) | DESCRIBE DETAIL table | Show table storage details |
| %fs ls 'dbfs:/.../_delta_log' | DESCRIBE HISTORY table | Explore Delta transaction log |
| %fs head 'dbfs:/.../_delta_log/xxx.json' | SELECT * FROM (DESCRIBE HISTORY table) WHERE version = N | Read specific Delta log entry |

Notebooks with %fs fixes:
- 1.2 - Understanding Delta Tables (4 cells)
- 1.3 - Advanced Delta Lake Features (4 cells)
- 1.4 - Databases and Tables on Databricks (6 cells)
- 1.3L - Databases and Tables on Databricks (2 cells)
- 1.2L Solution - Delta Lake (2 cells)
- 1.3L Solution - Databases and Tables on Databricks (2 cells)

### 3. Inline hive_metastore References Replaced with workspace

Fully-qualified table references (e.g., hive_metastore.de_associate_school.students) and catalog variable defaults (e.g., catalog_name='hive_metastore') in Labs and Includes notebooks were updated to use workspace.

Notebooks fixed:
- 4.3L - Databricks SQL (2 SQL queries)
- 4.3L Solution - Databricks SQL (2 SQL queries)
- Labs/Includes/Setup-Lab (function default parameter)
- Labs/Solutions/Includes/Setup-Lab (function default parameter)
- Includes/Copy-Datasets (catalog variable)

### Notes

- The %fs ls '/databricks-datasets' command in 1.1 - Notebook Basics was left unchanged as it references built-in Databricks sample datasets, not UC managed storage.
- External table %fs ls commands referencing dbfs:/mnt/demo/... paths were replaced with DESCRIBE EXTENDED since DBFS mounts are not accessible on serverless compute.
