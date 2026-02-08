# README

## Repository description

Two main folders are made to handle the data: `ingestion_engine` and `metadata`; in a true 
production environment each one should be a repository by its own with the correct environment specified as well
as be called separately from Databricks.

## Metadata

Stores the schema of each input source and its output table as well as requirements.

Sub-folder indicate the type source, very broad since this is for an assignment.

## Ingestion engine

Make ingestion of different data sources to Delta Tables. In the future, tests and 
data cleaning can be added as well as feature engineering.


## Clarifications

- Passwords are stored at compute level or in Databricks/Azure vaults
- Local virtual env was created using Python 3.12
- No need for Azure Data Factory, but it can be used to orchestrate the pipelines and may be useful if ingesting a massive amount of files
