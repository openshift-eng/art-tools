# ART MCP Server

Model Context Protocol (MCP) server for querying BigQuery tables containing ART (Automated Release Tooling Team) build data.

## Features

- **query_art_builds**: Execute custom queries with filtering, field selection, and ordering
- **get_build_summary**: Get aggregated statistics and metrics about builds  
- **search_builds_by_package**: Find builds containing specific packages
- **get_failed_builds**: Analyze failed builds for debugging

## Setup

1. Install dependencies:
```bash
pip install -e .
```

2. Configure BigQuery access:
```bash
# Set up authentication
gcloud auth application-default login

# Copy and edit environment file
cp .env.example .env
# Edit .env with your BigQuery project/dataset/table details
```

3. Run the server:
```bash
python tools/bigquery_mcp_server.py
```

## Configuration

Set these environment variables:
- `BIGQUERY_PROJECT_ID`: Your GCP project ID
- `BIGQUERY_DATASET_ID`: BigQuery dataset name
- `BIGQUERY_TABLE_ID`: BigQuery table name

## Schema

The server expects a table with these fields:
- `name`, `group`, `version`, `release`, `assembly` (STRING)
- `el_target`, `arches`, `installed_packages` (STRING) 
- `parent_images`, `source_repo`, `commitish` (STRING)
- `rebase_repo_url`, `rebase_commitish` (STRING)
- `embargoed`, `rmetic` (BOOLEAN)
- `start_time`, `end_time`, `ingestion_time` (TIMESTAMP)
- `artifact_type`, `engine`, `image_pullspec` (STRING)
- `image_tag`, `outcome`, `art_job_url` (STRING)
- `build_pipeline_url`, `pipeline_commit` (STRING)
- `schema_level`, `build_priority` (INTEGER)
- `record_id`, `build_id`, `nvr` (STRING)
- `installed_rpms`, `build_component` (STRING)

## Usage with Claude/Gemini

Configure the MCP server in your AI tool's settings to enable BigQuery data access for analysis and reporting.
