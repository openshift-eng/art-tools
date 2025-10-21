#!/usr/bin/env python3
"""
BigQuery MCP Server for ART Data

This MCP server provides tools for querying BigQuery tables containing ART (Automated Release Tooling Team) build data.
It allows AI assistants to retrieve and analyze build information, package data, and pipeline metrics.
"""

import logging
from typing import List

from google.auth import default
from google.cloud import bigquery
from mcp.server import FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bigquery-mcp-server")

# Default configuration
DEFAULT_PROJECT_ID = "openshift-art"
DEFAULT_DATASET_ID = "events"
DEFAULT_TABLE_ID = "builds"

# Initialize FastMCP server
mcp = FastMCP("bigquery-art-server")

# Default configuration
project_id = DEFAULT_PROJECT_ID
dataset_id = DEFAULT_DATASET_ID
table_id = DEFAULT_TABLE_ID
client = None

# Table schemas for validation
builds_schema_fields = {
    'name',
    'group',
    'version',
    'release',
    'assembly',
    'el_target',
    'arches',
    'installed_packages',
    'parent_images',
    'source_repo',
    'commitish',
    'rebase_repo_url',
    'rebase_commitish',
    'embargoed',
    'start_time',
    'end_time',
    'artifact_type',
    'engine',
    'image_pullspec',
    'image_tag',
    'outcome',
    'art_job_url',
    'build_pipeline_url',
    'pipeline_commit',
    'schema_level',
    'ingestion_time',
    'record_id',
    'build_id',
    'nvr',
    'rmetic',
    'installed_rpms',
    'build_component',
    'build_priority',
}

bundles_schema_fields = {
    'name',
    'group',
    'version',
    'release',
    'assembly',
    'source_repo',
    'commitish',
    'rebase_repo_url',
    'rebase_commitish',
    'start_time',
    'end_time',
    'engine',
    'image_pullspec',
    'image_tag',
    'outcome',
    'art_job_url',
    'build_pipeline_url',
    'pipeline_commit',
    'schema_level',
    'ingestion_time',
    'operand_nvrs',
    'operator_nvr',
    'record_id',
    'build_id',
    'nvr',
    'bundle_package_name',
    'bundle_csv_name',
    'build_component',
    'build_priority',
}

fbcs_schema_fields = {
    'name',
    'group',
    'version',
    'release',
    'assembly',
    'source_repo',
    'commitish',
    'rebase_repo_url',
    'rebase_commitish',
    'start_time',
    'end_time',
    'engine',
    'image_pullspec',
    'image_tag',
    'outcome',
    'art_job_url',
    'build_pipeline_url',
    'pipeline_commit',
    'schema_level',
    'ingestion_time',
    'bundle_nvrs',
    'arches',
    'record_id',
    'build_id',
    'nvr',
    'build_component',
    'build_priority',
}

taskruns_schema_fields = {
    'creation_time',
    'build_id',
    'task',
    'task_run',
    'task_run_uid',
    'pipeline_run',
    'pipeline_run_uid',
    'pod_name',
    'pod_phase',
    'scheduled_time',
    'initialized_time',
    'start_time',
    'containers',
    'max_finished_time',
    'success',
    'record_id',
}

# Table schema mapping
table_schemas = {
    'builds': builds_schema_fields,
    'bundles': bundles_schema_fields,
    'fbcs': fbcs_schema_fields,
    'taskruns': taskruns_schema_fields,
}


async def initialize_client():
    """Initialize BigQuery client with authentication."""
    global client, project_id
    try:
        credentials, project = default()
        client = bigquery.Client(credentials=credentials, project=project_id)
        logger.info(f"Initialized BigQuery client for project: {project_id}")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {str(e)}")
        raise


def validate_fields(fields: List[str], table_name: str = "builds") -> List[str]:
    """Validate and sanitize field names for a specific table."""
    schema = table_schemas.get(table_name, builds_schema_fields)
    valid_fields = []
    for field in fields:
        if field in schema:
            valid_fields.append(field)
        else:
            logger.warning(f"Invalid field name for {table_name}: {field}")

    # Default fields based on table type
    default_fields = {
        'builds': ["name", "outcome", "start_time"],
        'bundles': ["name", "outcome", "start_time"],
        'fbcs': ["name", "outcome", "start_time"],
        'taskruns': ["build_id", "task", "start_time"],
    }

    return valid_fields if valid_fields else default_fields.get(table_name, ["build_id", "start_time"])


def format_query_results(results: List[dict]) -> str:
    """Format query results as a readable table."""
    if not results:
        return "No results found."

    # Get column headers
    headers = list(results[0].keys())

    # Calculate column widths
    col_widths = {}
    for header in headers:
        col_widths[header] = max(len(str(header)), max(len(str(row.get(header, ""))) for row in results))

    # Format table
    lines = []

    # Header row
    header_row = " | ".join(str(header).ljust(col_widths[header]) for header in headers)
    lines.append(header_row)
    lines.append("-" * len(header_row))

    # Data rows
    for row in results:
        data_row = " | ".join(str(row.get(header, "")).ljust(col_widths[header]) for header in headers)
        lines.append(data_row)

    return "\n".join(lines)


@mcp.tool()
async def query_table(
    table_name: str,
    where_clause: str = "",
    select_fields: List[str] = [],
    limit: int = 100,
    order_by: str = "start_time DESC",
) -> str:
    """Query any ART table (builds, bundles, fbcs, taskruns). Returns data with automatic time partitioning for performance."""
    global client

    if not client:
        await initialize_client()

    if table_name not in table_schemas:
        raise Exception(f"Invalid table name. Must be one of: {', '.join(table_schemas.keys())}")

    # Use default fields if none provided
    if not select_fields:
        select_fields = list(table_schemas[table_name])[:5]  # First 5 fields as default

    select_fields = validate_fields(select_fields, table_name)
    limit = min(limit, 1000)

    # Build query with time filter for partition elimination (except taskruns which uses creation_time)
    fields_str = ", ".join(select_fields)
    table_ref = f"`{project_id}.{dataset_id}.{table_name}`"

    time_field = "creation_time" if table_name == "taskruns" else "start_time"
    partition_filter = f"{time_field} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)"

    query = f"SELECT {fields_str} FROM {table_ref} WHERE {partition_filter}"
    if where_clause:
        query += f" AND ({where_clause})"
    if order_by:
        query += f" ORDER BY {order_by}"
    query += f" LIMIT {limit}"

    logger.info(f"Executing {table_name} query: {query}")

    try:
        query_job = client.query(query)
        results = [dict(row) for row in query_job]

        formatted_results = format_query_results(results)
        return f"Found {len(results)} {table_name} records matching criteria.\n\n{formatted_results}"
    except Exception as e:
        raise Exception(f"{table_name} query execution failed: {str(e)}")


@mcp.tool()
async def get_build_summary(time_range_days: int = 30, group_by: str = "outcome") -> str:
    """Get summary statistics about ART builds including success rates, common failures, and timing metrics."""
    global client

    if not client:
        await initialize_client()

    table_ref = f"`{project_id}.{dataset_id}.{table_id}`"

    query = f"""
    SELECT 
        {group_by},
        COUNT(*) as build_count,
        AVG(TIMESTAMP_DIFF(end_time, start_time, MINUTE)) as avg_duration_minutes,
        MIN(start_time) as earliest_build,
        MAX(start_time) as latest_build
    FROM {table_ref}
    WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_range_days} DAY)
    GROUP BY {group_by}
    ORDER BY build_count DESC
    """

    try:
        query_job = client.query(query)
        results = [dict(row) for row in query_job]

        formatted_results = format_query_results(results)
        return f"Build summary for the last {time_range_days} days (grouped by {group_by}):\n\n{formatted_results}"
    except Exception as e:
        raise Exception(f"Summary query failed: {str(e)}")


@mcp.tool()
async def search_by_package(package_name: str, search_rpms: bool = True, limit: int = 50) -> str:
    """Search for builds containing specific packages in installed_packages or installed_rpms fields."""
    where_conditions = [f"installed_packages LIKE '%{package_name}%'"]
    if search_rpms:
        where_conditions.append(f"installed_rpms LIKE '%{package_name}%'")

    where_clause = " OR ".join(where_conditions)

    return await query_table(
        table_name="builds",
        where_clause=where_clause,
        select_fields=["name", "outcome", "start_time", "build_id", "installed_packages", "installed_rpms"],
        limit=min(limit, 200),
        order_by="start_time DESC",
    )


@mcp.tool()
async def get_failed_builds(days_back: int = 7, failure_type: str = "FAILED", limit: int = 25) -> str:
    """Get details about failed builds for debugging and analysis."""
    where_clause = (
        f"outcome = '{failure_type}' AND start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)"
    )

    return await query_table(
        table_name="builds",
        where_clause=where_clause,
        select_fields=["name", "outcome", "start_time", "end_time", "build_id", "art_job_url", "build_pipeline_url"],
        limit=min(limit, 100),
        order_by="start_time DESC",
    )


@mcp.tool()
async def search_by_field(
    table_name: str, field_name: str, search_value: str, additional_filters: str = "", limit: int = 50
) -> str:
    """Search any table by a specific field value with optional additional filters."""
    if table_name not in table_schemas:
        raise Exception(f"Invalid table name. Must be one of: {', '.join(table_schemas.keys())}")

    where_clause = f"{field_name} LIKE '%{search_value}%'"
    if additional_filters:
        where_clause += f" AND ({additional_filters})"

    return await query_table(table_name=table_name, where_clause=where_clause, limit=min(limit, 200))


@mcp.tool()
async def get_build_pipeline(build_id: str, include_taskruns: bool = True) -> str:
    """Get the complete build pipeline for a build_id: operator build -> bundles -> FBCs -> task runs."""
    global client

    if not client:
        await initialize_client()

    results = {}

    try:
        # Get the main build
        build_query = f"""
        SELECT name, build_id, outcome, start_time, end_time, nvr, artifact_type
        FROM `{project_id}.{dataset_id}.builds`
        WHERE build_id = '{build_id}'
        """

        build_job = client.query(build_query)
        build_results = [dict(row) for row in build_job]
        results["main_build"] = build_results

        if not build_results:
            return f"No build found with build_id: {build_id}"

        # Get related bundles
        bundles_query = f"""
        SELECT name, build_id, outcome, start_time, nvr, operator_nvr, bundle_package_name
        FROM `{project_id}.{dataset_id}.bundles`
        WHERE build_id = '{build_id}'
        """

        bundles_job = client.query(bundles_query)
        bundles_results = [dict(row) for row in bundles_job]
        results["bundles"] = bundles_results

        # Get related FBCs
        fbcs_query = f"""
        SELECT name, build_id, outcome, start_time, nvr, bundle_nvrs, arches
        FROM `{project_id}.{dataset_id}.fbcs`
        WHERE build_id = '{build_id}'
        """

        fbcs_job = client.query(fbcs_query)
        fbcs_results = [dict(row) for row in fbcs_job]
        results["fbcs"] = fbcs_results

        # Get task runs if requested
        if include_taskruns:
            taskruns_query = f"""
            SELECT task, task_run, start_time, max_finished_time, success, pod_phase
            FROM `{project_id}.{dataset_id}.taskruns`
            WHERE build_id = '{build_id}'
            ORDER BY start_time
            LIMIT 100
            """

            taskruns_job = client.query(taskruns_query)
            taskruns_results = [dict(row) for row in taskruns_job]
            results["taskruns"] = taskruns_results

        # Format the results
        output_lines = [f"Build Pipeline for build_id: {build_id}\n"]

        # Main build
        output_lines.append("=== MAIN BUILD ===")
        if results["main_build"]:
            output_lines.append(format_query_results(results["main_build"]))
        else:
            output_lines.append("No main build found")

        # Bundles
        output_lines.append(f"\n=== BUNDLES ({len(results['bundles'])}) ===")
        if results["bundles"]:
            output_lines.append(format_query_results(results["bundles"]))
        else:
            output_lines.append("No bundles found")

        # FBCs
        output_lines.append(f"\n=== FBCs ({len(results['fbcs'])}) ===")
        if results["fbcs"]:
            output_lines.append(format_query_results(results["fbcs"]))
        else:
            output_lines.append("No FBCs found")

        # Task runs
        if include_taskruns:
            output_lines.append(f"\n=== TASK RUNS ({len(results['taskruns'])}) ===")
            if results["taskruns"]:
                output_lines.append(format_query_results(results["taskruns"]))
            else:
                output_lines.append("No task runs found")

        return "\n".join(output_lines)

    except Exception as e:
        raise Exception(f"Build pipeline query failed: {str(e)}")


@mcp.tool()
async def search_related_builds(operator_nvr: str = "", bundle_nvr: str = "", search_depth: str = "full") -> str:
    """Search for related builds across the pipeline by operator or bundle NVR. search_depth: 'builds', 'bundles', 'fbcs', or 'full'."""
    global client

    if not client:
        await initialize_client()

    if not operator_nvr and not bundle_nvr:
        raise Exception("Either operator_nvr or bundle_nvr must be provided")

    try:
        results = {}
        build_ids = set()

        # Start with builds table
        if operator_nvr:
            builds_query = f"""
            SELECT name, build_id, outcome, start_time, nvr, artifact_type
            FROM `{project_id}.{dataset_id}.builds`
            WHERE nvr LIKE '%{operator_nvr}%'
            AND start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
            LIMIT 50
            """
        else:
            # Search bundles first, then get related builds
            bundles_query = f"""
            SELECT DISTINCT build_id
            FROM `{project_id}.{dataset_id}.bundles`
            WHERE operator_nvr LIKE '%{bundle_nvr}%' OR nvr LIKE '%{bundle_nvr}%'
            AND start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
            """

            bundles_job = client.query(bundles_query)
            bundle_build_ids = [row.build_id for row in bundles_job]

            if not bundle_build_ids:
                return f"No builds found related to bundle_nvr: {bundle_nvr}"

            build_ids_str = "', '".join(bundle_build_ids)
            builds_query = f"""
            SELECT name, build_id, outcome, start_time, nvr, artifact_type
            FROM `{project_id}.{dataset_id}.builds`
            WHERE build_id IN ('{build_ids_str}')
            """

        # Get builds
        builds_job = client.query(builds_query)
        builds_results = [dict(row) for row in builds_job]
        results["builds"] = builds_results

        # Collect build_ids
        for build in builds_results:
            build_ids.add(build["build_id"])

        if not build_ids or search_depth == "builds":
            output_lines = [f"Related Builds (search_depth: {search_depth})\n"]
            output_lines.append("=== BUILDS ===")
            output_lines.append(format_query_results(builds_results))
            return "\n".join(output_lines)

        # Get bundles if requested
        if search_depth in ["bundles", "fbcs", "full"]:
            build_ids_str = "', '".join(build_ids)
            bundles_query = f"""
            SELECT name, build_id, outcome, start_time, nvr, operator_nvr, bundle_package_name
            FROM `{project_id}.{dataset_id}.bundles`
            WHERE build_id IN ('{build_ids_str}')
            """

            bundles_job = client.query(bundles_query)
            bundles_results = [dict(row) for row in bundles_job]
            results["bundles"] = bundles_results

        # Get FBCs if requested
        if search_depth in ["fbcs", "full"]:
            fbcs_query = f"""
            SELECT name, build_id, outcome, start_time, nvr, bundle_nvrs, arches
            FROM `{project_id}.{dataset_id}.fbcs`
            WHERE build_id IN ('{build_ids_str}')
            """

            fbcs_job = client.query(fbcs_query)
            fbcs_results = [dict(row) for row in fbcs_job]
            results["fbcs"] = fbcs_results

        # Format output
        search_term = f"operator_nvr '{operator_nvr}'" if operator_nvr else f"bundle_nvr '{bundle_nvr}'"
        output_lines = [f"Related Builds for {search_term} (search_depth: {search_depth})\n"]

        output_lines.append(f"=== BUILDS ({len(results['builds'])}) ===")
        output_lines.append(format_query_results(results["builds"]))

        if "bundles" in results:
            output_lines.append(f"\n=== BUNDLES ({len(results['bundles'])}) ===")
            if results["bundles"]:
                output_lines.append(format_query_results(results["bundles"]))
            else:
                output_lines.append("No bundles found")

        if "fbcs" in results:
            output_lines.append(f"\n=== FBCs ({len(results['fbcs'])}) ===")
            if results["fbcs"]:
                output_lines.append(format_query_results(results["fbcs"]))
            else:
                output_lines.append("No FBCs found")

        return "\n".join(output_lines)

    except Exception as e:
        raise Exception(f"Related builds search failed: {str(e)}")


def main():
    """Main entry point for the MCP server."""
    import os

    # Get configuration from environment variables
    global project_id, dataset_id, table_id
    project_id = os.getenv("BIGQUERY_PROJECT_ID", DEFAULT_PROJECT_ID)
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", DEFAULT_DATASET_ID)
    table_id = os.getenv("BIGQUERY_TABLE_ID", DEFAULT_TABLE_ID)

    # Run the server using stdio transport
    logger.info("Starting BigQuery MCP Server with stdio transport")
    mcp.run()


if __name__ == "__main__":
    main()
