"""
Fixed Redshift database connection and configuration module using Data API
with proper transaction handling
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import boto3

logger = logging.getLogger(__name__)


class RedshiftConfig:
    """Redshift database configuration"""

    def __init__(self):
        self.host = os.getenv("REDSHIFT_ENDPOINT", "localhost")
        self.port = int(os.getenv("REDSHIFT_PORT", "5439"))
        self.database = os.getenv("REDSHIFT_DB", "book_sales")
        self.user = os.getenv("REDSHIFT_USER", "admin")
        self.password = os.getenv("REDSHIFT_PASSWORD", "")
        self.role_arn = os.getenv("REDSHIFT_ROLE_ARN", "")
        self.region = os.getenv("AWS_REGION", "us-east-2")
        self.cluster_identifier = os.getenv("REDSHIFT_CLUSTER", "")

    @property
    def connection_params(self) -> dict:
        """Get Redshift connection parameters"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "region": self.region,
        }

    @property
    def cluster_id_for_data_api(self) -> str:
        """Prefer explicit cluster identifier; fallback to parsing endpoint."""
        if self.cluster_identifier:
            return self.cluster_identifier
        # Fallback: extract cluster name from endpoint (e.g., <cluster>.<random>.redshift.amazonaws.com)
        try:
            return self.host.split(".")[0]
        except Exception:
            return self.host


class RedshiftManager:
    """Redshift connection manager using Data API with proper transaction handling"""

    def __init__(self, config: RedshiftConfig):
        self.config = config
        self.redshift_client = boto3.client("redshift-data", region_name=config.region)

    async def initialize(self):
        """Initialize Redshift Data API connection"""
        try:
            response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.config.cluster_id_for_data_api,
                Database=self.config.database,
                DbUser=self.config.user,
                Sql="SELECT 1",
            )

            await self._wait_for_completion(response["Id"])
            logger.info("Redshift Data API connection test successful")

        except Exception as e:
            logger.error(f"Failed to initialize Redshift Data API: {e}")
            raise

    async def close(self):
        """Close Redshift connection (no-op for Data API)"""
        logger.info("Redshift Data API connection closed")

    async def _wait_for_completion(self, statement_id: str, timeout: int = 300):
        """Wait for a statement to complete"""
        import time

        start_time = time.time()

        while time.time() - start_time < timeout:
            response = self.redshift_client.describe_statement(Id=statement_id)
            status = response["Status"]

            if status == "FINISHED":
                return response
            elif status == "FAILED":
                raise Exception(
                    f"Statement failed: {response.get('Error', 'Unknown error')}"
                )
            elif status == "ABORTED":
                raise Exception("Statement was aborted")

            await asyncio.sleep(2)

        raise Exception(f"Statement timed out after {timeout} seconds")

    @asynccontextmanager
    async def get_connection(self):
        """Get Redshift connection (Data API doesn't need persistent connections)"""
        yield self

    async def execute_query(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results"""
        try:
            # replace parameter placeholders
            formatted_query = query
            for i, arg in enumerate(args):
                formatted_query = formatted_query.replace(f"${i+1}", str(arg))

            response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.config.cluster_id_for_data_api,
                Database=self.config.database,
                DbUser=self.config.user,
                Sql=formatted_query,
            )

            result = await self._wait_for_completion(response["Id"])

            if result.get("ResultSet"):
                columns = [col["name"] for col in result["ResultSet"]["ColumnMetadata"]]
                rows = []
                for record in result["ResultSet"]["Records"]:
                    row = []
                    for field in record:
                        if "stringValue" in field:
                            row.append(field["stringValue"])
                        elif "longValue" in field:
                            row.append(field["longValue"])
                        elif "doubleValue" in field:
                            row.append(field["doubleValue"])
                        elif "booleanValue" in field:
                            row.append(field["booleanValue"])
                        else:
                            row.append(None)
                    rows.append(dict(zip(columns, row)))
                return rows
            else:
                return []

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    async def execute_command(self, command: str, *args) -> str:
        """Execute a command (INSERT, UPDATE, DELETE) with automatic commit"""
        try:
            # replace parameter placeholders
            formatted_command = command
            for i, arg in enumerate(args):
                formatted_command = formatted_command.replace(f"${i+1}", str(arg))

            # execute the command
            response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.config.cluster_id_for_data_api,
                Database=self.config.database,
                DbUser=self.config.user,
                Sql=formatted_command,
            )

            await self._wait_for_completion(response["Id"])

            # execute COMMIT separately
            commit_response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.config.cluster_id_for_data_api,
                Database=self.config.database,
                DbUser=self.config.user,
                Sql="COMMIT;",
            )

            # Wait for commit completion
            await self._wait_for_completion(commit_response["Id"])

            return "Command executed and committed successfully"

        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise

    async def execute_transaction(self, commands: List[str], *args) -> List[str]:
        """Execute multiple commands in a single transaction"""
        try:
            # Replace parameter placeholders for all commands
            formatted_commands = []
            for command in commands:
                formatted_command = command
                for i, arg in enumerate(args):
                    formatted_command = formatted_command.replace(f"${i+1}", str(arg))
                formatted_commands.append(formatted_command)

            # Execute BEGIN
            begin_response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.config.cluster_id_for_data_api,
                Database=self.config.database,
                DbUser=self.config.user,
                Sql="BEGIN;",
            )
            await self._wait_for_completion(begin_response["Id"])

            # Execute all commands
            for command in formatted_commands:
                response = self.redshift_client.execute_statement(
                    ClusterIdentifier=self.config.cluster_id_for_data_api,
                    Database=self.config.database,
                    DbUser=self.config.user,
                    Sql=command,
                )
                await self._wait_for_completion(response["Id"])

            # Execute COMMIT
            commit_response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.config.cluster_id_for_data_api,
                Database=self.config.database,
                DbUser=self.config.user,
                Sql="COMMIT;",
            )
            await self._wait_for_completion(commit_response["Id"])

            return ["All commands executed and committed successfully"]

        except Exception as e:
            logger.error(f"Transaction execution failed: {e}")
            raise


redshift_manager: Optional[RedshiftManager] = None


async def get_redshift_manager() -> RedshiftManager:
    """Get the global Redshift manager instance"""
    global redshift_manager
    if redshift_manager is None:
        config = RedshiftConfig()
        redshift_manager = RedshiftManager(config)
        await redshift_manager.initialize()
    return redshift_manager


async def close_redshift():
    """Close the global Redshift connection"""
    global redshift_manager
    if redshift_manager:
        await redshift_manager.close()
        redshift_manager = None
