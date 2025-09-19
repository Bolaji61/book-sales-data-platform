#!/usr/bin/env python3
"""
Database initialization script
"""

import asyncio
import logging

from database.connection import DatabaseConfig, DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def initialize_database():
    """Initialize the database with schema and initial data"""
    try:
        config = DatabaseConfig()
        db_manager = DatabaseManager(config)

        await db_manager.initialize()

        logger.info("Database initialized successfully!")

        async with db_manager.get_connection() as conn:
            result = await conn.fetchval("SELECT version()")
            logger.info(f"Connected to PostgreSQL: {result}")

            # Check if tables exist
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
            tables = await conn.fetch(tables_query)
            logger.info(
                f"Found {len(tables)} tables: {[t['table_name'] for t in tables]}"
            )

        await db_manager.close()

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(initialize_database())
