import os
import sys
import signal
from typing import Any
from mcp.server.fastmcp import FastMCP
from . import oracle_tools
from dotenv import load_dotenv


# Load the environment variables
load_dotenv()

# Initialize the FastMCP server
mcp = FastMCP("mcp-server-oracle")

oracle_tools.connection_string = os.getenv("ORACLE_CONNECTION_STRING")


@mcp.tool()
async def list_tables() -> str:
    """Get a list of all tables in the oracle database

    Args:
        None
    """
    return await oracle_tools.list_tables()


@mcp.tool()
async def describe_table(table_name: str) -> str:
    """Get a description of a table in the oracle database"

    Args:
        table_name (string): The name of the table to describe
    """
    return await oracle_tools.describe_table(table_name)


@mcp.tool()
async def reqd_query(query: str) -> str:
    """Execute SELECT queries to read data from the oracle database

    Args:
        query (string): The SELECT query to execute
    """
    return await oracle_tools.read_query(query)


def main() -> None:
    mcp.run(transport='stdio')


def dev() -> None:
    """
    Development function that handles Ctrl+C gracefully.
    This function calls main() but catches KeyboardInterrupt to allow 
    clean exit when user presses Ctrl+C.
    """
    print("mcp server starting", file=sys.stderr)

    # Define signal handler for cleaner exit
    def signal_handler(sig, frame):
        print("\nShutting down mcp server...", file=sys.stderr)
        sys.exit(0)

    # Register the signal handler for SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Run the server with proper exception handling
        main()
    except KeyboardInterrupt:
        print("\nShutting down mcp server...", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
