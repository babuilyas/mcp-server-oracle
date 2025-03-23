import oracledb
import asyncio

connection_string = ""


async def list_tables() -> list:
    tables = []
    try:
        # Run database operations in a separate thread
        def db_operation():
            result_tables = []
            with oracledb.connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT table_name FROM user_tables ORDER BY table_name")
                for row in cursor:
                    result_tables.append(row[0])
            return '\n'.join(result_tables)

        return await asyncio.to_thread(db_operation)
    except oracledb.DatabaseError as e:
        print('Error occurred:', e)
        return str(e)


async def describe_table(table_name: str) -> str:
    try:
        # Run database operations in a separate thread
        def db_operation(table):
            with oracledb.connect(connection_string) as conn:
                cursor = conn.cursor()

                # Create CSV headers
                result = [
                    "COLUMN_NAME,DATA_TYPE,NULLABLE,DATA_LENGTH,PRIMARY_KEY,FOREIGN_KEY"]

                # Get primary key columns
                pk_columns = []
                cursor.execute(
                    """
                    SELECT cols.column_name
                    FROM all_constraints cons, all_cons_columns cols
                    WHERE cons.constraint_type = 'P'
                    AND cons.constraint_name = cols.constraint_name
                    AND cons.owner = cols.owner
                    AND cols.table_name = :table_name
                    """,
                    table_name=table.upper()
                )
                for row in cursor:
                    pk_columns.append(row[0])

                # Get foreign key columns and references
                fk_info = {}
                cursor.execute(
                    """
                    SELECT a.column_name, c_pk.table_name as referenced_table, b.column_name as referenced_column
                    FROM all_cons_columns a
                    JOIN all_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name
                    JOIN all_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name
                    JOIN all_cons_columns b ON c_pk.owner = b.owner AND c_pk.constraint_name = b.constraint_name
                    WHERE c.constraint_type = 'R'
                    AND a.table_name = :table_name
                    """,
                    table_name=table.upper()
                )
                for row in cursor:
                    fk_info[row[0]] = f"{row[1]}.{row[2]}"

                # Get main column information
                cursor.execute(
                    """
                    SELECT column_name, data_type, nullable, data_length 
                    FROM user_tab_columns 
                    WHERE table_name = :table_name 
                    ORDER BY column_id
                    """,
                    table_name=table.upper()
                )

                rows_found = False
                for row in cursor:
                    rows_found = True
                    column_name = row[0]
                    data_type = row[1]
                    nullable = row[2]
                    data_length = str(row[3])
                    is_pk = "YES" if column_name in pk_columns else "NO"
                    fk_ref = fk_info.get(column_name, "NO")

                    # Format as CSV row
                    result.append(
                        f"{column_name},{data_type},{nullable},{data_length},{is_pk},{fk_ref}")

                if not rows_found:
                    return f"Table {table} not found or has no columns."

                return '\n'.join(result)

        return await asyncio.to_thread(db_operation, table_name)
    except oracledb.DatabaseError as e:
        print('Error occurred:', e)
        return str(e)


async def read_query(query: str) -> str:
    try:
        # Check if the query is a SELECT statement
        if not query.strip().upper().startswith('SELECT'):
            return "Error: Only SELECT statements are supported."

        # Run database operations in a separate thread
        def db_operation(query):
            with oracledb.connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(query)  # Execute query first

                # Get column names after executing the query
                columns = [col[0] for col in cursor.description]
                result = [','.join(columns)]  # Add column headers

                # Process each row
                for row in cursor:
                    # Convert each value in the tuple to string
                    string_values = [
                        str(val) if val is not None else "NULL" for val in row]
                    result.append(','.join(string_values))

                return '\n'.join(result)

        return await asyncio.to_thread(db_operation, query)
    except oracledb.DatabaseError as e:
        print('Error occurred:', e)
        return str(e)


if __name__ == "__main__":
    # Create and run the async event loop
    async def main():
        # print(await list_tables())
        print(await describe_table('CONTACT'))

    asyncio.run(main())
