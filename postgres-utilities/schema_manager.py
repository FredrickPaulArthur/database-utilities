import psycopg2
from psycopg2 import sql

class SchemaManager:
    def __init__(self, connection):
        self.conn = connection
        self.cur = self.conn.cursor()
        self.table_name = ""
        self.cols_list = []

    def create_table(self, table_name, columns_query):
        """
        Create a new table.
        `columns_query` should be a string like `id SERIAL PRIMARY KEY, name VARCHAR(100), age INT`
        """
        query = sql.SQL(f"CREATE TABLE IF NOT EXISTS {sql.Identifier(table_name).string} ({sql.SQL(columns_query).string})")
        try:
            self.cur.execute(query)
            self.conn.commit()
            print(f"Table '{table_name}' created successfully!")
        except Exception as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()

    def delete_table(self, table_name):
        """Delete a table from the database."""
        query = sql.SQL(f"DROP TABLE IF EXISTS {sql.Identifier(table_name).string}")
        try:
            self.cur.execute(query)
            self.conn.commit()
            print(f"Table '{table_name}' deleted successfully!")
        except Exception as e:
            print(f"Error deleting table: {e}")
            self.conn.rollback()

    def add_column(self, table_name, column_name, column_type):
        """
            Add a new column to an existing table. `Ex: ALTER TABLE users ADD COLUMN income INT`.
        """
        query = sql.SQL(f"ALTER TABLE {sql.Identifier(table_name).string} ADD COLUMN {sql.Identifier(column_name).string} {sql.SQL(column_type).string}")
        try:
            self.cur.execute(query)
            self.conn.commit()
            print(f"Column '{column_name}' added to table '{table_name}' successfully!")
        except Exception as e:
            print(f"Error adding column: {e}")
            self.conn.rollback()

    def drop_column(self, table_name, column_name):
        """Remove a column from an existing table."""
        query = sql.SQL(f"ALTER TABLE {sql.Identifier(table_name).string} DROP COLUMN IF EXISTS {sql.Identifier(column_name).string}")
        try:
            self.cur.execute(query)
            self.conn.commit()
            print(f"Column '{column_name}' dropped from table '{table_name}' successfully!")
        except Exception as e:
            print(f"Error dropping column: {e}")
            self.conn.rollback()

    def rename_table(self, old_name, new_name):
        """Rename an existing table."""
        query = sql.SQL(f"ALTER TABLE {sql.Identifier(old_name).string} RENAME TO {sql.Identifier(new_name).string}")
        try:
            self.cur.execute(query)
            self.conn.commit()
            print(f"Table renamed from '{old_name}' to '{new_name}' successfully!")
        except Exception as e:
            print(f"Error renaming table: {e}")
            self.conn.rollback()

    def rename_column(self, table_name, old_column_name, new_column_name):
        """Rename a column in an existing table."""
        query = sql.SQL("ALTER TABLE {} RENAME COLUMN {} TO {}").format(
            sql.Identifier(table_name),
            sql.Identifier(old_column_name),
            sql.Identifier(new_column_name)
        )
        try:
            self.cur.execute(query)
            self.conn.commit()
            print(f"Column '{old_column_name}' renamed to '{new_column_name}' successfully!")
        except Exception as e:
            print(f"Error renaming column: {e}")
            self.conn.rollback()

    def modify_column_type(self, table_name, column_name, new_type):
        """Modify the data type of an existing column."""
        query = sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE {}").format(
            sql.Identifier(table_name),
            sql.Identifier(column_name),
            sql.SQL(new_type)
        )
        try:
            self.cur.execute(query)
            self.conn.commit()
            print(f"Column '{column_name}' type modified to '{new_type}' successfully!")
        except Exception as e:
            print(f"Error modifying column type: {e}")
            self.conn.rollback()
