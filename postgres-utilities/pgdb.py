import psycopg2
import os
from dotenv import load_dotenv
from schema_manager import SchemaManager

# Loading environment variables
load_dotenv()

class Database:
    def __init__(self):
        # Retrieve credentials from environment variables
        self.dbname = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")        # localhost
        self.port = os.getenv("DB_PORT")        # 5432
        self.conn = None
        self.cur = None
        self.schema_manager = None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cur = self.conn.cursor()
            self.schema_manager = SchemaManager(self.conn)  # Initialize SchemaManager
            print(f'Connected to the database "{self.dbname}" successfully!')
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def close(self):
        """Close the cursor and connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("Connection closed.")

    def create_record(self, name, age):
        """Insert a new record into the users table."""
        query = "INSERT INTO users (name, age) VALUES (%s, %s)"
        try:
            self.cur.execute(query, (name, age))
            self.conn.commit()
            print("Record inserted successfully!")
        except Exception as e:
            print(f"Error inserting record: {e}")

    def read_records(self):
        """Retrieve all records from the users table."""
        query = "SELECT id, name, age FROM users"
        try:
            self.cur.execute(query)
            rows = self.cur.fetchall()
            for row in rows:
                print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")
        except Exception as e:
            print(f"Error reading records: {e}")

    def update_record(self, table_name, user_id, new_name, new_age):
        """Update an existing record in the users table."""
        query = "UPDATE users SET name = %s, age = %s WHERE id = %s"
        try:
            self.cur.execute(query, (new_name, new_age, user_id))
            self.conn.commit()
            print("Record updated successfully!")
        except Exception as e:
            print(f"Error updating record: {e}")

    def delete_record(self, user_id):
        """Delete a record from the users table."""
        query = "DELETE FROM users WHERE id = %s"
        try:
            self.cur.execute(query, (user_id,))
            self.conn.commit()
            print("Record deleted successfully!")
        except Exception as e:
            print(f"Error deleting record: {e}")

    def transaction_example(self):
        """Perform a multi-query transaction."""
        try:
            self.cur.execute("UPDATE users SET age = age + 1 WHERE age < 50")
            self.cur.execute("UPDATE users SET age = age - 1 WHERE age >= 50")
            self.conn.commit()  # Commit the transaction if all queries are successful
            print("Transaction completed successfully!")
        except Exception as e:
            self.conn.rollback()  # Rollback the transaction in case of error
            print(f"Error in transaction: {e}")

    def create_index(self):
        """Create an index on the 'name' column of the users table."""
        query = "CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)"
        try:
            self.cur.execute(query)
            self.conn.commit()
            print("Index created successfully!")
        except Exception as e:
            print(f"Error creating index: {e}")

    def get_top_users(self):
        """Retrieve the top 5 users with the highest ages using a CTE."""
        query = """
        WITH AgeRanks AS (
            SELECT id, name, age, RANK() OVER (ORDER BY age DESC) AS rank
            FROM users
        )
        SELECT id, name, age FROM AgeRanks WHERE rank <= 5;
        """
        try:
            self.cur.execute(query)
            rows = self.cur.fetchall()
            for row in rows:
                print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")
        except Exception as e:
            print(f"Error fetching top users: {e}")

    def get_user_by_id(self, user_id):
        """Retrieve a user by their ID."""
        query = "SELECT id, name, age FROM users WHERE id = %s"
        try:
            self.cur.execute(query, (user_id,))
            user = self.cur.fetchone()
            if user:
                print(f"ID: {user[0]}, Name: {user[1]}, Age: {user[2]}")
            else:
                print("User not found")
        except Exception as e:
            print(f"Error fetching user by ID: {e}")

    def handle_error(self):
        """Handle errors with try/except blocks."""
        try:
            self.cur.execute("SELECT * FROM non_existent_table")
            rows = self.cur.fetchall()
        except psycopg2.Error as e:
            print(f"An error occurred: {e}")