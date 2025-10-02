from pgdb import Database
from dotenv import load_dotenv
load_dotenv()

## Code for a defined schema = | id | Name | Age |

# Initialize the Database instance and connect - Retrieve credentials from environment variables
db = Database()
db.connect()

# Creating a table using SchemaManager
columns_query = "id SERIAL PRIMARY KEY, name VARCHAR(100), age INT"
users_schema_manager = db.schema_manager


# CRUD operations (using the existing Database class methods)

# CREATE
users_schema_manager.create_table("users", columns_query)
db.create_record('John Doe', 30)
db.create_record('Maria DB', 22)
db.create_record('Mongo DB', 34)
db.create_record('Jane', 12)

# READ
db.read_records()
# db.schema_manager.add_column("users", "email", "VARCHAR(100)")  # Modifying schema : Update (e.g., adding a new column)

# UPDATE
# Edit specific rows in a table where the name="xyz"
db.update_record("users", 1, "Bob Marley", 35)
db.read_records()

print()
db.cur.execute("SELECT * FROM users;")
print(db.cur.fetchall())

# DELETE
db.schema_manager.delete_table("users")     # Deleting a table

# Close the connection when done
db.close()
