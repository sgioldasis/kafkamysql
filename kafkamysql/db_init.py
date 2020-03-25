import utils

# Connect to database
db_connection = utils.connect()

# Get a cursor
db_cursor = db_connection.cursor()

# Run multiple statements 
for statement in utils.get_sql('db_init.sql').split('==='):
    if len(statement) > 0:
        print(statement)
        db_cursor.execute(statement)

# List database tables
db_cursor.execute("SHOW TABLES")
for table in db_cursor:
    print(table)

# Close the cursor
db_cursor.close()

# Close the connection to database
db_connection.close()