import db_utils

# Connect to database
db_connection = db_utils.connect()

# Get a cursor
db_cursor = db_connection.cursor()

# Drop database table
# db_cursor.execute(db_utils.get_sql('drop_table.sql'))

# Create database table
# db_cursor.execute(db_utils.get_sql('create_table.sql'))

# Test multiple statements
# with db_connection.cursor() as cursor:
for statement in db_utils.get_sql('test_margins.sql').split('//'):
    if len(statement) > 0:
        print(statement)
        db_cursor.execute(statement)


# List database tables
db_cursor.execute("SHOW TABLES")
for table in db_cursor:
    print(table)

# Close the connection to database
db_connection.close()