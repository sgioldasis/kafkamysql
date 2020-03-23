import mysql.connector

db_connection = mysql.connector.connect(
  host="localhost",
  user="user",
  passwd="password",
  database="db"
)

data = {
    "id": "d6707ce40a1447baaf012f948fb5b356",
    "customer_id": "8e0fdcefc3ad45acbb5a2abc506c6c9f",
    "created_at": "2019-05-04T23:34:19.5934998Z",
    "text": "<p>aliquam sed amet dolore consectetuer ut dolore elit dolor euismod elit erat</p>",
    "ad_type": "Premium",
    "price": 29.04,
    "currency": "EUR",
    "payment_type": "Card",
    "payment_cost": 0.17,
}

table_name = 'Classifieds'

sql_drop_table = f"""
DROP TABLE IF EXISTS `{table_name}`
"""

sql_create_table = """
CREATE TABLE `{table_name}` (
    id                  VARCHAR(255), 
    customer_id         VARCHAR(255), 
    created_at          VARCHAR(255)
)
"""

print('Creating table ...')

db_cursor = db_connection.cursor()

db_cursor.execute("DROP TABLE IF EXISTS `Classifieds`")

#Here creating database table as Classifieds'
db_cursor.execute("CREATE TABLE Classifieds (id INT, name VARCHAR(255))")

#Get database table'
db_cursor.execute("SHOW TABLES")
for table in db_cursor:
	print(table)