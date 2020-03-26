import utils
import argparse


def main(env):
    # Load configuration
    config = utils.load_config(env)
    mysql_config = config["mysql"]

    # Print DB info
    print(f"Environment    : {env}")
    print(f"MySQL host     : {mysql_config['host']}:{mysql_config['port']}")
    print(f"MySQL database : {mysql_config['db']}")

    # Connect to database
    db_connection = utils.connect(mysql_config)

    # Get a cursor
    db_cursor = db_connection.cursor()

    # Run multiple statements
    for statement in utils.get_sql("db_init.sql").split("==="):
        if len(statement) > 0:
            # print(statement)
            db_cursor.execute(statement)

    # List database tables
    db_cursor.execute("SHOW TABLES")
    for table in db_cursor:
        print(table)

    # Close the cursor
    db_cursor.close()

    # Close the connection to database
    db_connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize database")
    parser.add_argument(
        "env",
        nargs="?",
        choices=["test", "prod", "docker"],
        default="test",
        help="Environment code",
    )
    args = parser.parse_args()

    main(args.env)
