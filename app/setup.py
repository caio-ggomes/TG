import happybase
from config_loader import ConfigLoader
from database_loader.middleware import DatabaseLoaderMiddleware


CONFIG_PATH = "app/config.yaml"


def setup_connection(config_loader: ConfigLoader) -> happybase.Connection:
    return happybase.Connection(
        host=config_loader.connection_host,
        port=config_loader.connection_port,
    )


def setup_table(connection: happybase.Connection, config_loader: ConfigLoader) -> happybase.Table:
    if config_loader.table_name.encode() in connection.tables():
        connection.delete_table(config_loader.table_name, disable=True)
    connection.create_table(
        name=config_loader.table_name,
        families={family: dict() for family in config_loader.column_families}
    )
    return connection.table(config_loader.table_name)


def main():

    config_loader = ConfigLoader(CONFIG_PATH)

    print("Setting up connection.")
    connection = setup_connection(config_loader)
    print("Creating table.")
    table = setup_table(connection, config_loader)

    for database_name in config_loader.database_names:
        print(f"Setting up {database_name} loader.")
        loader = DatabaseLoaderMiddleware().get_loader(
            **config_loader.get_loader_kwargs(database_name),
        )
        print(f"Loading data from {database_name} to table.")
        loader.load(table)

    print("Finished column family database setup. Closing HBase connection.")
    connection.close()


if __name__ == '__main__':
    main()
