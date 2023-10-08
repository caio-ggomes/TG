import happybase


HOST = "hbase" 
PORT = 9090
TABLE_NAME = "teste_1"


def main():

    print("Setting up connection.")
    connection = happybase.Connection(host=HOST, port=PORT)
    print("Fetching table.")
    table = connection.table(TABLE_NAME)

    for key, data in table.scan(row_prefix=b"GLO1005"):
        print(key, data)

    print("Finished inspection. Closing HBase connection.")
    connection.close()


if __name__ == '__main__':
    main()
