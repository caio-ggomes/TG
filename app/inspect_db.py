from datetime import datetime
import struct

import happybase


HOST = "hbase" 
PORT = 9090
TABLE_NAME = "teste_1"


def main():

    print("Setting up connection.")
    connection = happybase.Connection(host=HOST, port=PORT)
    print("Fetching table.")
    table = connection.table(TABLE_NAME)

    for key, data in table.scan(row_prefix=b"TAM3434"):
        print(key, data)

    for value, timestamp in table.cells(row='TAM3434 2023-01-23 10:56:00', column="posicao:vl_latitude", include_timestamp=True)[-5:]:
        print(struct.unpack("f", value)[0], datetime.fromtimestamp(timestamp))

    print("Finished inspection. Closing HBase connection.")
    connection.close()


if __name__ == '__main__':
    main()
