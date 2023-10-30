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

    for key, data in table.scan(row_prefix=b"GLO9236"):
        print(key, data)

    for value, timestamp in table.cells(row='GLO9236 2023-01-23 00:00:00', column="posicao:vl_latitude", include_timestamp=True)[-5:]:
        print(struct.unpack("f", value)[0], datetime.fromtimestamp(timestamp))

    print("Finished inspection. Closing HBase connection.")
    connection.close()


if __name__ == '__main__':
    main()
