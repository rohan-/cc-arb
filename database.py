__author__ = 'rohan'

# fyp_trades | CREATE TABLE `fyp_trades` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `buy_price` decimal(20,15) NOT NULL,
#   `sell_price` decimal(20,15) NOT NULL,
#   `size` decimal(20,15) NOT NULL,
#   `status` varchar(20) NOT NULL,
#   `time_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#   `pair` varchar(20) NOT NULL,
#   PRIMARY KEY (`id`)
# )
#
# fyp_buy | CREATE TABLE `fyp_buy` (
#   `id` int(11) DEFAULT NULL,
#   `trade_ID` int(11) NOT NULL AUTO_INCREMENT,
#   `exchange_name` varchar(50) NOT NULL,
#   `price` decimal(20,15) NOT NULL,
#   `size` decimal(20,15) NOT NULL,
#   `total` decimal(20,15) NOT NULL,
#   `status` varchar(20) NOT NULL,
#   `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#   PRIMARY KEY (`trade_ID`),
#   KEY `id` (`id`),
#   CONSTRAINT `fyp_buy_ibfk_1` FOREIGN KEY (`id`) REFERENCES `fyp_trades` (`id`)
# )
#
# fyp_sell | CREATE TABLE `fyp_sell` (
#   `id` int(11) DEFAULT NULL,
#   `trade_ID` int(11) NOT NULL AUTO_INCREMENT,
#   `exchange_name` varchar(50) NOT NULL,
#   `price` decimal(20,15) NOT NULL,
#   `size` decimal(20,15) NOT NULL,
#   `total` decimal(20,15) NOT NULL,
#   `status` varchar(20) NOT NULL,
#   `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#   PRIMARY KEY (`trade_ID`),
#   KEY `id` (`id`),
#   CONSTRAINT `fyp_sell_ibfk_1` FOREIGN KEY (`id`) REFERENCES `fyp_trades` (`id`)
# )

import mysql.connector
from mysql.connector import Error

class database():
    try:
        connection = mysql.connector.connect(user='', password='', host='', database='')
        cursor = connection.cursor()

    except Error as e:
        print e

    def _database_operation(self, method, data = []):
        cursor = self.connection.cursor()

        if method == 'write':
            if self.connection.is_connected():
                cursor = self.connection.cursor()
                # sell price = data[1][4]
                #x = '245.55'
                cursor.execute("INSERT INTO fyp_trades (id, buy_price, sell_price, size, status, pair, profit) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                ('null', data[0][4], data[1][4], data[0][3], 'placed', data[0][1], (data[1][4] - data[0][4])))

                id_query = cursor.execute("SELECT id FROM fyp_trades ORDER BY id DESC LIMIT 1")
                id = str(cursor.fetchone()[0])

                cursor.execute("INSERT INTO fyp_buy (id, trade_ID, exchange_name, price, size, total, status) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (id, 'null', data[0][0], data[0][4], data[0][3], (data[0][3] * data[0][4]), 'executed'))

                cursor.execute("INSERT INTO fyp_sell (id, trade_ID, exchange_name, price, size, total, status) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (id, 'null', data[1][0], data[1][4], data[1][3], (data[1][3] * data[1][4]), 'executed'))

                self.connection.commit()
                cursor.close()

    def insert_trades(self, buy_data = [], sell_data = []):
        return self._database_operation('write', data = [buy_data, sell_data])

    def update_trades(self, id, trade_ID):
        pass

database_object = database()
