from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Customer import Customer, BadCustomer
from Business.Order import Order, BadOrder
from Business.Dish import Dish, BadDish
from Business.OrderDish import OrderDish
from decimal import Decimal


# ---------------------------------- CRUD API: ----------------------------------
# Basic database functions

def create_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE CUSTOMERS("
                     "cust_id INTEGER NOT NULL PRIMARY KEY CHECK (cust_id > 0),"
                     "full_name TEXT NOT NULL,"
                     "phone TEXT NOT NULL,"
                     "address TEXT NOT NULL CHECK (LENGTH(address) >= 3));"
                     ""
                     "CREATE TABLE ORDERS("
                     "order_id INTEGER NOT NULL PRIMARY KEY CHECK (order_id > 0),"
                     "date TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL);"
                     ""
                     "CREATE TABLE DISHES("
                     "dish_id INTEGER NOT NULL PRIMARY KEY CHECK (dish_id > 0),"
                     "name TEXT NOT NULL CHECK (LENGTH(name) >= 3),"
                     "price DECIMAL NOT NULL check (price > 0),"
                     "is_active BOOLEAN NOT NULL);"
                     ""
                     "CREATE TABLE CUSTOMERS_PLACE_ORDERS("
                     "order_id INTEGER PRIMARY KEY NOT NULL CHECK (order_id > 0),"
                     "FOREIGN KEY(order_id) REFERENCES ORDERS(order_id) ON DELETE CASCADE,"
                     "cust_id INTEGER NOT NULL CHECK (cust_id > 0),"
                     "FOREIGN KEY(cust_id) REFERENCES CUSTOMERS(cust_id) ON DELETE CASCADE);"
                     ""
                     "CREATE TABLE DISHES_IN_ORDERS("
                     "order_id INTEGER NOT NULL CHECK (order_id > 0),"
                     "dish_id INTEGER NOT NULL CHECK (dish_id > 0),"
                     "FOREIGN KEY(order_id) REFERENCES ORDERS(order_id) ON DELETE CASCADE,"
                     "FOREIGN KEY(dish_id) REFERENCES DISHES(dish_id) ON DELETE CASCADE,"
                     "PRIMARY KEY(order_id, dish_id), "
                     "amount INTEGER NOT NULL CHECK (amount > 0),"
                     "price DECIMAL NOT NULL CHECK ( price > 0 ) );"
                     ""
                     "CREATE TABLE CUSTOMERS_LIKE_DISHES("
                     "cust_id INTEGER NOT NULL CHECK (cust_id > 0),"
                     "dish_id INTEGER NOT NULL CHECK (dish_id > 0),"
                     "FOREIGN KEY (cust_id) REFERENCES CUSTOMERS(cust_id) ON DELETE CASCADE,"
                     "FOREIGN KEY (dish_id) REFERENCES DISHES(dish_id) ON DELETE CASCADE,"
                     "PRIMARY KEY (cust_id, dish_id));"
                     ""
                     "CREATE VIEW ACTIVE_DISHES_VIEW AS "
                     "SELECT dish_id, price "
                     "FROM DISHES "
                     "WHERE is_active = True;"
                     ""
                     "CREATE VIEW CUSTOMERS_ORDERS_TOTAL_PRICE_VIEW AS "
                     "SELECT DIO.order_id, CPO.cust_id, "
                     "COALESCE(SUM(DIO.amount * DIO.price), 0.0) AS total_order_price "
                     "FROM DISHES_IN_ORDERS DIO "
                     "LEFT OUTER JOIN CUSTOMERS_PLACE_ORDERS CPO ON CPO.order_id = DIO.order_id "
                     "GROUP BY DIO.order_id, CPO.cust_id;"
                     ""
                     "CREATE VIEW MOST_LIKED_DISHES_RANKING_VIEW AS "
                     "SELECT D.dish_id, COALESCE(COUNT(CLD.cust_id), 0) AS amount_likes "
                     "FROM DISHES D "
                     "LEFT OUTER JOIN CUSTOMERS_LIKE_DISHES CLD ON D.dish_id = CLD.dish_id "
                     "GROUP BY D.dish_id;"
                     ""
                     "CREATE VIEW MOST_LIKED_DISH_VIEW AS "
                     "SELECT MLDRV.dish_id, MLDRV.amount_likes "
                     "FROM MOST_LIKED_DISHES_RANKING_VIEW MLDRV "
                     "where MLDRV.amount_likes > 0;"
                     ""
                     "CREATE VIEW MOST_PURCHASED_DISH_VIEW AS "
                     "SELECT dish_id, SUM(amount) AS purchased_amount "
                     "FROM DISHES_IN_ORDERS "
                     "GROUP BY dish_id;"
                     ""
                     "CREATE VIEW PROFIT_PER_MONTH_VIEW AS "
                     "SELECT EXTRACT(YEAR FROM O.date) AS year, "
                     "EXTRACT(MONTH FROM O.date) AS month, "
                     "COALESCE(SUM(DIO.price * DIO.amount), 0.0) AS profit "
                     "FROM ORDERS O "
                     "LEFT OUTER JOIN DISHES_IN_ORDERS DIO ON O.order_id = DIO.order_id "
                     "GROUP BY EXTRACT(YEAR FROM O.date), EXTRACT(MONTH FROM O.date);"
                     ""
                     "CREATE VIEW MONTHS_VIEW AS "
                     "SELECT 1 AS month UNION ALL "
                     "SELECT 2 UNION ALL "
                     "SELECT 3 UNION ALL "
                     "SELECT 4 UNION ALL "
                     "SELECT 5 UNION ALL "
                     "SELECT 6 UNION ALL "
                     "SELECT 7 UNION ALL "
                     "SELECT 8 UNION ALL "
                     "SELECT 9 UNION ALL "
                     "SELECT 10 UNION ALL "
                     "SELECT 11 UNION ALL "
                     "SELECT 12;"
                     ""
                     "CREATE VIEW SIMILAR_CUSTOMERS_VIEW AS "
                     "SELECT CLD1.cust_id AS customer, CLD2.cust_id AS similar_customer "
                     "FROM CUSTOMERS_LIKE_DISHES CLD1 "
                     "JOIN CUSTOMERS_LIKE_DISHES CLD2 ON  CLD1.dish_id = CLD2.dish_id "
                     "WHERE CLD1.cust_id != CLD2.cust_id "
                     "GROUP BY CLD1.cust_id , CLD2.cust_id "
                     "HAVING COUNT(DISTINCT CLD1.dish_id) >= 3;"
                     ""
                     "CREATE VIEW ORDERED_DISHES_PROFIT_VIEW AS "
                     "SELECT DIO.dish_id, DIO.price, "
                     "DIO.price * AVG(DIO.amount) AS average_profit "
                     "FROM DISHES_IN_ORDERS DIO GROUP BY DIO.dish_id, DIO.price;"
                     ""
                     "CREATE VIEW ACTIVE_ORDERED_DISHES_CURRENT_PROFIT_VIEW AS "
                     "SELECT D.dish_id, ODPW.average_profit AS current_average_profit "
                     "FROM DISHES D JOIN ORDERED_DISHES_PROFIT_VIEW ODPW "
                     "ON D.dish_id = ODPW.dish_id "
                     "WHERE D.is_active = TRUE AND D.price = ODPW.price;")
    except DatabaseException.ConnectionInvalid as e:
        return None
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return None
    except DatabaseException.CHECK_VIOLATION as e:
        return None
    except DatabaseException.UNIQUE_VIOLATION as e:
        return None
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return None
    except Exception as e:
        return None
    finally:
        # will happen any way after try termination or exception handling
        if conn is not None:
            conn.close()


def clear_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM CUSTOMERS_LIKE_DISHES;"
                     "DELETE FROM DISHES_IN_ORDERS;"
                     "DELETE FROM CUSTOMERS_PLACE_ORDERS;"
                     "DELETE FROM ORDERS;"
                     "DELETE FROM DISHES;"
                     "DELETE FROM CUSTOMERS;")
    except DatabaseException.ConnectionInvalid as e:
        return None
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return None
    except DatabaseException.CHECK_VIOLATION as e:
        return None
    except DatabaseException.UNIQUE_VIOLATION as e:
        return None
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return None
    except Exception as e:
        return None
    finally:
        if conn is not None:
            conn.close()


def drop_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP VIEW IF EXISTS ACTIVE_ORDERED_DISHES_CURRENT_PROFIT_VIEW;"
                     "DROP VIEW IF EXISTS ORDERED_DISHES_PROFIT_VIEW;"
                     "DROP VIEW IF EXISTS SIMILAR_CUSTOMERS_VIEW;"
                     "DROP VIEW IF EXISTS MONTHS_VIEW;"
                     "DROP VIEW IF EXISTS PROFIT_PER_MONTH_VIEW;"
                     "DROP VIEW IF EXISTS MOST_PURCHASED_DISH_VIEW;"
                     "DROP VIEW IF EXISTS MOST_LIKED_DISH_VIEW;"
                     "DROP VIEW IF EXISTS MOST_LIKED_DISHES_RANKING_VIEW;"
                     "DROP VIEW IF EXISTS CUSTOMERS_ORDERS_TOTAL_PRICE_VIEW;"
                     "DROP VIEW IF EXISTS ACTIVE_DISHES_VIEW;"
                     "DROP TABLE IF EXISTS CUSTOMERS_LIKE_DISHES CASCADE;"
                     "DROP TABLE IF EXISTS DISHES_IN_ORDERS CASCADE;"
                     "DROP TABLE IF EXISTS CUSTOMERS_PLACE_ORDERS CASCADE;"
                     "DROP TABLE IF EXISTS ORDERS CASCADE;"
                     "DROP TABLE IF EXISTS DISHES CASCADE;"
                     "DROP TABLE IF EXISTS CUSTOMERS CASCADE;")
    except DatabaseException.ConnectionInvalid as e:
        return None
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return None
    except DatabaseException.CHECK_VIOLATION as e:
        return None
    except DatabaseException.UNIQUE_VIOLATION as e:
        return None
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return None
    except Exception as e:
        return None
    finally:
        # will happen any way after try termination or exception handling
        if conn is not None:
            conn.close()


# CRUD API

def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO CUSTOMERS(cust_id, full_name, phone, address)"
                        "VALUES({}, {}, {}, {})").format(sql.Literal(customer.get_cust_id()),
                                                         sql.Literal(customer.get_full_name()),
                                                         sql.Literal(customer.get_phone()),
                                                         sql.Literal(customer.get_address()))

        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def get_customer(customer_id: int) -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT cust_id, full_name, phone, address FROM CUSTOMERS WHERE cust_id = {id}").format(
            id=sql.Literal(customer_id))
        rows_effected, res = conn.execute(query)
        if not rows_effected:
            return BadCustomer()
        cust_id, full_name, phone, address = res.rows[0]
        return Customer(cust_id, full_name, phone, address)
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM CUSTOMERS WHERE cust_id = {id}").format(id=sql.Literal(customer_id))
        rows_effected, _ = conn.execute(query)
        if not rows_effected:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def add_order(order: Order) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO ORDERS(order_id, date)"
                        "VALUES({}, {})").format(sql.Literal(order.get_order_id()), sql.Literal(order.get_datetime()))
        rows_effected, _ = conn.execute(query)
        # if not rows_effected:
        #     return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def get_order(order_id: int) -> Order:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT order_id, date FROM ORDERS WHERE order_id = {id}").format(
            id=sql.Literal(order_id))
        rows_effected, res = conn.execute(query)
        if not rows_effected:
            return BadOrder()
        id_order, order_date = res.rows[0]
        return Order(id_order, order_date)
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


def delete_order(order_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM ORDERS WHERE order_id = {id}").format(id=sql.Literal(order_id))
        rows_effected, _ = conn.execute(query)
        if not rows_effected:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def add_dish(dish: Dish) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO DISHES(dish_id, name, price, is_active) VALUES({}, {}, {}, {})").format(
            sql.Literal(dish.get_dish_id()),
            sql.Literal(dish.get_name()),
            sql.Literal(dish.get_price()),
            sql.Literal(dish.get_is_active())
        )
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def get_dish(dish_id: int) -> Dish:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT dish_id, name, price, is_active FROM DISHES WHERE dish_id = {id}").format(
            id=sql.Literal(dish_id)
        )
        rows_effected, result = conn.execute(query)
        if rows_effected == 0:
            return BadDish()
        row = result.rows[0]
        return Dish(row[0], row[1], float(row[2]), row[3])
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


# CHECKED
def update_dish_price(dish_id: int, price: float) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE DISHES SET price = {price} WHERE dish_id = {id} "
                        "AND price > 0 AND {id} IN (SELECT dish_id FROM ACTIVE_DISHES_VIEW)").format(
            price=sql.Literal(price),
            id=sql.Literal(dish_id)
        )
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def update_dish_active_status(dish_id: int, is_active: bool) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE DISHES SET is_active = {is_active} WHERE dish_id = {id}").format(
            is_active=sql.Literal(is_active),
            id=sql.Literal(dish_id)
        )
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def customer_placed_order(customer_id: int, order_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO CUSTOMERS_PLACE_ORDERS(order_id, cust_id) VALUES({}, {})").format(
            sql.Literal(order_id), sql.Literal(customer_id))
        rows_effected, _ = conn.execute(query)
        # if rows_effected == 0:
        #     return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def get_customer_that_placed_order(order_id: int) -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT cust_id, full_name, phone, address FROM CUSTOMERS "
            "WHERE cust_id = ("
            "   SELECT cust_id FROM CUSTOMERS_PLACE_ORDERS "
            "   WHERE order_id = {id})"
        ).format(id=sql.Literal(order_id))
        rows_effected, result = conn.execute(query)
        if rows_effected == 0:
            return BadCustomer()
        customer_id, customer_name, customer_phone, customer_address = result.rows[0]
        return Customer(customer_id, customer_name, customer_phone, customer_address)
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


def order_contains_dish(order_id: int, dish_id: int, amount: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO DISHES_IN_ORDERS(order_id, dish_id, amount, price) "
                        "SELECT {id_order}, {id_dish}, {dish_amount}, ADV.price "
                        "FROM ACTIVE_DISHES_VIEW ADV "
                        "WHERE ADV.dish_id = {id_dish} ").format(id_order=sql.Literal(order_id),
                                                                 dish_amount=sql.Literal(amount),
                                                                 id_dish=sql.Literal(dish_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS  # wasn't in active_dishes_view (might not be there since id is invalid)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def order_does_not_contain_dish(order_id: int, dish_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM DISHES_IN_ORDERS WHERE order_id = {order_id} AND dish_id = {dish_id}").format(
            order_id=sql.Literal(order_id),
            dish_id=sql.Literal(dish_id),
        )
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def get_all_order_items(order_id: int) -> List[OrderDish]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT dish_id, amount, price FROM DISHES_IN_ORDERS WHERE order_id = {id} ORDER BY "
                        "dish_id ASC").format(id=sql.Literal(order_id))
        rows_effected, res = conn.execute(query)
        if not rows_effected:
            return []
        dishes_in_order = []
        for row in res.rows:
            dish_id, dish_amount, dish_price = row
            dishes_in_order.append(OrderDish(dish_id, dish_amount, dish_price))
        return dishes_in_order
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


def customer_likes_dish(cust_id: int, dish_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO CUSTOMERS_LIKE_DISHES(cust_id, dish_id) VALUES({}, {})"
        ).format(sql.Literal(cust_id), sql.Literal(dish_id))
        rows_effected, _ = conn.execute(query)
        # if rows_effected == 0:
        #     return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def customer_dislike_dish(cust_id: int, dish_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM CUSTOMERS_LIKE_DISHES WHERE cust_id = {} AND dish_id = {}"
        ).format(
            sql.Literal(cust_id),
            sql.Literal(dish_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()
    return ReturnValue.OK


def get_all_customer_likes(cust_id: int) -> List[Dish]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT d.dish_id, d.name, d.price, d.is_active "
                        "FROM DISHES d JOIN CUSTOMERS_LIKE_DISHES cld ON d.dish_id = cld.dish_id "
                        "WHERE cld.cust_id = {cust_id} "
                        "ORDER BY d.dish_id ASC").format(cust_id=sql.Literal(cust_id))
        rows_effected, result = conn.execute(query)
        # if no dishes are liked or customer doesn't exist, return empty list
        if rows_effected == 0:
            return []
        dishes = []
        for row in result.rows:
            dish_id, dish_name, dish_price, dish_status = row
            dishes.append(Dish(dish_id, dish_name, dish_price, dish_status))
        return dishes
    except DatabaseException.ConnectionInvalid as e:
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return []
    except Exception as e:
        return []
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------- BASIC API: ----------------------------------

# Basic API

#  in get_order_total_price the order id can be of an anonymous order
def get_order_total_price(order_id: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT total_order_price FROM CUSTOMERS_ORDERS_TOTAL_PRICE_VIEW WHERE order_id = {id}").format(
            id=sql.Literal(order_id))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            return 0.0  # the order has 0 dishes
        total_price_order_id = res.rows[0][0]
        return float(total_price_order_id) if total_price_order_id is not None else 0.0  # SUM() might return NULL
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


def get_max_amount_of_money_cust_spent(cust_id: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT MAX(total_order_price) AS customer_max_money_order_spent "
                        "FROM CUSTOMERS_ORDERS_TOTAL_PRICE_VIEW WHERE cust_id = {}").format(
            sql.Literal(cust_id)
        )
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            return 0.0
        max_spent = res.rows[0][0]
        return float(max_spent) if max_spent is not None else 0.0  # max() can return NULL if all the values are NULL
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


def get_most_expensive_anonymous_order() -> Order:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT O.order_id, O.date, COALESCE(SUM(DIO.price * DIO.amount), 0.0) AS total_price "
                        "FROM ORDERS O "
                        "LEFT OUTER JOIN DISHES_IN_ORDERS DIO ON O.order_id = DIO.order_id "
                        "WHERE O.order_id NOT IN (SELECT CPO.order_id FROM CUSTOMERS_PLACE_ORDERS CPO) "
                        "GROUP BY O.order_id, O.date "
                        "ORDER BY total_price DESC, O.order_id ASC")
        rows_effected, res = conn.execute(query)
        return Order(res.rows[0][0], res.rows[0][1])
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()


def is_most_liked_dish_equal_to_most_purchased() -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT top_purchased.dish_id AS dish_id FROM"
                        "(SELECT MPDV.dish_id FROM MOST_PURCHASED_DISH_VIEW MPDV "
                        "ORDER BY MPDV.purchased_amount DESC, MPDV.dish_id ASC "
                        "LIMIT 1) AS top_purchased "
                        "INNER JOIN "
                        "(SELECT MLDV.dish_id FROM MOST_LIKED_DISH_VIEW MLDV ORDER BY "
                        "MLDV.amount_likes DESC, MLDV.dish_id ASC LIMIT 1) AS top_liked "
                        "ON top_purchased.dish_id = top_liked.dish_id")
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            return False
    except Exception as e:
        pass
    finally:
        if conn is not None:
            conn.close()
    return True


# ---------------------------------- ADVANCED API: ----------------------------------

# Advanced API

def get_customers_ordered_top_5_dishes() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT CPO.cust_id "
                        "FROM CUSTOMERS_PLACE_ORDERS CPO "
                        "INNER JOIN DISHES_IN_ORDERS DIO ON CPO.order_id = DIO.order_id "
                        "WHERE DIO.dish_id IN (SELECT MLDRV.dish_id FROM MOST_LIKED_DISHES_RANKING_VIEW MLDRV "
                        "ORDER BY MLDRV.amount_likes DESC , MLDRV.dish_id ASC LIMIT 5) "
                        "GROUP BY CPO.cust_id "
                        "HAVING COUNT(DISTINCT DIO.dish_id) = 5 "
                        "ORDER BY CPO.cust_id ASC")
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            return []
        return [row[0] for row in res.rows]
    except DatabaseException.ConnectionInvalid as e:
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return []
    except Exception as e:
        return []
    finally:
        if conn is not None:
            conn.close()


def get_non_worth_price_increase() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = ("SELECT DISTINCT ODPW.dish_id "
                 "FROM ORDERED_DISHES_PROFIT_VIEW ODPW "
                 "JOIN ACTIVE_ORDERED_DISHES_CURRENT_PROFIT_VIEW AODCPV "
                 "ON ODPW.dish_id = AODCPV.dish_id "
                 "WHERE ODPW.average_profit > AODCPV.current_average_profit "
                 "AND ODPW.price < (SELECT price FROM DISHES WHERE dish_id = AODCPV.dish_id) "
                 "ORDER BY ODPW.dish_id ASC;")
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            return []
        id_list_res = []
        for row in res.rows:
            id_list_res.append(row[0])
        return id_list_res
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return []
    except Exception as e:
        return []
    finally:
        if conn is not None:
            conn.close()


def get_total_profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT M.month, COALESCE(SUM(PPMV.profit), 0.0) AS profit "
                        "FROM MONTHS_VIEW M "
                        "LEFT OUTER JOIN PROFIT_PER_MONTH_VIEW PPMV "
                        "ON PPMV.month = M.month AND PPMV.year = {} "
                        "GROUP BY M.month "
                        "ORDER BY M.month DESC").format(sql.Literal(year))
        _, res = conn.execute(query)
        profit_per_month = []
        for row in res.rows:
            profit_per_month.append((row[0], float(row[1])))
        return profit_per_month
    except DatabaseException.ConnectionInvalid as e:
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return []
    except Exception as e:
        return []
    finally:
        if conn is not None:
            conn.close()


def get_potential_dish_recommendations(cust_id: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT CLD.dish_id "
                        "FROM CUSTOMERS_LIKE_DISHES CLD "
                        "WHERE CLD.cust_id IN(SELECT SCV.similar_customer "
                        "FROM SIMILAR_CUSTOMERS_VIEW SCV WHERE SCV.customer = {id}) AND CLD.dish_id NOT IN("
                        "SELECT dish_id from CUSTOMERS_LIKE_DISHES WHERE cust_id = {id}) "
                        "ORDER BY CLD.dish_id ASC").format(id=sql.Literal(cust_id))
        rows_affected, res = conn.execute(query)
        if rows_affected == 0:
            return []
        dish_id_recommendations = []
        for row in res.rows:
            dish_id_recommendations.append(row[0])
        return dish_id_recommendations
    except DatabaseException.ConnectionInvalid as e:
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return []
    except Exception as e:
        return []
    finally:
        if conn is not None:
            conn.close()
