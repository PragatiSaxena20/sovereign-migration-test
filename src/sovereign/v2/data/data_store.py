import logging
import pickle
import sqlite3
from enum import StrEnum
from typing import Any, Protocol

from structlog.typing import FilteringBoundLogger

from sovereign import config
from sovereign.types import DiscoveryRequest, DiscoveryResponse
from sovereign.v2.logging import get_named_logger
from sovereign.v2.types import Context, DiscoveryEntry, WorkerNode


class ComparisonOperator(StrEnum):
    EqualTo = "equal_to"
    LessThanOrEqualTo = "less_than_or_equal_to"
    GreaterThan = "greater_than"


class DataType(StrEnum):
    Context = "context"
    DiscoveryEntry = "discovery_request"
    WorkerNode = "worker_node"


class DataStoreProtocol(Protocol):
    def count_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> int: ...
    def delete_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> bool: ...
    def find_all_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> list[Any]: ...
    def find_all_matching_property(
        self,
        data_type: DataType,
        match_property_name: str,
        comparison_operator: ComparisonOperator,
        match_property_value: Any,
        property_name: str,
    ) -> list[Any]:
        """
        Find all items of the given data type where the 'match property' matches the given value
        according to the specified comparison operator, and return the specified property from
        each matching item.
        """
        ...

    def get(self, data_type: DataType, key: str) -> Any | None: ...
    def get_property(
        self, data_type: DataType, key: str, property_name: str,
    ) -> Any | None: ...
    def min_by_property(
        self,
        data_type: DataType,
        property_name: str,
    ) -> Any | None: ...
    def set(self, data_type: DataType, key: str, value: Any) -> bool: ...
    def set_property(
        self, data_type: DataType, key: str, property_name: str, property_value: Any,
    ) -> bool: ...


class InMemoryDataStore(DataStoreProtocol):
    def __init__(self):
        self.logger: FilteringBoundLogger = get_named_logger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            level=logging.INFO,
        )

        self.stores: dict[DataType, dict[str, Any]] = {
            DataType.Context: dict[str, Context](),
            DataType.DiscoveryEntry: dict[str, DiscoveryEntry](),
            DataType.WorkerNode: dict[str, WorkerNode](),
        }

    @staticmethod
    def _compare(left: Any, operator: ComparisonOperator, right: Any) -> bool:
        if operator == ComparisonOperator.EqualTo:
            return left == right
        elif operator == ComparisonOperator.LessThanOrEqualTo:
            return left <= right
        elif operator == ComparisonOperator.GreaterThan:
            return left > right
        return False

    def count_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> int:
        store: dict[str, Any] = self.stores[data_type]
        return sum(
            1
            for item in store.values()
            if self._compare(
                getattr(item, property_name), comparison_operator, property_value,
            )
        )

    def delete_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> bool:
        store: dict[str, Any] = self.stores[data_type]
        keys_to_delete = [
            key
            for key, store_item in store.items()
            if self._compare(
                getattr(store_item, property_name), comparison_operator, property_value,
            )
        ]

        for key in keys_to_delete:
            self.logger.debug("Deleting item", data_type=data_type, key=key)
            del store[key]

        return True

    def find_all_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> list[Any]:
        store: dict[str, Any] = self.stores[data_type]
        return [
            item
            for item in store.values()
            if self._compare(
                getattr(item, property_name), comparison_operator, property_value,
            )
        ]

    def find_all_matching_property(
        self,
        data_type: DataType,
        match_property_name: str,
        comparison_operator: ComparisonOperator,
        match_property_value: Any,
        property_name: str,
    ) -> list[Any]:
        return [
            getattr(value, property_name)
            for value in self.find_all_matching(
                data_type,
                match_property_name,
                comparison_operator,
                match_property_value,
            )
        ]

    def get(self, data_type: DataType, key: str) -> Any | None:
        store: dict[str, Any] = self.stores[data_type]
        return store.get(key)

    def get_property(
        self, data_type: DataType, key: str, property_name: str,
    ) -> Any | None:
        if value := self.get(data_type, key):
            return getattr(value, property_name)
        return None

    def min_by_property(
        self,
        data_type: DataType,
        property_name: str,
    ) -> Any | None:
        store = self.stores[data_type]
        if not store:
            return None
        return min(store.values(), key=lambda item: getattr(item, property_name))

    def set(self, data_type: DataType, key: str, value: Any) -> bool:
        store: dict[str, Any] = self.stores[data_type]
        store[key] = value
        return True

    def set_property(
        self, data_type: DataType, key: str, property_name: str, property_value: Any,
    ) -> bool:
        item = self.get(data_type, key)
        if item is None:
            return False
        setattr(item, property_name, property_value)
        return True


def _get_primary_key(data_type: DataType) -> str:
    match data_type:
        case DataType.Context:
            return "name"
        case DataType.DiscoveryEntry:
            return "request_hash"
        case DataType.WorkerNode:
            return "node_id"


def _get_operator_sql(operator: ComparisonOperator) -> str:
    if operator == ComparisonOperator.EqualTo:
        return "="
    if operator == ComparisonOperator.LessThanOrEqualTo:
        return "<="
    if operator == ComparisonOperator.GreaterThan:
        return ">"
    raise ValueError(f"Unsupported comparison operator: {operator}")


def _get_table_name(data_type: DataType) -> str:
    match data_type:
        case DataType.Context:
            return "contexts"
        case DataType.DiscoveryEntry:
            return "discovery_entries"
        case DataType.WorkerNode:
            return "worker_nodes"


def _validate_column(data_type: DataType, column_name: str) -> str | None:
    valid_columns = {
        DataType.Context: {
            "name",
            "data",
            "data_hash",
            "last_refreshed_at",
            "refresh_after",
        },
        DataType.DiscoveryEntry: {
            "request_hash",
            "template",
            "request",
            "response",
            "last_rendered_at",
        },
        DataType.WorkerNode: {"node_id", "last_heartbeat"},
    }
    if column_name not in valid_columns[data_type]:
        return None
    return column_name


class SqliteDataStore(DataStoreProtocol):
    def __init__(self):
        self.logger: FilteringBoundLogger = get_named_logger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            level=logging.INFO,
        )
        self.db_path = config.worker_v2_data_store_path

        self._init_tables()

    def _init_tables(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contexts (
            name TEXT PRIMARY KEY,
            data BLOB,
            data_hash INT,
            refresh_after INTEGER,
            last_refreshed_at INTEGER
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS discovery_entries (
            request_hash TEXT PRIMARY KEY,
            template TEXT,
            request TEXT,
            response TEXT,
            last_rendered_at INTEGER
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS worker_nodes (
            node_id TEXT PRIMARY KEY,
            last_heartbeat INTEGER
        )
        """)

        conn.commit()

    def _get_connection(self):
        # check_same_thread=False allows SQLite connections to be shared across threads
        # and means that we need to ensure thread safety ourselves.
        # isolation_level=None uses autocommit mode,
        # which prevents "cannot commit - no transaction is active" errors in multi-threaded contexts.
        conn = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None,
        )
        # configure the connection to return rows as sqlite3.Row objects,
        # allowing access to columns by name as well as by index.
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _row_to_object(data_type: DataType, row: sqlite3.Row) -> Any:
        match data_type:
            case DataType.Context:
                return Context(
                    name=row["name"],
                    data=pickle.loads(row["data"]),
                    data_hash=row["data_hash"],
                    last_refreshed_at=row["last_refreshed_at"],
                    refresh_after=row["refresh_after"],
                )
            case DataType.DiscoveryEntry:
                return DiscoveryEntry(
                    request_hash=row["request_hash"],
                    template=row["template"],
                    request=DiscoveryRequest.model_validate_json(row["request"]),
                    response=DiscoveryResponse.model_validate_json(row["response"])
                    if row["response"] is not None
                    else None,
                    last_rendered_at=row["last_rendered_at"],
                )
            case DataType.WorkerNode:
                return WorkerNode(
                    node_id=row["node_id"],
                    last_heartbeat=row["last_heartbeat"],
                )

    def _object_to_values(self, obj: Any) -> dict[str, Any]:
        if isinstance(obj, Context):
            try:
                pickled = pickle.dumps(obj.data)
            except TypeError as e:
                self.logger.error("Failed to pickle context data", name=obj.name)
                raise e

            return {
                "name": obj.name,
                "data": pickled,
                "data_hash": obj.data_hash,
                "last_refreshed_at": obj.last_refreshed_at,
                "refresh_after": obj.refresh_after,
            }
        if isinstance(obj, DiscoveryEntry):
            return {
                "request_hash": obj.request_hash,
                "template": obj.template,
                "request": obj.request.model_dump_json(),
                "response": obj.response.model_dump_json()
                if obj.response is not None
                else None,
                "last_rendered_at": obj.last_rendered_at,
            }
        if isinstance(obj, WorkerNode):
            return {
                "node_id": obj.node_id,
                "last_heartbeat": obj.last_heartbeat,
            }
        raise ValueError(f"Unsupported object type: {type(obj)}")

    def count_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> int:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot count matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return 0

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"SELECT COUNT(*) FROM {table} WHERE {column} {operator} ?"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (property_value,))
            row = cursor.fetchone()
            return row[0] if row else 0
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error counting matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=property_value,
            )
            return 0

    def delete_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> bool:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot delete matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return False

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"DELETE FROM {table} WHERE {column} {operator} ?"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (property_value,))
            conn.commit()
            return True
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error deleting matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=property_value,
            )
            return False

    def find_all_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> list[Any]:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot find all matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return []

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"SELECT * FROM {table} WHERE {column} {operator} ?"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (property_value,))
            return [self._row_to_object(data_type, row) for row in cursor.fetchall()]
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error finding matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=property_value,
            )
            return []

    def find_all_matching_property(
        self,
        data_type: DataType,
        match_property_name: str,
        comparison_operator: ComparisonOperator,
        match_property_value: Any,
        property_name: str,
    ) -> list[Any]:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot find property for all matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return []

        match_column = _validate_column(data_type, match_property_name)

        if match_column is None:
            self.logger.error(
                "Cannot find property for all matching, invalid column name",
                data_type=data_type,
                column=match_property_name,
            )
            return []

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"SELECT {column} FROM {table} WHERE {match_column} {operator} ?"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (match_property_value,))
            return [row[0] for row in cursor.fetchall()]
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error finding matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=match_property_value,
            )
            return []

    def get(self, data_type: DataType, key: str) -> Any | None:
        table = _get_table_name(data_type)
        primary_key_column = _get_primary_key(data_type)
        sql = f"SELECT * FROM {table} WHERE {primary_key_column} = ?"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (key,))
            row = cursor.fetchone()
            return self._row_to_object(data_type, row) if row else None
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error getting record",
                data_type=data_type,
                key=key,
            )
            return None

    def get_property(
        self, data_type: DataType, key: str, property_name: str,
    ) -> Any | None:
        table = _get_table_name(data_type)
        primary_key_column = _get_primary_key(data_type)
        property_column = _validate_column(data_type, property_name)

        if property_column is None:
            self.logger.error(
                "Cannot get property, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return None

        sql = f"SELECT {property_column} FROM {table} WHERE {primary_key_column} = ?"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (key,))
            row = cursor.fetchone()
            return row[0] if row else None
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error getting property",
                data_type=data_type,
                key=key,
                property=property_name,
            )
            return None

    def min_by_property(
        self,
        data_type: DataType,
        property_name: str,
    ) -> Any | None:
        table = _get_table_name(data_type)
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot get min of property, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return None

        sql = f"SELECT * FROM {table} ORDER BY {column} ASC LIMIT 1"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            return self._row_to_object(data_type, row) if row else None
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error getting min by property",
                data_type=data_type,
                property=property_name,
            )
            return None

    def set(self, data_type: DataType, key: str, value: Any) -> bool:
        table = _get_table_name(data_type)
        value_dict = self._object_to_values(value)

        columns = ", ".join(value_dict.keys())
        placeholders = ", ".join("?" for _ in value_dict)
        sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(value_dict.values()))
            conn.commit()
            if cursor.rowcount == 0:
                return False
            return True
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error saving record",
                data_type=data_type,
                key=key,
                values=value_dict,
            )
            return False

    def set_property(
        self, data_type: DataType, key: str, property_name: str, property_value: Any,
    ) -> bool:
        table = _get_table_name(data_type)
        primary_key_column = _get_primary_key(data_type)
        property_column = _validate_column(data_type, property_name)

        if property_column is None:
            self.logger.error(
                "Cannot set property, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return False

        sql = f"""
            INSERT INTO {table} ({primary_key_column}, {property_column})
            VALUES (?, ?)
            ON CONFLICT({primary_key_column}) DO UPDATE SET {property_column} = excluded.{property_column}
        """

        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (key, property_value))
            conn.commit()
            return True
        except (sqlite3.Error, ValueError):
            self.logger.exception(
                "Error setting property",
                data_type=data_type,
                key=key,
                property=property_name,
                value=property_value,
            )
            return False


class PostgresDataStore(DataStoreProtocol):
    def __init__(
        self,
        dsn: str | None = None,
        min_connections: int = 1,
        max_connections: int = 10,
    ):
        import psycopg2.pool

        self.logger: FilteringBoundLogger = get_named_logger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            level=logging.INFO,
        )

        self.dsn = dsn or config.worker_v2_data_store_path
        if not self.dsn:
            raise ValueError("PostgreSQL DSN must be provided via config")

        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_connections,
            maxconn=max_connections,
            dsn=self.dsn,
        )

        self._init_tables()

        self.logger.info("PostgreSQL data store initialized", dsn=self.dsn)

    def _get_connection(self):
        """Get a connection from the pool."""
        return self.pool.getconn()

    def _put_connection(self, conn):
        """Return a connection to the pool."""
        self.pool.putconn(conn)

    def _init_tables(self):
        """Initialize database tables if they don't exist."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS contexts (
                    name TEXT PRIMARY KEY,
                    data BYTEA,
                    data_hash INTEGER,
                    refresh_after INTEGER,
                    last_refreshed_at INTEGER
                )
                """)

                cursor.execute("""
                CREATE TABLE IF NOT EXISTS discovery_entries (
                    request_hash TEXT PRIMARY KEY,
                    template TEXT,
                    request TEXT,
                    response TEXT,
                    last_rendered_at INTEGER
                )
                """)

                cursor.execute("""
                CREATE TABLE IF NOT EXISTS worker_nodes (
                    node_id TEXT PRIMARY KEY,
                    last_heartbeat INTEGER
                )
                """)

                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_discovery_entries_template
                ON discovery_entries (template)
                """)

                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_worker_nodes_heartbeat
                ON worker_nodes (last_heartbeat)
                """)

                conn.commit()
        finally:
            self._put_connection(conn)

    def _row_to_object(
        self, data_type: DataType, row: tuple, columns: list[str],
    ) -> Any:
        """Convert a database row to a domain object."""
        row_dict = dict(zip(columns, row, strict=False))

        match data_type:
            case DataType.Context:
                return Context(
                    name=row_dict["name"],
                    data=pickle.loads(row_dict["data"]),
                    data_hash=row_dict["data_hash"],
                    last_refreshed_at=row_dict["last_refreshed_at"],
                    refresh_after=row_dict["refresh_after"],
                )
            case DataType.DiscoveryEntry:
                return DiscoveryEntry(
                    request_hash=row_dict["request_hash"],
                    template=row_dict["template"],
                    request=DiscoveryRequest.model_validate_json(row_dict["request"]),
                    response=DiscoveryResponse.model_validate_json(row_dict["response"])
                    if row_dict["response"] is not None
                    else None,
                    last_rendered_at=row_dict["last_rendered_at"],
                )
            case DataType.WorkerNode:
                return WorkerNode(
                    node_id=row_dict["node_id"],
                    last_heartbeat=row_dict["last_heartbeat"],
                )

    def _object_to_values(self, obj: Any) -> dict[str, Any]:
        """Convert a domain object to a dictionary for database storage."""
        if isinstance(obj, Context):
            try:
                pickled = pickle.dumps(obj.data)
            except TypeError:
                self.logger.exception("Failed to pickle context data", name=obj.name)
                raise

            return {
                "name": obj.name,
                "data": pickled,
                "data_hash": obj.data_hash,
                "last_refreshed_at": obj.last_refreshed_at,
                "refresh_after": obj.refresh_after,
            }
        if isinstance(obj, DiscoveryEntry):
            return {
                "request_hash": obj.request_hash,
                "template": obj.template,
                "request": obj.request.model_dump_json(),
                "response": obj.response.model_dump_json()
                if obj.response is not None
                else None,
                "last_rendered_at": obj.last_rendered_at,
            }
        if isinstance(obj, WorkerNode):
            return {
                "node_id": obj.node_id,
                "last_heartbeat": obj.last_heartbeat,
            }
        raise ValueError(f"Unsupported object type: {type(obj)}")

    def count_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> int:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot count matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return 0

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"SELECT COUNT(*) FROM {table} WHERE {column} {operator} %s"

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (property_value,))
                row = cursor.fetchone()
                return row[0] if row else 0
        except Exception:
            self.logger.exception(
                "Error counting matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=property_value,
            )
            return 0
        finally:
            self._put_connection(conn)

    def delete_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> bool:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot delete matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return False

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"DELETE FROM {table} WHERE {column} {operator} %s"

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (property_value,))
                conn.commit()
                return True
        except Exception:
            self.logger.exception(
                "Error deleting matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=property_value,
            )
            conn.rollback()
            return False
        finally:
            self._put_connection(conn)

    def find_all_matching(
        self,
        data_type: DataType,
        property_name: str,
        comparison_operator: ComparisonOperator,
        property_value: Any,
    ) -> list[Any]:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot find all matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return []

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"SELECT * FROM {table} WHERE {column} {operator} %s"

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (property_value,))
                columns = [desc[0] for desc in cursor.description]
                return [
                    self._row_to_object(data_type, row, columns)
                    for row in cursor.fetchall()
                ]
        except Exception:
            self.logger.exception(
                "Error finding matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=property_value,
            )
            return []
        finally:
            self._put_connection(conn)

    def find_all_matching_property(
        self,
        data_type: DataType,
        match_property_name: str,
        comparison_operator: ComparisonOperator,
        match_property_value: Any,
        property_name: str,
    ) -> list[Any]:
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot find property for all matching, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return []

        match_column = _validate_column(data_type, match_property_name)

        if match_column is None:
            self.logger.error(
                "Cannot find property for all matching, invalid column name",
                data_type=data_type,
                column=match_property_name,
            )
            return []

        operator = _get_operator_sql(comparison_operator)
        table = _get_table_name(data_type)
        sql = f"SELECT {column} FROM {table} WHERE {match_column} {operator} %s"

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (match_property_value,))
                return [row[0] for row in cursor.fetchall()]
        except Exception:
            self.logger.exception(
                "Error finding matching records",
                data_type=data_type,
                column=property_name,
                operator=comparison_operator,
                value=match_property_value,
            )
            return []
        finally:
            self._put_connection(conn)

    def get(self, data_type: DataType, key: str) -> Any | None:
        table = _get_table_name(data_type)
        primary_key_column = _get_primary_key(data_type)
        sql = f"SELECT * FROM {table} WHERE {primary_key_column} = %s"

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    return self._row_to_object(data_type, row, columns)
                return None
        except Exception:
            self.logger.exception(
                "Error getting record",
                data_type=data_type,
                key=key,
            )
            return None
        finally:
            self._put_connection(conn)

    def get_property(
        self, data_type: DataType, key: str, property_name: str,
    ) -> Any | None:
        table = _get_table_name(data_type)
        primary_key_column = _get_primary_key(data_type)
        property_column = _validate_column(data_type, property_name)

        if property_column is None:
            self.logger.error(
                "Cannot get property, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return None

        sql = f"SELECT {property_column} FROM {table} WHERE {primary_key_column} = %s"

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception:
            self.logger.exception(
                "Error getting property",
                data_type=data_type,
                key=key,
                property=property_name,
            )
            return None
        finally:
            self._put_connection(conn)

    def min_by_property(
        self,
        data_type: DataType,
        property_name: str,
    ) -> Any | None:
        table = _get_table_name(data_type)
        column = _validate_column(data_type, property_name)

        if column is None:
            self.logger.error(
                "Cannot get min of property, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return None

        sql = f"SELECT * FROM {table} ORDER BY {column} ASC LIMIT 1"

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    return self._row_to_object(data_type, row, columns)
                return None
        except Exception:
            self.logger.exception(
                "Error getting min by property",
                data_type=data_type,
                property=property_name,
            )
            return None
        finally:
            self._put_connection(conn)

    def set(self, data_type: DataType, key: str, value: Any) -> bool:
        table = _get_table_name(data_type)
        primary_key_column = _get_primary_key(data_type)
        value_dict = self._object_to_values(value)

        columns = ", ".join(value_dict)
        placeholders = ", ".join("%s" for _ in value_dict)
        updates = ", ".join(f"{col} = EXCLUDED.{col}" for col in value_dict)

        sql = f"""
            INSERT INTO {table} ({columns}) VALUES ({placeholders})
            ON CONFLICT ({primary_key_column}) DO UPDATE SET {updates}
        """

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(value_dict.values()))
                conn.commit()
                return True
        except Exception:
            self.logger.exception(
                "Error saving record",
                data_type=data_type,
                key=key,
            )
            conn.rollback()
            return False
        finally:
            self._put_connection(conn)

    def set_property(
        self, data_type: DataType, key: str, property_name: str, property_value: Any,
    ) -> bool:
        table = _get_table_name(data_type)
        primary_key_column = _get_primary_key(data_type)
        property_column = _validate_column(data_type, property_name)

        if property_column is None:
            self.logger.error(
                "Cannot set property, invalid column name",
                data_type=data_type,
                column=property_name,
            )
            return False

        sql = f"""
            INSERT INTO {table} ({primary_key_column}, {property_column})
            VALUES (%s, %s)
            ON CONFLICT ({primary_key_column})
            DO UPDATE SET {property_column} = EXCLUDED.{property_column}
        """

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (key, property_value))
                conn.commit()
                return True
        except Exception:
            self.logger.exception(
                "Error setting property",
                data_type=data_type,
                key=key,
                property=property_name,
                value=property_value,
            )
            conn.rollback()
            return False
        finally:
            self._put_connection(conn)
