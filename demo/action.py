import threading
from abc import (
    ABC,
    abstractproperty,
    abstractclassmethod,
)

from cloudpickle import (
    loads,
)

from demo.utils import (
    make_flight_result,
)


class AbstractAction(ABC):
    @abstractclassmethod
    @abstractproperty
    def name(cls):
        pass

    @abstractclassmethod
    @abstractproperty
    def description(cls):
        pass

    @abstractclassmethod
    def do_action(cls, server, context, action):
        pass


class HealthCheckAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "healthcheck"

    @classmethod
    @property
    def description(cls):
        return "NOP: check that communication is established"

    @classmethod
    def do_action(cls, server, context, action):
        yield make_flight_result(None)


class ClearAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "clear"

    @classmethod
    @property
    def description(cls):
        return "Clear the stored flights."

    @classmethod
    def do_action(cls, server, context, action):
        raise NotImplementedError(f"{action.type} is not implemented.")


class ShutdownAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "shutdown"

    @classmethod
    @property
    def description(cls):
        return "Shut down this server."

    @classmethod
    def do_action(cls, server, context, action):
        yield make_flight_result("Shutdown!")
        # Shut down on background thread to avoid blocking current request
        threading.Thread(target=server._shutdown).start()


class ListExchangesAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "list-exchanges"

    @classmethod
    @property
    def description(cls):
        return "Get a list of all exchange commands available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        yield make_flight_result(
            tuple(exchanger.command for exchanger in server.exchangers.values())
        )


class AddActionAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "add-action"

    @classmethod
    @property
    def description(cls):
        return "Add an action to the server's repertoire of actions"

    @classmethod
    def do_action(cls, server, context, action):
        action_class = loads(action.body)
        server.actions[action_class.name] = action_class
        yield make_flight_result(None)


class AddExchangeAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "add-exchange"

    @classmethod
    @property
    def description(cls):
        return "Add an exchange to the server's repertoire of exchanges"

    @classmethod
    def do_action(cls, server, context, action):
        exchange_class = loads(action.body)
        server.exchangers[exchange_class.command] = exchange_class
        yield make_flight_result(None)


class QueryExchangeAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "query-exchange"

    @classmethod
    @property
    def description(cls):
        return "Get metadata about a particular exchange available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        exchange_name = loads(action.body)
        exchanger = server.exchangers.get(exchange_name)
        query_result = exchanger.query_result if exchanger else None
        yield make_flight_result(query_result)


class ListTablesAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "list_tables"

    @classmethod
    @property
    def description(cls):
        return "Get the names of all tables available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        tables = server._conn.tables
        yield make_flight_result(tuple(tables))


class TableInfoAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "table_info"

    @classmethod
    @property
    def description(cls):
        return "Get info about a particular table available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        table_name = action.body.to_pybytes().decode("utf-8")
        schema = server._conn.get_schema(table_name)
        yield make_flight_result(schema)


class DropTableAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "drop_table"

    @classmethod
    @property
    def description(cls):
        return "Drop a table on this server."

    @classmethod
    def do_action(cls, server, context, action):
        table_name = loads(action.body)
        server._conn.execute(table_name)
        yield make_flight_result(f"dropped table {table_name}")


class DropViewAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "drop_view"

    @classmethod
    @property
    def description(cls):
        return "Drop a view on this server."

    @classmethod
    def do_action(cls, server, context, action):
        table_name = loads(action.body)
        server._conn.drop_view(table_name)
        yield make_flight_result(f"dropped view {table_name}")


class ReadParquetAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "read_parquet"

    @classmethod
    @property
    def description(cls):
        return "Read parquet files into this server."

    @classmethod
    def do_action(cls, server, context, action):
        args = loads(action.body)

        table_name = args["table_name"]
        source_list = args["source_list"]

        server._conn.read_parquet(source_list, table_name)
        yield make_flight_result(f"read parquet file {table_name}")


actions = {
    action.name: action
    for action in (
        HealthCheckAction,
        ClearAction,
        ShutdownAction,
        ListExchangesAction,
        QueryExchangeAction,
        AddActionAction,
        AddExchangeAction,
        ListTablesAction,
        TableInfoAction,
        DropTableAction,
        DropViewAction,
        ReadParquetAction,
    )
}
