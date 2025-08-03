import logging
import re
import psycopg
from psycopg.rows import dict_row
from typing import List, Dict, Optional, Union, Tuple, Any
from contextlib import contextmanager
from config.env_config import get_environment_variables

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) 

class RawSQL:
    def __init__(self, sql_expression: str):
        self.sql_expression = sql_expression

    def __str__(self):
        return self.sql_expression

class PostgreSQLDB:

    __ALLOWED_OPERATORS = {"=", "<", "<=", ">", ">=", "!=", "IN", "NOT IN", "BETWEEN", "IS NULL", "IS NOT NULL", "LIKE", "ILIKE"}
    __ALLOWED_ORDER = {"ASC", "DESC"}

    def __init__(self) -> None:
        """
        Clase para gestionar conexiones y consultas seguras a una base de datos PostgreSQL.
        """
        env = get_environment_variables()

        self._db_name = env.DB_NAME
        self._ip = env.DB_IP
        self._port = env.DB_PORT
        self._username = env.DB_USERNAME
        self._password = env.DB_PASSWORD
        self.__allowed_tables = set()
        self.__allowed_fields = set()
        self.__load_allowed_tables_and_fields()

    def get_ip(self) -> str:
        return self._ip

    def set_ip(self, ip: str) -> None:
        self._ip = ip

    def get_port(self) -> str:
        return self._port

    def set_port(self, port: str) -> None:
        self._port = port

    def get_username(self) -> str:
        return self._username

    def set_username(self, username: str) -> None:
        self._username = username

    def get_password(self) -> str:
        return self._password

    def set_password(self, password: str) -> None:
        self._password = password

    def get_db_name(self) -> str:
        return self._db_name

    def set_db_name(self, db_name: str) -> None:
        self._db_name = db_name

    def __load_allowed_tables_and_fields(self, schema: str = "public") -> None:
        """
        Carga en memoria las tablas y campos permitidos desde la base de datos.
        """
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE';
            """, (schema,))
            tables = [row['table_name'] for row in cur.fetchall()]
            self.__allowed_tables = set(tables)

            fields = set()
            for table in tables:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s;
                """, (schema, table))
                cols = [row['column_name'] for row in cur.fetchall()]
                fields.update(cols)
            self.__allowed_fields = fields

    @contextmanager
    def connection(self):
        """
        Context manager para manejar la conexión a la base de datos.
        """
        conn = psycopg.connect(
            user=self.get_username(),
            password=self.get_password(),
            host=self.get_ip(),
            port=self.get_port(),
            dbname=self.get_db_name(),
            row_factory=dict_row
        )
        try:
            yield conn
        except Exception as e:
            logger.error(f"Error en conexión: {e}")
            raise
        finally:
            conn.close()

    def __validate_table(self, table_name: str) -> None:
        """
        Valida que la tabla esté en la lista de tablas permitidas.
        Permite alias en la forma 'table AS t' o 'table t'.
        """
        parts = table_name.strip().split()
        base_table = parts[0]
        
        if base_table not in self.__allowed_tables:
            raise ValueError(f"Tabla no permitida: {base_table}")
            
        if len(parts) > 1:
            alias = parts[-1] if len(parts) == 2 else parts[2] if len(parts) == 3 and parts[1].upper() == "AS" else None
            if alias:
                self.__validate_alias_name(alias)

    def __validate_field(self, field: str) -> None:
        """
        Valida que un campo esté permitido.
        Soporta campos con alias tipo 'alias.field'.
        """
        if isinstance(field, RawSQL):
            return
        parts = field.split('.')
        base_field = parts[-1]
        
        if len(parts) > 1:  # Tiene alias
            self.__validate_alias_name(parts[0])
            
        if base_field not in self.__allowed_fields:
            raise ValueError(f"Campo no permitido: {base_field}")

    def __validate_fields(self, fields: List[str]) -> None:
        for f in fields:
            self.__validate_field(f)

    def __validate_operator(self, operator: str) -> None:
        if operator.upper() not in self.__ALLOWED_OPERATORS:
            raise ValueError(f"Operador no permitido: {operator}")

    def __validate_order_direction(self, direction: str) -> None:
        if direction.upper() not in self.__ALLOWED_ORDER:
            raise ValueError(f"Dirección ORDER BY no permitida: {direction}")

    def __validate_alias_name(self, alias: str) -> None:
        """
        Valida que el alias sea seguro: solo letras, números y _ permitidos.
        """
        if not re.match(r'^[A-Za-z0-9_]+$', alias):
            raise ValueError(f"Alias no válido o potencialmente peligroso: {alias}")

    def __validate_join_on(self, on_clause: str) -> None:
        """
        Valida la cláusula ON de un JOIN para evitar inyección.
        Ahora soporta múltiples operadores.
        """
        operators_pattern = '|'.join(map(re.escape, self.__ALLOWED_OPERATORS))
        pattern = rf'^\s*([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\s*({operators_pattern})\s*([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\s*$'
        if not re.match(pattern, on_clause):
            raise ValueError(f"Cláusula ON no permitida o insegura: {on_clause}")
        
        parts = re.split(rf'\s*({operators_pattern})\s*', on_clause)
        for part in [parts[0], parts[2]]:
            alias = part.split('.')[0]
            self.__validate_alias_name(alias)

    def __execute_query(
        self,
        query: str,
        values: Optional[List[Any]] = None,
        fetch: bool = False
    ) -> Union[bool, List[Dict[str, Any]], Tuple[bool, int]]:
        """
        Ejecuta una consulta SQL parametrizada.
        """
        try:
            with self.connection() as client:
                with client.cursor() as cursor:
                    cursor.execute(query, values or [])
                    if fetch:
                        results = cursor.fetchall()
                        return results
                    rows_affected = cursor.rowcount
                    client.commit()
                    return True, rows_affected
        except Exception as e:
            # logger.error(f"Error ejecutando consulta: {e}")
            return False, 0 if not fetch else []

    def __build_where_clause(self, conditions: List[Dict], values: list) -> str:
        """
        Construye la cláusula WHERE con soporte para múltiples operadores.
        """
        where_clauses = []
        for cond in conditions:
            field = cond['field']
            operator = cond['operator'].upper()
            value = cond.get('value')
            
            self.__validate_field(field)
            self.__validate_operator(operator)
            
            if operator in ('IS NULL', 'IS NOT NULL'):
                where_clauses.append(f"{field} {operator}")
            elif operator in ('IN', 'NOT IN'):
                if not isinstance(value, (list, tuple)):
                    raise ValueError(f"El valor para {operator} debe ser una lista")
                placeholders = ', '.join(['%s'] * len(value))
                where_clauses.append(f"{field} {operator} ({placeholders})")
                values.extend(value)
            elif operator == 'BETWEEN':
                if not isinstance(value, (list, tuple)) or len(value) != 2:
                    raise ValueError("BETWEEN requiere una lista/tupla con 2 valores")
                where_clauses.append(f"{field} BETWEEN %s AND %s")
                values.extend([value[0], value[1]])
            else:
                where_clauses.append(f"{field} {operator} %s")
                values.append(value)
                
        return " AND ".join(where_clauses)

    def select(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.__validate_table(params["table"])

        fields = params.get("fields")
        fields_clause = "*"
        if fields:
            self.__validate_fields(fields)
            fields_clause = ", ".join(str(f) if isinstance(f, RawSQL) else f for f in fields)

        query = f"SELECT {fields_clause} FROM {params['table']}"
        values = []

        for join in params.get('joins', []):
            self.__validate_table(join['table'])
            self.__validate_join_on(join['on'])
            query += f" {join['type']} JOIN {join['table']} ON {join['on']}"

        where_conditions = params.get('filters', {}).get('where', [])
        if where_conditions:
            if isinstance(where_conditions, dict):
                where_conditions = [where_conditions]
                
            where_clause = self.__build_where_clause(where_conditions, values)
            query += f" WHERE {where_clause}"

        group_by = params.get('filters', {}).get('group_by', [])
        if group_by:
            self.__validate_fields(group_by)
            query += " GROUP BY " + ", ".join(group_by)

        order_by = params.get('filters', {}).get('order_by', [])
        if order_by:
            order_clauses = []
            for ob in order_by:
                self.__validate_field(ob['field'])
                self.__validate_order_direction(ob['direction'])
                order_clauses.append(f"{ob['field']} {ob['direction'].upper()}")
            query += " ORDER BY " + ", ".join(order_clauses)

        limit = params.get('filters', {}).get('limit')
        if limit is not None:
            if not (isinstance(limit, int) and limit > 0):
                raise ValueError("LIMIT debe ser un entero positivo")
            query += f" LIMIT {limit}"

        return self.__execute_query(query, values, fetch=True)

    def insert(self, params: Dict[str, Any]) -> Tuple[bool, int]:
        self.__validate_table(params["table"])
        self.__validate_fields(list(params["values"].keys()))

        table = params["table"]
        values_dict = params["values"]

        columns = []
        placeholders = []
        values = []

        for col, val in values_dict.items():
            columns.append(col)
            if isinstance(val, RawSQL):
                placeholders.append(str(val))
            else:
                placeholders.append("%s")
                values.append(val)

        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        return self.__execute_query(query, values, fetch=False)

    def update(self, params: Dict[str, Any]) -> Tuple[bool, int]:
        self.__validate_table(params["table"])
        self.__validate_fields(list(params["values"].keys()))

        table = params["table"]
        set_values = params["values"]
        filters = params.get("filters", {}).get("where", [])

        set_clause = ", ".join([f"{k} = %s" for k in set_values.keys()])
        set_values_list = list(set_values.values())

        where_clause = ""
        where_values = []
        if filters:
            if isinstance(filters, dict):
                filters = [filters]
            where_clause = " WHERE " + self.__build_where_clause(filters, where_values)

        query = f"UPDATE {table} SET {set_clause}{where_clause}"
        return self.__execute_query(query, set_values_list + where_values, fetch=False)

    def delete(self, params: Dict[str, Any]) -> Tuple[bool, int]:
        self.__validate_table(params["table"])

        filters = params.get("filters", {}).get("where", [])

        where_clause = ""
        where_values = []
        if filters:
            if isinstance(filters, dict):
                filters = [filters]
            where_clause = " WHERE " + self.__build_where_clause(filters, where_values)

        query = f"DELETE FROM {params['table']}{where_clause}"
        return self.__execute_query(query, where_values, fetch=False)
    
# # Ejemplo de uso:
# params = {
#     "table": "cv_backend_inferences AS inf",  
#     "fields": [
#         "loc.cvb_loc_name", 
#         "inf.cvb_inf_timestamp",
#         "inf.cvb_inf_inference_result",
#     ],
#     "joins": [
#         {
#             "type": "INNER",
#             "table": "cv_backend_locations AS loc",
#             "on": "loc.cvb_loc_process_id = inf.cvb_inf_process_id"
#         }
#     ],
#     "filters": {
#         "where": [
#             {"field": "inf.cvb_inf_process_id", "operator": ">=", "value": 2},
#             {"field": "inf.cvb_inf_timestamp", "operator": "<=", "value": "2025-02-20 17:01:28.569474+00"}
#         ],
#         "order_by": [{"field": "inf.cvb_inf_process_id", "direction": "DESC"}],
#         "limit": 10
#     }
# }

# pg = PostgreSQLDB()
# results = pg.select(params)
# for row in results:
#     print(row)