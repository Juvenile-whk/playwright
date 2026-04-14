from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from playwright01.utils.logger import *


class DbQueries:
    """数据库增删改查通用工具类 (纯 SQL 拼接层)。

    依赖的 db_client 需实现:
        execute_query(sql: str, params: dict | None) -> list | None  (用于读)
        execute_update(sql: str, params: dict | None) -> int | None  (用于写)
    """

    _ALLOWED_JOIN_TYPES = {"INNER", "LEFT", "RIGHT", "FULL"}
    _COMPARE_OPS = {
        "$gt": ">",
        "$gte": ">=",
        "$lt": "<",
        "$lte": "<=",
        "$ne": "!=",
        "$like": "LIKE",
        "$not_like": "NOT LIKE",
    }

    def __init__(self, db_client: Any) -> None:
        self.db_client = db_client

    # ================================================================
    #  核心：读写执行分流 (对接 db_util)
    # ================================================================

    def _execute_read(self, sql: str, params: Optional[Dict[str, Any]]) -> list:
        """执行读操作（SELECT），调用 db_client.execute_query"""
        result = self.db_client.execute_query(sql, params)
        return result if result is not None else []

    def _execute_write(self, sql: str, params: Optional[Dict[str, Any]]) -> int:
        """执行写操作（INSERT/UPDATE/DELETE），调用 db_client.execute_update"""
        rowcount = self.db_client.execute_update(sql, params)
        return rowcount if rowcount is not None else 0

    # ================================================================
    #  查 询  (SELECT) -> 返回 list
    # ================================================================

    def select(
            self,
            table: str,
            select_fields: Optional[List[str]] = None,
            where_conditions: Optional[Dict[str, Any]] = None,
            order_by: Optional[str] = None,
            limit: Optional[int] = None,
    ) -> list:
        """单表查询"""
        self._validate_table(table)
        select_clause = self._build_select_clause(select_fields)
        where_sql, params = self._build_where_clause(where_conditions)
        order_sql = f" ORDER BY {order_by}" if order_by else ""
        limit_sql = self._build_limit_clause(limit)

        sql = f"SELECT {select_clause} FROM {table} {where_sql}{order_sql}{limit_sql}"
        return self._execute_read(sql, params)

    def join_query(
            self,
            main_table: str,
            joins: Optional[List[Dict[str, str]]] = None,
            select_fields: Optional[List[str]] = None,
            where_conditions: Optional[Dict[str, Any]] = None,
            order_by: Optional[str] = None,
            limit: Optional[int] = None,
    ) -> list:
        """多表 JOIN 查询"""
        self._validate_table(main_table)
        if joins is None:
            joins = []

        select_clause = self._build_select_clause(select_fields)
        join_sql = self._build_join_clause(joins)
        where_sql, params = self._build_where_clause(where_conditions)
        order_sql = f" ORDER BY {order_by}" if order_by else ""
        limit_sql = self._build_limit_clause(limit)

        sql = f"SELECT {select_clause} FROM {main_table} {join_sql}{where_sql}{order_sql}{limit_sql}"
        return self._execute_read(sql, params)

    def count(self, table: str, where_conditions: Optional[Dict[str, Any]] = None) -> int:
        """计数查询"""
        self._validate_table(table)
        where_sql, params = self._build_where_clause(where_conditions)
        sql = f"SELECT COUNT(*) AS cnt FROM {table}{where_sql}"
        rows = self._execute_read(sql, params)

        if not rows:
            return 0

        first_row = rows[0]

        # 1. 如果底层配置了返回字典
        if isinstance(first_row, dict):
            return int(first_row.get("cnt", 0))

        # 2. 无论是原生的 tuple/list，还是 SQLAlchemy 2.0 的 Row 对象，统统兼容！
        try:
            return int(first_row[0])
        except Exception:
            return 0

    def exists(self, table: str, where_conditions: Optional[Dict[str, Any]] = None) -> bool:
        """判断是否存在"""
        return self.count(table, where_conditions) > 0

    # ================================================================
    #  增  (INSERT) -> 返回 int (影响行数)
    # ================================================================

    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """单条插入"""
        self._validate_table(table)
        if not data or not isinstance(data, dict):
            raise ValueError("data must be a non-empty dict")

        columns, placeholders, params = self._build_insert_values(data)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return self._execute_write(sql, params)

    def batch_insert(self, table: str, data_list: List[Dict[str, Any]]) -> int:
        """批量插入"""
        self._validate_table(table)
        if not data_list or not isinstance(data_list, list):
            raise ValueError("data_list must be a non-empty list")

        base_columns = list(data_list[0].keys())
        all_params: Dict[str, Any] = {}
        value_groups: List[str] = []

        for row_idx, row in enumerate(data_list):
            if not isinstance(row, dict):
                raise ValueError(f"data_list[{row_idx}] must be a dict")
            phs: List[str] = []
            for col_idx, col in enumerate(base_columns):
                if col not in row:
                    raise ValueError(f"data_list[{row_idx}] missing column '{col}'")
                key = f"batch_{row_idx}_{col_idx}"
                phs.append(f":{key}")
                all_params[key] = row[col]
            value_groups.append(f"({', '.join(phs)})")

        columns_sql = ", ".join(base_columns)
        values_sql = ", ".join(value_groups)
        sql = f"INSERT INTO {table} ({columns_sql}) VALUES {values_sql}"
        return self._execute_write(sql, params)

    # ================================================================
    #  改  (UPDATE) -> 返回 int (影响行数)
    # ================================================================

    def update(
            self,
            table: str,
            data: Dict[str, Any],
            where_conditions: Optional[Dict[str, Any]] = None,
            *,
            unsafe_allow_full_table: bool = False,
    ) -> int:
        """更新数据"""
        self._validate_table(table)
        if not data or not isinstance(data, dict):
            raise ValueError("data must be a non-empty dict")

        set_sql, set_params = self._build_set_clause(data)
        where_sql, where_params = self._build_where_clause(where_conditions)

        if not where_sql and not unsafe_allow_full_table:
            raise ValueError("UPDATE without WHERE will affect all rows. Set unsafe_allow_full_table=True to proceed.")

        params = {**set_params, **where_params}
        sql = f"UPDATE {table} SET {set_sql}{where_sql}"
        return self._execute_write(sql, params)

    # ================================================================
    #  删  (DELETE) -> 返回 int (影响行数)
    # ================================================================

    def delete(
            self,
            table: str,
            where_conditions: Optional[Dict[str, Any]] = None,
            *,
            unsafe_allow_full_table: bool = False,
    ) -> int:
        """删除数据"""
        self._validate_table(table)
        where_sql, params = self._build_where_clause(where_conditions)

        if not where_sql and not unsafe_allow_full_table:
            raise ValueError("DELETE without WHERE will delete all rows. Set unsafe_allow_full_table=True to proceed.")

        sql = f"DELETE FROM {table}{where_sql}"
        return self._execute_write(sql, params)

    # ================================================================
    #  内部 SQL 拼接方法 (不动底层，只造字符串)
    # ================================================================

    def _validate_table(self, table: str) -> None:
        if not isinstance(table, str) or not table.strip():
            raise ValueError("table must be a non-empty string")

    def _build_select_clause(self, select_fields: Optional[List[str]]) -> str:
        if not select_fields:
            return "*"
        cleaned = [f.strip() for f in select_fields if isinstance(f, str) and f.strip()]
        return ", ".join(cleaned)

    def _build_join_clause(self, joins: List[Dict[str, str]]) -> str:
        parts = []
        for cfg in joins:
            jtype = str(cfg.get("type", "INNER")).upper().strip()
            table = str(cfg.get("table", "")).strip()
            on = str(cfg.get("on", "")).strip()
            if jtype not in self._ALLOWED_JOIN_TYPES or not table or not on:
                raise ValueError(f"invalid join config: {cfg!r}")
            parts.append(f"{jtype} JOIN {table} ON {on}")
        return (" " + " ".join(parts)) if parts else ""

    def _build_where_clause(self, where_conditions: Optional[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        if not where_conditions or not isinstance(where_conditions, dict):
            return "", {}

        params, parts = {}, []
        for idx, (field, value) in enumerate(where_conditions.items()):
            field = field.strip()

            if isinstance(value, list):
                if not value: continue
                phs = [f":in_{idx}_{j}" for j in range(len(value))]
                for j, v in enumerate(value): params[f"in_{idx}_{j}"] = v
                parts.append(f"{field} IN ({', '.join(phs)})")

            elif isinstance(value, dict):
                for op_idx, (op_key, op_val) in enumerate(value.items()):
                    if op_key == "$null":
                        parts.append(f"{field} IS NULL" if op_val is True else f"{field} IS NOT NULL")
                    elif op_key == "$between":
                        if not isinstance(op_val, (list, tuple)) or len(op_val) != 2:
                            raise ValueError("$between value must be a [low, high] list/tuple")
                        parts.append(f"{field} BETWEEN :btw_{idx}_lo AND :btw_{idx}_hi")
                        params[f"btw_{idx}_lo"], params[f"btw_{idx}_hi"] = op_val[0], op_val[1]
                    elif op_key in self._COMPARE_OPS:
                        key = f"cond_{idx}_{op_idx}"
                        parts.append(f"{field} {self._COMPARE_OPS[op_key]} :{key}")
                        params[key] = op_val
                    else:
                        raise ValueError(f"unsupported operator: {op_key}")
            else:
                params[f"eq_{idx}"] = value
                parts.append(f"{field} = :eq_{idx}")

        return (f" WHERE {' AND '.join(parts)}" if parts else ""), params

    def _build_set_clause(self, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        parts, params = [], {}
        for idx, (col, val) in enumerate(data.items()):
            params[f"set_{idx}"] = val
            parts.append(f"{col.strip()} = :set_{idx}")
        return ", ".join(parts), params

    def _build_insert_values(self, data: Dict[str, Any], row_prefix: str = "v") -> Tuple[str, str, Dict[str, Any]]:
        columns, placeholders, params = [], [], {}
        for idx, (col, val) in enumerate(data.items()):
            params[f"{row_prefix}_{idx}"] = val
            columns.append(col.strip())
            placeholders.append(f":{row_prefix}_{idx}")
        return ", ".join(columns), ", ".join(placeholders), params

    def _build_limit_clause(self, limit: Optional[int]) -> str:
        if limit is None or not isinstance(limit, int) or limit <= 0:
            return ""
        return f" LIMIT {limit}"
