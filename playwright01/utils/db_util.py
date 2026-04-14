from urllib.parse import quote_plus  # 防止密码含特殊字符
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from playwright01.utils.logger import *

# 尝试导入驱动
try:
    import oracledb

    ORACLEDB_AVAILABLE = True
except ImportError:
    ORACLEDB_AVAILABLE = False

try:
    import cx_Oracle

    CX_ORACLE_AVAILABLE = True
except ImportError:
    CX_ORACLE_AVAILABLE = False

try:
    import pymysql

    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False


class DatabaseUtil:
    def __init__(
            self,
            username: str,
            password: str,
            host: str,
            port: str,
            service_name: str = None,  # Oracle 专属
            sid: str = None,  # Oracle 专属
            database: str = None,  # MySQL 专属
            db_type: str = "auto",  # "auto", "oracle", "mysql"
            mode: str = "auto",  # Oracle 专属: "thin", "thick", "cx_oracle", "auto"
            lib_dir: str = None,  # Oracle 专属: Instant Client 路径
    ):
        """
        统一数据库连接工具类 (Oracle / MySQL)

        :param db_type: "auto"(自动识别), "oracle", "mysql"
        """
        if not all([username, password, host, port]):
            raise ValueError("数据库连接基础参数不能为空")

        self.username = username
        self.password = password
        self.host = host
        self.port = str(port)
        self.service_name = service_name
        self.sid = sid
        self.database = database
        self.lib_dir = lib_dir
        self.mode = mode.lower()
        self.db_type = db_type.lower()

        # 安全编码密码
        self.safe_password = quote_plus(password)

        # 自动识别数据库类型
        self._auto_detect_db_type()

        # 初始化引擎
        self.engine = None
        self._init_engine()

        # 创建 Session 工厂
        self.Session = sessionmaker(bind=self.engine)

    def _auto_detect_db_type(self):
        """根据传入的参数自动推断数据库类型"""
        if self.db_type != "auto":
            return

        if self.database and not self.service_name and not self.sid:
            self.db_type = "mysql"
        elif (self.service_name or self.sid) and not self.database:
            self.db_type = "oracle"
        else:
            raise ValueError(
                "Auto 模式无法识别数据库类型：请明确传入 MySQL 的 'database' 或 Oracle 的 'service_name'/'sid'"
            )

    def _init_engine(self):
        """根据 db_type 分发到不同的引擎初始化逻辑"""
        if self.db_type == "mysql":
            self._init_mysql_engine()
        elif self.db_type == "oracle":
            self._init_oracle_engine()
        else:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")

    def _init_mysql_engine(self):
        """初始化 MySQL 引擎"""
        if not PYMYSQL_AVAILABLE:
            raise RuntimeError("连接 MySQL 需要 pymysql 驱动，请运行: pip install pymysql")
        if not self.database:
            raise ValueError("MySQL 连接必须提供 'database' 参数")

        database_url = f"mysql+pymysql://{self.username}:{self.safe_password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4"

        try:
            self.engine = create_engine(
                database_url,
                pool_size=2,
                max_overflow=3,
                pool_recycle=3600,  # MySQL 防止断开连接的重要参数
                echo=False
            )
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ 使用 MySQL (pymysql) 连接成功")
        except Exception as e:
            raise RuntimeError(f"MySQL 引擎初始化/连接失败: {e}")

    def _init_oracle_engine(self):
        """初始化 Oracle 引擎 (兼容原有所有逻辑)"""
        if not (self.service_name or self.sid):
            raise ValueError("Oracle 连接必须提供 'service_name' 或 'sid'")

        # 构造 DSN
        if self.service_name:
            dsn_str = f"{self.host}:{self.port}/{self.service_name}"
        else:
            dsn_str = f"{self.host}:{self.port}:{self.sid}"

        url_base = f"{self.username}:{self.safe_password}@{dsn_str}"

        if self.mode == "thin":
            if not ORACLEDB_AVAILABLE:
                raise RuntimeError("oracledb 未安装，请运行: pip install oracledb")
            database_url = f"oracle+oracledb://{url_base}"
            self._create_oracle_engine(database_url, "oracledb Thin")

        elif self.mode == "thick":
            if not ORACLEDB_AVAILABLE:
                raise RuntimeError("oracledb 未安装")
            try:
                oracledb.init_oracle_client(lib_dir=self.lib_dir)
            except Exception as e:
                raise RuntimeError(f"init_oracle_client 失败: {e}")
            self._create_oracle_engine(f"oracle+oracledb://{url_base}", "oracledb Thick")

        elif self.mode == "cx_oracle":
            if not CX_ORACLE_AVAILABLE:
                raise RuntimeError("cx_Oracle 未安装")
            try:
                cx_Oracle.init_oracle_client(lib_dir=self.lib_dir)
            except Exception as e:
                raise RuntimeError(f"init_oracle_client 失败: {e}")
            self._create_oracle_engine(f"oracle+cx_oracle://{url_base}", "cx_Oracle")

        elif self.mode == "auto":
            errors = []
            # 1. 尝试 Thin
            if ORACLEDB_AVAILABLE:
                try:
                    self._create_oracle_engine(f"oracle+oracledb://{url_base}", "oracledb Thin")
                    return
                except Exception as e:
                    errors.append(f"Thin: {e}")
            # 2. 尝试 Thick
            if ORACLEDB_AVAILABLE:
                try:
                    oracledb.init_oracle_client(lib_dir=self.lib_dir)
                    self._create_oracle_engine(f"oracle+oracledb://{url_base}", "oracledb Thick")
                    return
                except Exception as e:
                    errors.append(f"Thick: {e}")
            # 3. 尝试 cx_Oracle
            if CX_ORACLE_AVAILABLE:
                try:
                    cx_Oracle.init_oracle_client(lib_dir=self.lib_dir)
                    self._create_oracle_engine(f"oracle+cx_oracle://{url_base}", "cx_Oracle")
                    return
                except Exception as e:
                    errors.append(f"cx_Oracle: {e}")
            raise RuntimeError("所有 Oracle 驱动模式均失败:\n" + "\n".join(errors))
        else:
            raise ValueError("mode 必须是 'thin', 'thick', 'cx_oracle' 或 'auto'")

    def _create_oracle_engine(self, database_url: str, mode_name: str):
        """内部方法：创建 Oracle 引擎并测试连接"""
        self.engine = create_engine(database_url, pool_size=1, max_overflow=0, echo=False)
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM DUAL"))
        print(f"✅ 使用 {mode_name} 模式连接成功")

    # ================================================================
    #  以下核心执行方法无需修改，SQLAlchemy 天然屏蔽了数据库差异
    # ================================================================

    def execute_query(self, query, params=None):
        """执行查询 (返回 list)"""
        try:
            with self.engine.connect() as connection:
                query_obj = text(query) if isinstance(query, str) else query
                debug_sql = _render_sql_for_logging(query_obj, params)
                logger.info(f"[QUERY] {debug_sql}")

                result = connection.execute(query_obj, params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            error_msg = f"查询失败: {e}"
            print(error_msg)
            logger.error(error_msg)
            return None

    def execute_update(self, query, params=None):
        """执行更新/插入/删除 (返回影响行数)"""
        try:
            with self.engine.connect() as connection:
                query_obj = text(query) if isinstance(query, str) else query
                debug_sql = _render_sql_for_logging(query_obj, params)
                logger.info(f"[UPDATE] {debug_sql}")

                result = connection.execute(query_obj, params or {})
                connection.commit()
                return result.rowcount
        except SQLAlchemyError as e:
            error_msg = f"执行失败: {e}"
            print(error_msg)
            logger.error(error_msg)
            return None

    def get_session(self):
        """获取 ORM Session"""
        return self.Session()

    def close(self):
        """关闭连接池"""
        if self.engine:
            self.engine.dispose()


def _render_sql_for_logging(query_obj, params):
    """安全地将 SQL 渲染为可读字符串（仅用于日志）"""
    sql_str = str(query_obj)
    if not params or not isinstance(params, dict):
        return sql_str

    sorted_params = sorted(params.items(), key=lambda x: -len(x[0]))
    rendered = sql_str

    for key, val in sorted_params:
        placeholder = f":{key}"
        if placeholder not in rendered:
            continue

        if val is None:
            replacement = "NULL"
        elif isinstance(val, str):
            safe_val = val.replace("'", "''")
            replacement = f"'{safe_val}'"
        elif isinstance(val, (int, float)):
            replacement = str(val)
        else:
            safe_val = str(val).replace("'", "''")
            replacement = f"'{safe_val}'"

        rendered = rendered.replace(placeholder, replacement, 1)

    return rendered


# ================================================================
#  使用示例
# ================================================================

# 1. 原来的 Oracle 写法（完全兼容，一行都不用改，自动识别为 Oracle）
"""
db_oracle = DatabaseUtil(
    username="user",
    password="pwd",
    host="192.168.1.100",
    port="1521",
    service_name="ORCL",
    lib_dir=r'C:\ORACLE\instantclient_11_2'
)
"""

# 2. 新增的 MySQL 写法（自动识别为 MySQL）
"""
db_mysql = DatabaseUtil(
    username="root",
    password="123456",
    host="127.0.0.1",
    port="3306",
    database="test_db"    # 传了 database 就会自动走 MySQL
)
"""

# 3. 强行指定类型（不怕参数冲突）

db_mysql = DatabaseUtil(
    db_type="mysql",      # 强制 MySQL
    username="root",
    password="123456",
    host="127.0.0.1",
    port="3306",
    database="fba"
)


# 4. 和之前的 DbQueries 完美配合（无需任何修改）
# from playwright01.utils.db_queries import DbQueries
# q = DbQueries(db_mysql)
# q.select("users", where_conditions={"status": "ACTIVE"})
