from playwright01.utils.db_util import DatabaseUtil
from playwright01.utils.db_queries import DbQueries


def test_a():

    # 1. 建立 MySQL 连接
    db_client = DatabaseUtil(
        username="root",
        password="123456", # 改成你的密码
        host="127.0.0.1",
        port="3306",
        database="fba"        # 改成你存放上述表的数据库名
    )

    q = DbQueries(db_client)

    print("\n" + "="*50)
    print("1. 测试 INSERT (单条插入)")
    print("="*50)
    q.insert(
        table="users",
        data={"username": "测试用户A", "age": 25, "status": "ACTIVE", "dept_id": 1}
    )

    print("\n" + "="*50)
    print("2. 测试 SELECT (等值、比较、IN、LIKE、IS NULL)")
    print("="*50)
    users = q.select(
        table="users",
        where_conditions={
            "status": "ACTIVE",                      # 等值
            "age": {"$gte": 25, "$lt": 40},         # 比较操作符
            "username": {"$like": "%张%"},           # 模糊查询
            "dept_id": {"$null": False}              # 不为空
        }
    )
    for u in users: print(u)

    print("\n" + "="*50)
    print("3. 测试 COUNT 和 EXISTS")
    print("="*50)
    active_count = q.count("users", {"status": "ACTIVE"})
    print(f"活跃用户数: {active_count}")
    print(f"是否存在研发部: {q.exists('departments', {'dept_name': '研发部'})}")

    print("\n" + "="*50)
    print("4. 测试 LEFT JOIN (用户左连部门，王五没部门也会出来)")
    print("="*50)
    left_join_res = q.join_query(
        main_table="users u",
        joins=[
            {"type": "LEFT", "table": "departments d", "on": "u.dept_id = d.id"}
        ],
        select_fields=["u.username", "d.dept_name"],
        order_by="u.id ASC"
    )
    for row in left_join_res: print(row) # 期望看到王五的 dept_name 是 None

    print("\n" + "="*50)
    print("5. 测试 RIGHT JOIN (用户右连日志，系统日志也会出来)")
    print("="*50)
    right_join_res = q.join_query(
        main_table="users u",
        joins=[
            {"type": "RIGHT", "table": "system_logs sl", "on": "u.id = sl.user_id"}
        ],
        select_fields=["u.username", "sl.action", "sl.log_time"],
        order_by="sl.log_time ASC"
    )
    for row in right_join_res: print(row) # 期望看到最后一行用户名为 None，动作为"执行全库备份"

    print("\n" + "="*50)
    print("6. 测试 INNER JOIN + WHERE 过滤软删除 (只查有效订单)")
    print("="*50)
    inner_join_res = q.join_query(
        main_table="orders o",
        joins=[
            {"type": "INNER", "table": "users u", "on": "o.user_id = u.id"}
        ],
        select_fields=["u.username", "o.product_name", "o.amount"],
        where_conditions={
            "o.deleted_at": {"$null": True},         # 未被软删除
            "o.status": ["PAID", "SHIPPED"],         # IN 查询
            "o.amount": {"$gte": 1000}               # 金额大于 1000
        }
    )
    for row in inner_join_res: print(row) # 期望只有张三的两笔订单

    print("\n" + "="*50)
    print("7. 测试 UPDATE (软删除测试用户A)")
    print("="*50)
    q.update(
        table="users",
        data={"status": "DELETED", "deleted_at": "2024-10-11 10:00:00"},
        where_conditions={"username": "测试用户A"}
    )

    print("\n" + "="*50)
    print("8. 测试 DELETE (物理删除测试用户A)")
    print("="*50)
    # 因为加了防误删机制，必须传 unsafe_allow_full_table=True 才能不带 WHERE
    # 这里我们带上 WHERE
    q.delete(
        table="users",
        where_conditions={"username": "测试用户A"}
    )

    db_client.close()
    print("\n✅ 所有测试用例执行完毕！")
