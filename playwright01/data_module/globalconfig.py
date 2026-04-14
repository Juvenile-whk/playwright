
# 调试和报告配置
DEBUG_MODE = False
# 测试用例失败时是否创建bug
ENABLE_BUG_CREATION = False
# 测试用例执行完是否生成allure报告
ENABLE_ALLURE_REPORT = False


BUG_URL = 'https://www.cat2bug.com:8022'
BASE_URL = 'https://www.cat2bug.com:8022/prod-api'
USER = 'xxx'
PWD = 'xxx123456'





# 根据失败用例的文件名提取模块关键字，并查找配置中的负责人ID
BUG_ASSIGNMENT_RULES = {
    'test_用户管理.py': 'DEV001',
    'test_订单流程_支付功能测试.py': 'DEV002',
    'test_询报价流程_无PN申请转询价.py': 'DEV003'
}

DEFAULT_ASSIGNEE = '454'