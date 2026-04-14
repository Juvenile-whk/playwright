from playwright01.module import *


class LoginPage(PageObject):
    def __init__(self, page):
        super().__init__(page)
        self.url = "/signin"
        self.user = self.page.get_by_placeholder("用户名/邮箱/手机号")
        self.pwd = self.page.get_by_placeholder("密码")
        self.state = self.page.locator('//span[@class="anticon anticon-bell"]').first


    def login(self, user, pwd):
        self.navigate()
        self.user.fill(user)
        self.pwd.fill(pwd)
        self.click_button("登录")