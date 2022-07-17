from matplotlib.pyplot import switch_backend
from selenium.webdriver.remote.webdriver import WebDriver as wd
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as wdw
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains as AC
import selenium
from bs4 import BeautifulSoup as BS
import json

class WebVPN:
    def __init__(self, opt: dict, headless=False):
        self.root_handle = None
        self.driver: wd = None
        self.userid = opt["username"]
        self.passwd = opt["password"]
        self.headless = headless

    def login_webvpn(self):
        """
        Log in to WebVPN with the account specified in `self.userid` and `self.passwd`

        :return:
        """
        d = self.driver
        if d is not None:
            d.close()
        d = selenium.webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
        d.get("https://webvpn.tsinghua.edu.cn/login")
        username = d.find_elements(By.XPATH,
                                   '//div[@class="login-form-item"]//input'
                                   )[0]
        password = d.find_elements(By.XPATH,
                                   '//div[@class="login-form-item password-field" and not(@id="captcha-wrap")]//input'
                                   )[0]
        username.send_keys(str(self.userid))
        password.send_keys(self.passwd)
        d.find_element(By.ID, "login").click()
        self.root_handle = d.current_window_handle
        self.driver = d
        return d

    def access(self, url_input):
        """
        Jump to the target URL in WebVPN

        :param url_input: target URL
        :return:
        """
        d = self.driver
        url = By.ID, "quick-access-input"
        btn = By.ID, "go"
        wdw(d, 5).until(EC.visibility_of_element_located(url))
        actions = AC(d)
        actions.move_to_element(d.find_element(*url))
        actions.click()
        actions.\
            key_down(Keys.CONTROL).\
            send_keys("A").\
            key_up(Keys.CONTROL).\
            send_keys(Keys.DELETE).\
            perform()

        d.find_element(*url)
        d.find_element(*url).send_keys(url_input)
        d.find_element(*btn).click()

    def switch_another(self):
        """
        If there are only 2 windows handles, switch to the other one

        :return:
        """
        d = self.driver
        assert len(d.window_handles) == 2
        wdw(d, 5).until(EC.number_of_windows_to_be(2))
        for window_handle in d.window_handles:
            if window_handle != d.current_window_handle:
                d.switch_to.window(window_handle)
                return

    def to_root(self):
        """
        Switch to the home page of WebVPN

        :return:
        """
        self.driver.switch_to.window(self.root_handle)

    def close_all(self):
        """
        Close all window handles

        :return:
        """
        while True:
            try:
                l = len(self.driver.window_handles)
                if l == 0:
                    break
            except selenium.common.exceptions.InvalidSessionIdException:
                return
            self.driver.switch_to.window(self.driver.window_handles[0])
            self.driver.close()

    def login_info(self):
        """
        TODO: After successfully logged into WebVPN, login to info.tsinghua.edu.cn

        :return:
        """

        self.access("info.tsinghua.edu.cn")
        self.switch_another()
        d=self.driver
        user=d.find_element(By.ID,"userName")
        user.send_keys(self.userid)
        passwd=d.find_element(By.NAME,"password")
        passwd.send_keys(self.passwd)
        but= d.find_element(By.XPATH,"/html/body/table[2]/tbody/tr/td[3]/table/tbody/tr/td[6]/input")
        but.click()
        wdw(d,5).until(EC.visibility_of_element_located((By.XPATH,"//*[@id=\"9-771_table\"]/div/ul/li[2]")))
        d.close()
        self.to_root()
        return 
        # Hint: - Use `access` method to jump to info.tsinghua.edu.cn
        #       - Use `switch_another` method to change the window handle
        #       - Wait until the elements are ready, then preform your actions
        #       - Before return, make sure that you have logged in successfully
        raise NotImplementedError

    def get_grades(self):
        """
        TODO: Get and calculate the GPA for each semester.

        Example return / print:
            2020-秋: *.**
            2021-春: *.**
            2021-夏: *.**
            2021-秋: *.**
            2022-春: *.**

        :return:
        """
        self.access("zhjw.cic.tsinghua.edu.cn/cj.cjCjbAll.do?m=bks_cjdcx&cjdlx=zw")
        self.switch_another()
        d=self.driver
        table = d.find_element(By.XPATH,"/html/body/center/table[2]/tbody")
        soup=BS(table.get_attribute("innerHTML"),'lxml')
        a=soup.find_all('tr')
        a=a[1:]
        result={}
        sems=[]
        b=[]
        for i in range(len(a)):
          temp=a[i].find_all("td")
          temp2=[]
          for j in temp:
            r=j.contents[0]
            temp2.append(str(r).replace('\n','').replace('\t','').replace(' ',''))
          b.append(temp2)
        for k in b:
          sem=str(k[5])
          st=str(k[3])
          if st=='P' or st== 'F':
            continue
          if result.__contains__(sem):
            result[sem].append((int(k[2]),float(k[4])))
          else:
            result[sem]=[(int(k[2]),float(k[4]))]
            sems.append(sem)
        for sem in sems:
          t=result[sem]
          sum=0.0
          score=0.0
          for course in t:
            sum+=course[0]
            score+=course[0]*course[1]
          score/=sum
          score=('%.2f'%score)
          result[sem]=score
        return result
        # Hint: - You can directly switch into
        #         `zhjw.cic.tsinghua.edu.cn/cj.cjCjbAll.do?m=bks_cjdcx&cjdlx=zw`
        #         after logged in
        #       - You can use Beautiful Soup to parse the HTML content or use
        #         XPath directly to get the contents
        #       - You can use `element.get_attribute("innerHTML")` to get its
        #         HTML code

        raise NotImplementedError

if __name__ == "__main__":
    # TODO: Write your own query process
    with open("WebVPN_crawler\settings.json") as f:
      dic = json.load(f)
      f.close()
    web = WebVPN(dic)
    web.login_webvpn()
    web.login_info()
    print(web.get_grades())
    
    #raise NotImplementedError
