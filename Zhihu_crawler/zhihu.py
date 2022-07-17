import requests
import json
import pymysql
from bs4 import BeautifulSoup as BS
import logging
import time

fmt = '%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S'
level = logging.INFO

formatter = logging.Formatter(fmt, datefmt)
logger = logging.getLogger()
logger.setLevel(level)

file = logging.FileHandler("../zhihu.log", encoding='utf-8')
file.setLevel(level)
file.setFormatter(formatter)
logger.addHandler(file)

console = logging.StreamHandler()
console.setLevel(level)
console.setFormatter(formatter)
logger.addHandler(console)

def getQid(url):
  temp=url[0:31]
  if temp =='https://www.zhihu.com/question/':
    return int(url[31:])
  else:
    return 0
class ZhihuCrawler:
    def __init__(self):
        with open("Zhihu_crawler\zhihu.json", "r", encoding="utf8") as f:
            self.settings = json.load(f)  # Load settings
        logger.info("Settings loaded")


    def sleep(self, sleep_key, delta=0):
        """
        Execute sleeping for a time configured in the settings

        :param sleep_key: the sleep time label
        :param delta: added to the sleep time
        :return:
        """
        _t = self.settings["config"][sleep_key] + delta
        logger.info(f"Sleep {_t} second(s)")
        time.sleep(_t)

    def query(self, sql, args=None, op=None):
        """
        Execute an SQL query

        :param sql: the SQL query to execute
        :param args: the arguments in the query
        :param op: the operation to cursor after query
        :return: op(cur)
        """
        conn = pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
            **self.settings['mysql']
        )
        if args and not (isinstance(args, tuple) or isinstance(args, list)):
            args = (args,)
        with conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql, args)
                    conn.commit()
                    if op is not None:
                        return op(cur)
                except:  # Log query then exit
                    if hasattr(cur, "_last_executed"):
                        logger.error("Exception @ " + cur._last_executed)
                    else:
                        logger.error("Exception @ " + sql)
                    raise

    def watch(self, top=None):
        """
        The crawling flow

        :param top: only look at the first `top` entries in the board. It can be used when debugging
        :return:
        """
        self.create_table()
        while True:
            logger.info("Begin crawling ...")
            try:
                crawl_id = None
                begin_time = time.time()
                crawl_id = self.begin_crawl(begin_time)

                try:
                    board_entries = self.get_board()
                except RuntimeError as e:
                    if isinstance(e.args[0], requests.Response):
                        logger.exception(e.args[0].status_code, e.args[0].text)
                    raise
                else:
                    logger.info(
                        f"Get {len(board_entries)} items: {','.join(map(lambda x: x['title'][:20], board_entries))}")
                if top:
                    board_entries = board_entries[:top]

                # Process each entry in the hot list
                for idx, item in enumerate(board_entries):
                    self.sleep("interval_between_question")
                    detail = {
                        "created": None,
                        "visitCount": None,
                        "followerCount": None,
                        "answerCount": None,
                        "raw": None,
                        "hit_at": None
                    }
                    if item["qid"] is None:
                        logger.warning(f"Unparsed URL @ {item['url']} ranking {idx} in crawl {crawl_id}.")
                    else:
                        try:
                            detail = self.get_question(item["qid"])
                        except Exception as e:
                            if len(e.args) > 0 and isinstance(e.args[0], requests.Response):
                                logger.exception(f"{e}; {e.args[0].status_code}; {e.args[0].text}")
                            else:
                                logger.exception(f"{str(e)}")
                        else:
                            logger.info(f"Get question detail for {item['title']}: raw detail length {len(detail['raw']) if detail['raw'] else 0}")
                    try:
                        self.add_entry(crawl_id, idx, item, detail)
                    except Exception as e:
                        logger.exception(f"Exception when adding entry {e}")
                self.end_crawl(crawl_id)
            except Exception as e:
                logger.exception(f"Crawl {crawl_id} encountered an exception {e}. This crawl stopped.")
            self.sleep("interval_between_board", delta=(begin_time - time.time()))

    def create_table(self):
        """
        Create tables to store the hot question records and crawl records

        """
        sql = f"""
CREATE TABLE IF NOT EXISTS `crawl` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `begin` DOUBLE NOT NULL,
    `end` DOUBLE,
    PRIMARY KEY (`id`) USING BTREE
)
AUTO_INCREMENT = 1 
CHARACTER SET = utf8mb4 
COLLATE = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `record`  (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `qid` INT NOT NULL,
    `crawl_id` BIGINT NOT NULL,
    `hit_at` DOUBLE,
    `ranking` INT NOT NULL,
    `title` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL ,
    `heat` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    `created` INT,
    `visitCount` INT,
    `followerCount` INT,
    `answerCount` INT,
    `excerpt` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    `raw` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ,
    `url` VARCHAR(255),
    PRIMARY KEY (`id`) USING BTREE,
    INDEX `CrawlAssociation` (`crawl_id`) USING BTREE,
    CONSTRAINT `CrawlAssociationFK` FOREIGN KEY (`crawl_id`) REFERENCES `crawl` (`id`)
) 
AUTO_INCREMENT = 1 
CHARACTER SET = utf8mb4 
COLLATE = utf8mb4_unicode_ci;

"""
        self.query(sql)

    def begin_crawl(self, begin_time) -> (int,float):
        """
        Mark the beginning of a crawl
        :param begin_time:
        :return: (Crawl ID, the time marked when crawl begin)
        """
        sql = """
INSERT INTO crawl (begin) VALUES(%s);
"""
        return self.query(sql, begin_time, lambda x: x.lastrowid)

    def end_crawl(self, crawl_id: int):
        """
        Mark the ending time of a crawl

        :param crawl_id: Crawl ID
        """
        sql = """
UPDATE crawl SET end = %s WHERE id = %s;
"""
        self.query(sql, (time.time(), crawl_id))

 

    def add_entry(self, crawl_id, idx, board, detail):
        """
        Add a question entry to database

        :param crawl_id: Crawl ID
        :param idx: Ranking in the board
        :param board: dict, info from the board
        :param detail: dict, info from the detail page
        """
        sql = \
            """
INSERT INTO record (`qid`, `crawl_id`, `title`, `heat`, `created`, `visitCount`, `followerCount`, `answerCount`,`excerpt`, `raw`, `ranking`, `hit_at`, `url`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""
        self.query(
            sql,
            (
                board["qid"],
                crawl_id,
                board["title"],
                board["heat"],
                detail["created"],
                detail["visitCount"],
                detail["followerCount"],
                detail["answerCount"],
                board["excerpt"],
                detail["raw"],
                idx,
                detail["hit_at"],
                board["url"]
            )
        )

    def get_board(self) -> list:
        """
        TODO: Fetch current hot questions
        
        :return: hot question list, ranking from high to low

        Return Example:
        [
            {
                'title': '针对近期生猪市场非理性行为，国家发展改革委研究投放猪肉储备，此举对市场将产生哪些积极影响？',
                'heat': '76万热度',
                'excerpt': '据国家发展改革委微信公众号 7 月 5 日消息，针对近期生猪市场出现盲目压栏惜售等非理性行为，国家发展改革委价格司正研究启动投放中央猪肉储备，并指导地方适时联动投放储备，形成调控合力，防范生猪价格过快上涨。',
                'url': 'https://www.zhihu.com/question/541600869',
                'qid': 541600869,
            },
            {
                'title': '有哪些描写夏天的古诗词？',
                'heat': '41万热度',
                'excerpt': None,
                'url': 'https://www.zhihu.com/question/541032225',
                'qid': 541032225,
            },
            {
                'title':    # 问题标题
                'heat':     # 问题热度
                'excerpt':  # 问题摘要
                'url':      # 问题网址
                'qid':      # 问题编号
            }
            ...
        ]
        """
        
        results=[]
        resp = requests.get("https://www.zhihu.com/hot",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0","Cookie":"_zap=e40d7aab-28e0-4b4c-913f-890089f0dd7d; d_c0=\"APCfNZ0jMxWPTnqS2WvWHnRE2GLV1Il-PMs=|1656989140\"; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1657110834,1657589610,1657780303,1657867448; captcha_session_v2=2|1:0|10:1657784325|18:captcha_session_v2|88:cFRRVzNMT1E5K3Z1RHdQVmR0TlBaSkRrZlJ0VXFBd0VkSGNQM2dtdDJXOEl4d0crQld4Rzl5Rkk5Qk9jQkVkZA==|9dc936750bdf67ddc8ca24dc6b3980df8782be9dd4d78eda52153435d0c6826a; __snaker__id=uGFy37d3vALoLEal; YD00517437729195%3AWM_NI=fK8WrIeSBevvj7MwDrfjWqM6Dud74NaQCrlNj7OPSRyYFf9zyOyKIXezuTg%2B4gGsxEQCsdREwCQ1YQmeR856xObMJkL1pKDhrskfZ11uVUlBl3kVXtyO%2FsAa9ydJu90nTVM%3D; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6ee97c85394ef8c9bf04796ac8ea7c55b968e9e86d55bf6ababd3c760aceda682e22af0fea7c3b92aa695f8b8f143a78d8e86f57eaae98398c77aa6b69992d03e88abacd8b746a38f8d93ef46aae981b1d25abb878a87bc72bb91b7accc54a7a6bbb5e621f8b98994fb4f939dbd85d14fb59d9fd2dc5bafbb9ed9b83ebc91aeaec96190efc0acc764b2ada587d93bf3ab9ab6ef4ab0eda5b7db80bb91e194c46bfba7acabf833f4b29cb7d037e2a3; YD00517437729195%3AWM_TID=Dv%2B9DP9yt5VEVQBAEUMvz7hOC78jbQ0X; gdxidpyhxdE=06W0aezaL%2BzWc9HqnacUyWy09%2BDqHb1fvGlaerUicy1T9qr9YwvEV%2BbMy8xZ7eJaBR43tm0oqL5M2nGHCiHTvDh8TzbsvTts%2B9N%2B0PD7oU47XpfP%2FY2MgeducEfN0x3Wuo2LcN1xuC0PGMycRp%2F21je6BnrD2O9HesYQhaInyG8ZmgQ8%3A1657799483632; _9755xjdesxxd_=32; captcha_ticket_v2=2|1:0|10:1657784400|17:captcha_ticket_v2|704:eyJ2YWxpZGF0ZSI6IkNOMzFfcUJ2QzRvRldydnp4STR0Mmh6bWFYMXhSNGJhaEgyYXRjLjdvX3k1Y2g1bTFnV0hnT0VwV2o4NXBnMlNsUHZGNERNbWFXU1hQbFFaNmYxZ3pKckdwc2lmazF6dnpYUldEZlZvRC5jbHF2dGE2eDF3SmQ2N2hzdmJfSWZHZDZIWUE1VEg3VlRLZ2h2SEhSbzE5bWNJcjBybzAuOWxxdTJMbl9UOHZvYUtKcEE1VW43UXpNNG1IWFpkMENYNjBEMElZZGpkRzYwV0NHclNnMmlyQ2ouNDFGTDhWU2F2ckZzWjB0Vl9iX2xhVl9HY2hLOTVENFFMQVEwMXp3cU5Cb083R1haaW9PMjZoMHpCb3JpeGs0NFNtYUtnVmhqVkVYbzFJWE13N2laY0FpanFrVXFwSnVDLURlODc3cm5aQklWb0FWTURId1o2b3lwZU1XNEt1X2VzYkkyVWJkMnhFYU0weF9MbUludEluZ3hTMmRhUy45OGlYWUlUZXJfTlpPUXBQdzJqYlpLRjJpeE91d3AwcXJTX2VjUm55bkZnLmR1N3QxSHVIOC1sbXV4dG5sbE9XWG00YVNDdVVSbHRqb0RnSjZnY2ZrSndfa2xWMUNXcjV2OHBuNjhHNUs4aVA4SGdRSG5aUC5JbG9lUGRUeWkxRGhXazBxdFJKY2djMyJ9|051c3cbb2faab6d0e119a9c14fc44c4f1aeda6b99f8c08b11936bea89184a2ff; z_c0=2|1:0|10:1657784412|4:z_c0|92:Mi4xd0lQV0JBQUFBQUFBOEo4MW5TTXpGU1lBQUFCZ0FsVk5XeGE5WXdBTXg1NV96Z2RwUzBPSklNMllWVXgzYU1uYVBR|c47d4baed05afe28ed0159c80293653eaf52ad96be4ba27958921fca5b9ebfa4; q_c1=238150b8f0c24666838561105122e78b|1657784412000|1657784412000; tst=h; _xsrf=cecc4a2d-fdff-487b-b811-6dac2205257f; KLBRSID=cdfcc1d45d024a211bb7144f66bda2cf|1657867710|1657867446; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1657867448; NOT_UNREGISTER_WAITING=1; SESSIONID=69Mx5U91LEmLpgtnBM0x2gjWwbbhIslmW0svzs03wQI; JOID=VFoSAUNwgT4SaYLXT3R74d4BLmpdQr12YFLT63sywFZbE-unJM2N4HRjg9dAHyQycwp9ItVyuRmPKRNUOWIzHEo=; osd=V18RCk5zhD0ZZIHSTH924tsCJWdeR759bVHW6HA_w1NYGOakIc6G7XdmgNxNHCExeAd-J9Z5tBqKKhhZOmcwF0c="})
        soup = BS(resp.text,'lxml')
        script=soup.find("script",id ='js-initialData',type="text/json")
        script=json.loads(script.contents[0])
        s2=script['initialState']['topstory']['hotList']
        for i in range(len(s2)):
          s3=s2[i]['target']
          temp={}
          temp['url']=s3['link']['url']
          temp['heat']=s3['metricsArea']['text']
          temp['excerpt']=s3['excerptArea']['text']
          temp['title']=s3['titleArea']['text']
          temp['qid']=getQid(temp['url'])
          results.append(temp)
        return results
       
        # Hint: - Parse HTML, pay attention to the <section> tag.
        #       - Use keyword argument `class_` to specify the class of a tag in `find`
        #       - Hot Question List can be accessed in https://www.zhihu.com/hot

        raise NotImplementedError

    def get_question(self, qid: int) -> dict:
        """
        TODO: Fetch question info by question ID

        :param qid: Question ID
        :return: a dict of question info

        Return Example:
        {
            "created": 1657248657,      # 问题的创建时间
            "followerCount": 5980,      # 问题的关注数量
            "visitCount": 2139067,      # 问题的浏览次数
            "answerCount": 2512         # 问题的回答数量
            "title": "日本前首相安倍      # 问题的标题
                晋三胸部中枪已无生命
                体征 ，嫌疑人被控制，
                目前最新进展如何？背
                后原因为何？",
            "raw": "<p>据央视新闻，        # 问题的详细描述
                当地时间8日，日本前
                首相安倍晋三当天上午
                在奈良发表演讲时中枪
                。据悉，安倍晋三在上
                救护车时还有意。。。",
            "hit_at": 1657264954.3134503  # 请求的时间戳
        }


        """
        if qid==0:
          temp={}
          temp['created']=0
          temp['followerCount']=0
          temp["visitCount"]=0
          temp['answerCount']=0
          temp['title']='skip'
          temp['raw']='skip'
          temp['hit_at']=0.0
          return temp
        url = "https://www.zhihu.com/question/"+str(qid)
        time1=time.time()
        resp = requests.get(url,headers={"Cookie":"SESSIONID=NEgUlT0aMHRJE387Cw5T1jvOv54VxsO37dPBGflzynr; JOID=VFkWCkK6yM7tcFJrTLUwFSkd9dRa2f2qnk8TJyvWu7mzA24jNJCVFYB4V2NGdXwQXIzLtM50d930a7rEWYlbno4=; osd=UFgSA02-ycrkf1ZqSLw_ESgZ_Nte2PmjkUsSIyLZv7i3CmEnNZScGoR5U2pJcX0UVYPPtcp9eNn1b7PLXYhfl4E=; _zap=e40d7aab-28e0-4b4c-913f-890089f0dd7d; d_c0=\"APCfNZ0jMxWPTnqS2WvWHnRE2GLV1Il-PMs=|1656989140\"; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1657110834,1657589610,1657780303,1657867448; captcha_session_v2=2|1:0|10:1657784325|18:captcha_session_v2|88:cFRRVzNMT1E5K3Z1RHdQVmR0TlBaSkRrZlJ0VXFBd0VkSGNQM2dtdDJXOEl4d0crQld4Rzl5Rkk5Qk9jQkVkZA==|9dc936750bdf67ddc8ca24dc6b3980df8782be9dd4d78eda52153435d0c6826a; __snaker__id=uGFy37d3vALoLEal; YD00517437729195%3AWM_NI=fK8WrIeSBevvj7MwDrfjWqM6Dud74NaQCrlNj7OPSRyYFf9zyOyKIXezuTg%2B4gGsxEQCsdREwCQ1YQmeR856xObMJkL1pKDhrskfZ11uVUlBl3kVXtyO%2FsAa9ydJu90nTVM%3D; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6ee97c85394ef8c9bf04796ac8ea7c55b968e9e86d55bf6ababd3c760aceda682e22af0fea7c3b92aa695f8b8f143a78d8e86f57eaae98398c77aa6b69992d03e88abacd8b746a38f8d93ef46aae981b1d25abb878a87bc72bb91b7accc54a7a6bbb5e621f8b98994fb4f939dbd85d14fb59d9fd2dc5bafbb9ed9b83ebc91aeaec96190efc0acc764b2ada587d93bf3ab9ab6ef4ab0eda5b7db80bb91e194c46bfba7acabf833f4b29cb7d037e2a3; YD00517437729195%3AWM_TID=Dv%2B9DP9yt5VEVQBAEUMvz7hOC78jbQ0X; gdxidpyhxdE=06W0aezaL%2BzWc9HqnacUyWy09%2BDqHb1fvGlaerUicy1T9qr9YwvEV%2BbMy8xZ7eJaBR43tm0oqL5M2nGHCiHTvDh8TzbsvTts%2B9N%2B0PD7oU47XpfP%2FY2MgeducEfN0x3Wuo2LcN1xuC0PGMycRp%2F21je6BnrD2O9HesYQhaInyG8ZmgQ8%3A1657799483632; _9755xjdesxxd_=32; captcha_ticket_v2=2|1:0|10:1657784400|17:captcha_ticket_v2|704:eyJ2YWxpZGF0ZSI6IkNOMzFfcUJ2QzRvRldydnp4STR0Mmh6bWFYMXhSNGJhaEgyYXRjLjdvX3k1Y2g1bTFnV0hnT0VwV2o4NXBnMlNsUHZGNERNbWFXU1hQbFFaNmYxZ3pKckdwc2lmazF6dnpYUldEZlZvRC5jbHF2dGE2eDF3SmQ2N2hzdmJfSWZHZDZIWUE1VEg3VlRLZ2h2SEhSbzE5bWNJcjBybzAuOWxxdTJMbl9UOHZvYUtKcEE1VW43UXpNNG1IWFpkMENYNjBEMElZZGpkRzYwV0NHclNnMmlyQ2ouNDFGTDhWU2F2ckZzWjB0Vl9iX2xhVl9HY2hLOTVENFFMQVEwMXp3cU5Cb083R1haaW9PMjZoMHpCb3JpeGs0NFNtYUtnVmhqVkVYbzFJWE13N2laY0FpanFrVXFwSnVDLURlODc3cm5aQklWb0FWTURId1o2b3lwZU1XNEt1X2VzYkkyVWJkMnhFYU0weF9MbUludEluZ3hTMmRhUy45OGlYWUlUZXJfTlpPUXBQdzJqYlpLRjJpeE91d3AwcXJTX2VjUm55bkZnLmR1N3QxSHVIOC1sbXV4dG5sbE9XWG00YVNDdVVSbHRqb0RnSjZnY2ZrSndfa2xWMUNXcjV2OHBuNjhHNUs4aVA4SGdRSG5aUC5JbG9lUGRUeWkxRGhXazBxdFJKY2djMyJ9|051c3cbb2faab6d0e119a9c14fc44c4f1aeda6b99f8c08b11936bea89184a2ff; z_c0=2|1:0|10:1657784412|4:z_c0|92:Mi4xd0lQV0JBQUFBQUFBOEo4MW5TTXpGU1lBQUFCZ0FsVk5XeGE5WXdBTXg1NV96Z2RwUzBPSklNMllWVXgzYU1uYVBR|c47d4baed05afe28ed0159c80293653eaf52ad96be4ba27958921fca5b9ebfa4; q_c1=238150b8f0c24666838561105122e78b|1657784412000|1657784412000; tst=h; _xsrf=cecc4a2d-fdff-487b-b811-6dac2205257f; KLBRSID=e42bab774ac0012482937540873c03cf|1657886298|1657884328; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1657885994; SESSIONID=d7ns0FNmctuTs3Oz0vTpLUEgueUTQn9oSo8KRFcBeOr; JOID=VFoSAUNwgT4SaYLXT3R74d4BLmpdQr12YFLT63sywFZbE-unJM2N4HRjg9dAHyQycwp9ItVyuRmPKRNUOWIzHEo=; osd=V18RCk5zhD0ZZIHSTH924tsCJWdeR759bVHW6HA_w1NYGOakIc6G7XdmgNxNHCExeAd-J9Z5tBqKKhhZOmcwF0c=;NOT_UNREGISTER_WAITING=1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"})
        soup=BS(resp.text,'lxml')
        ans=soup.find('script',id="js-initialData",type='text/json')
        content=json.loads(ans.text)
        content_i=content['initialState']['entities']['questions'][str(qid)]
        temp={}
        temp['created']=content['initialState']['entities']['questions'][str(qid)]['created']
        temp['followerCount']=content_i['followerCount']
        temp["visitCount"]=content_i['visitCount']
        temp['answerCount']=content_i['answerCount']
        temp['title']=content_i['title']
        temp['raw']=content_i['detail']
        temp['hit_at']=time1
        # Hint: - Parse JSON, which is embedded in a <script> and contains all information you need.
        #       - After find the element in soup, use `.text` attribute to get the inner text
        #       - Use `json.loads` to convert JSON string to `dict` or `list`
        #       - You may first save the JSON in a file, format it and locate the info you need
        #       - Use `time.time()` to create the time stamp
        #       - Question can be accessed in https://www.zhihu.com/question/<Question ID>
        return temp
        raise NotImplementedError

if __name__ == "__main__":
    z = ZhihuCrawler()
    z.watch()
