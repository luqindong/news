import time
import calendar
import os
from fake_useragent import UserAgent
import requests
from lxml import etree
from queue import Queue
from multiprocessing import Process,Pool
import random,threading
import pymongo


# 时间类
class getTime():
    # http://www.chinanews.com/scroll-news/2011/0101/news.shtml
    # 获取此部分2013/0101---2020/1231
    def history_time(self):
        all_Time = {}
        # 获取从2013-至今的年份列表
        years = [num for num in range(2013, time.localtime().tm_year+1)]
        # 遍历这些年
        for year in years:
            yTime = []
            # 遍历每年的12个月
            for month in range(1, 13):
                # calendar模块获取某年某月的天数
                days = calendar.monthrange(year, month)[1]
                month = '0' + str(month) if month < 10 else str(month)
                # 遍历当前月的每一天
                for day in range(1, days+1):
                    day = '0' + str(day) if day < 10 else str(day)
                    str_time = str(year) + '/' + str(month) + str(day)
                    yTime.append(str_time)

            all_Time[year] = yTime
        return all_Time

    # 获取当前时间：年_月_日/时:分:秒
    def now_time(self):
        self.now_time = time.strftime('%Y_%m_%d/%H:%M:%S',time.localtime())

# 新闻文件内容保存类
class saveDocument():
    # 判断是否存在文件夹
    def isFile(self,time):
        year = time.split('/')[0]
        month = time.split('/')[1][0:2]
        day = time.split('/')[1][2:]
        # 判断是否存在文件夹，不存在则创建
        if os.path.exists('news_data/' + year + '年') is False:
            # 不存在年文件夹，需创建文件夹
            os.mkdir('news_data/' + year + '年')

        # 判断是否存在月文件夹，不存在则创建
        if os.path.exists('news_data/' + year + '年/' + month + '月') is False:
            # 不存在月文件夹，需创建文件夹
            os.mkdir('news_data/' + year + '年/' + month + '月')

        # 每天，总路径及文件名称.类型
        self.path = 'news_data/' + year + '年/' + month + '月/' + year + '_' + month + '_' +day +'.txt'

    # 以年/月/年_月_日.txt方式保存新闻内容
    def saveNewsFile(self,newsData):
        # 总路径及文件名称.类型
        path =self.path
        # 创建txt文件及写入
        with open(path,'w+',encoding='utf-8') as f:
            # 写入内容
            f.write(newsData)
            # 关闭文件
            f.close()

    # 其他文件每月保存
    def saveTitle(self,time,newsTitles):
        save = saveDocument()
        save.isFile(time)

        year = time.split('/')[0]
        month = time.split('/')[1][0:2]
        otherPath = 'news_data/' + year + '年/' + month + '月/' + year + '_' + month + '_新闻标题.txt'
        # 创建txt文件及写入
        with open(otherPath, 'a+', encoding='utf-8') as f:
            # 写入内容
            for newsTitle in newsTitles:
                f.write(newsTitle)
            # 关闭文件
            f.close()

    # 总方法
    def saveDocument(self,time,newsData):
        save = saveDocument()
        save.isFile(time)
        save.saveNewsFile(newsData)

# mongo数据保存类
class saveMongo():
    # 初始化连接数据库
    def __init__(self):
        self.client = pymongo.MongoClient('127.0.0.1',27017)

    # 导入新闻标题数据
    def inNewsTitle(self,data):
        db = self.client['zhongXinnews']['newsTitle']
        db.insert_many(data)
        self.client.close()

    # 导入新闻内容数据
    def inNewsData(self,data):
        db = self.client['zhongXinnews']['newsData']
        db.insert(data)
        self.client.close()

# 新闻标题数据解析获取类
class newsUrl():
    # 配置随机请求头
    def __init__(self):
        self.ua = UserAgent()

    # url拼接及获取每天所有新闻标题的txt
    def getNewsUrls(self, value):
        save = saveDocument()
        url = 'http://www.chinanews.com/scroll-news/{}/news.shtml'

        url = url.format(value)
        header = {
            'User-Agent': self.ua.random
        }

        res = requests.get(url, headers=header).content
        e = etree.HTML(res)

        # 获取每天的新闻标题
        try:
            title1 = e.xpath('//h1/text()')[0].encode('iso-8859-1').decode('utf8')
            title2 = e.xpath('//h1/span/text()')[0].encode('iso-8859-1').decode('utf8')
            save.saveTitle(value, [title1, title2 + '\n', ])
        except IndexError:
            return save.saveTitle(value, [value,'异常数据'])
        except Exception:
            title1 = e.xpath('//h1/text()')[0]
            title2 = e.xpath('//h1/span/text()')[0]
            save.saveTitle(value, [title1, title2 + '\n', ])

        newsTitle1s = e.xpath('//div[@class="dd_lm"]/a/text()')
        newsTitle2s = e.xpath('//div[@class="dd_bt"]/a/text()')
        newsTitle3s = e.xpath('//div[@class="dd_time"]/text()')

        titleList = []
        for newsTitle1,newsTitle2,newsTitle3 in zip(newsTitle1s,newsTitle2s,newsTitle3s):
            try:
                newsTitle1 = newsTitle1.encode('iso-8859-1').decode('utf8')
                newsTitle2 = newsTitle2.encode('iso-8859-1').decode('utf8')
                titleList.append('[' + newsTitle1 + ']  ' + newsTitle3 + '  ' + newsTitle2 + '\n')
            except Exception:
                titleList.append('[' + newsTitle1 + ']  ' + newsTitle3 + '  ' + newsTitle2 + '\n')

        save.saveTitle(value, titleList)
        save.saveTitle(value, ['\n'])



        # 获取每天每个新闻的url，返回url列表
        base_urlList = []
        newsUrls = e.xpath('//div[@class="dd_bt"]/a/@href')
        for base_url in newsUrls:
            if 'http://www.chinanews.com' not in base_url:
                base_url = 'http://www.chinanews.com' + base_url
            base_urlList.append(base_url)

        return base_urlList

# 新闻内容数据解析获取
class newsDatas():
    # 配置随机请求头等
    def __init__(self):
        self.ua = UserAgent()
        self.savemongo = saveMongo()

    # 获取新闻页面
    def getData(self,urls):
        for url in  urls:
            header = {'User-Agent': self.ua.random}
            res = requests.get(url, headers=header).content
            e = etree.HTML(res)

            if 'http://www.chinanews.com/tp' in url:
                '''
                category = '图片频道'
                title = e.xpath('//i[@class="title"]/text()')[0]
                imgsNum = int(e.xpath('//span[@id="showTotal"]/text()')[0])
                #imgUrls = e.xpath('//a[@id="apDiv1"]/@href')[0]
                text = e.xpath('//div[@class="t3"]/text()')[0]
                fortime = e.xpath('//div[@class="left-t"]/text()')[0].split('：')[0].replace('\u3000来源', '')
                editor = e.xpath('//span[@id="editor_baidu"]/text()')[0].replace('责任编辑：', '')

                item = {
                    'category': category,
                    'title': title,
                    'fortime': fortime,
                    'year': fortime.split('年')[0],
                    'month': fortime.split('年')[1].split('月')[0],
                    'day': fortime.split('年')[1].split('月')[1].split('日')[0],
                    'time': fortime.split('日')[1],
                    'imgsNum': imgsNum,
                    'editor': editor,
                    'text': text,
                }
                self.savemongo.inNewsData(item)
                print(item)
                '''
                pass
            else:
                category = e.xpath('//div[@id="nav"]/a[2]/text()')[0]
                title = e.xpath('//div[@class="content"]/h1/text()')[0]
                fortime = e.xpath('//div[@class="left-t"]/text()')[0].split('：')[0].replace('\u3000来源','')
                source = e.xpath('//div[@class="left-t"]/a[1]/text()')[0]
                if source == '参与互动':
                    source = e.xpath('//div[@class="left-t"]/text()')[0].split('：')[1]
                author = e.xpath('//span[@id="author_baidu"]/text()')[0].replace('作者：','')
                editor = e.xpath('//span[@id="editor_baidu"]/text()')[0].replace('责任编辑：','')
                text = ''.join(e.xpath('//div[@class="left_zw"]/p/text()')).replace('\u3000\u3000','\n')

                item = {
                    'category': category,
                    'title': title,
                    'fortime': fortime,
                    'year': fortime.split('年')[0],
                    'month': fortime.split('年')[1].split('月')[0],
                    'day': fortime.split('年')[1].split('月')[1].split('日')[0],
                    'time': fortime.split('日')[1],
                    'source': source,
                    'author': author,
                    'editor': editor,
                    'text': text
                }
                self.savemongo.inNewsData(item)
                print(item)


# 多线程-生产者类
class Producer(threading.Thread):
    # 初始化设置
    def __init__(self, times_dict, queue):
        threading.Thread.__init__(self, )
        self.times_dict = times_dict
        self.queue = queue

    # 重写run,向队列写入每天的url,以天为单位的列表url
    def run(self):
        # 遍历每年
        for values in self.times_dict.values():
            # 遍历每天
            for value in values:
                value_xx = value.split('/')
                today = time.strftime('%m%d', time.localtime())
                if value_xx[0]=='2020' and value_xx[1]>today:
                    pass
                else:
                    base_urlList = newsUrl.getNewsUrls(value)
                    if self.queue.qsize()>1900:
                        time.sleep(5)
                    self.queue.put(base_urlList)

# 多线程-消费者类
class Consumer(threading.Thread):
    # 初始化设置
    def __init__(self, queue):
        threading.Thread.__init__(self, )
        self.queue = queue


    # 重写run,获取队列中的url解析网页获取数据
    def run(self):
        # 队列中获取以天为单位的列表url
        while True:
            newsDatas.getData(self.queue.get())
            if self.queue.qsize()==0:
                time.sleep(10)
                if self.queue.qsize()==0:
                    break


def main():
    start =time.time()

    queue = Queue(maxsize=2000)
    all_times = getTime.history_time()

    # 开始生产
    thread_list = []
    for key in all_times.keys():
        produce = Producer({key: all_times[key]}, queue)
        thread_list.append(produce)

    for t in thread_list:
        t.start()

    # 开始消费
    thread_list1 = []
    for i in range(10):
        consumer = Consumer(queue,)
        thread_list1.append(consumer)

    for t in thread_list1:
        t.start()


    for t in thread_list:
        t.join()
    for t in thread_list1:
        t.join()

    end = time.time()
    print('ok',end-start)


if __name__ == '__main__':
    getTime = getTime()
    newsUrl = newsUrl()
    newsDatas = newsDatas()

    times_dict = getTime.history_time()

    main()

