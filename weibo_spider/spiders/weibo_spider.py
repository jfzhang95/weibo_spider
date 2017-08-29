#!usr/bin/env python
#-*- coding:utf-8 -*-
"""
@author: Jeff Zhang
@date:   
"""

import re
import datetime
from scrapy.spider import CrawlSpider
from scrapy.selector import Selector
from scrapy.http import Request
from weibo_spider.items import InformationItem, TweetsItem, FollowsItem, FansItem


class weiboSpider(CrawlSpider):
    name = "weibo_spider"
    host = "https://weibo.cn"
    start_ids = [
        5235640836, 5676304901, 5871897095, 2139359753, 5579672076, 2517436943, 5778999829, 5780802073, 2159807003,
        1756807885, 3378940452, 5762793904, 1885080105, 5778836010, 5722737202, 3105589817, 5882481217, 5831264835,
        2717354573, 3637185102, 1934363217, 5336500817, 1431308884, 5818747476, 5073111647, 5398825573, 2501511785,
    ]
    scrawl_ID = set(start_ids)
    finish_ID = set() # 记录爬取过的微博ID

    def start_request(self):
        while self.scrawl_ID.__len__():
            ID = self.scrawl_ID.pop()
            self.finish_ID.add(ID)
            ID = str(ID)
            follows = []
            followsItem = FollowsItem()
            followsItem['_id'] = ID
            followsItem['follows'] = follows
            fans = []
            fansItem = FansItem()
            fansItem['_id'] = ID
            fansItem['fans'] = fans

            url_follows = "https://weibo.cn/{}/follow".format(ID)
            url_fans = "https://weibo.cn/{}/fans".format(ID)
            url_tweets = "https://weibo.cn/u/{}".format(ID)
            url_information0 = "https://weibo.cn/{}/info".format(ID)

            yield Request(url_follows, callback=self.parse3, meta={"item": followsItem, "result": follows})  # 去爬关注人
            yield Request(url_fans, callback=self.parse3, meta={"item": fansItem, "result": fans})  # 去爬粉丝
            yield Request(url_information0, callback=self.parse0, meta={"ID": ID})  # 去爬个人信息
            yield Request(url_tweets, callback=self.parse2, meta={"ID": ID})  # 去爬微博

    def parse0(self, response):
        """抓取个人信息1"""

        informationItem = InformationItem()
        sel = Selector(response)
        text0 = sel.xpath('body/div[@class="u"]/div[@class="tip2"]').extract_first()
        if text0:
            num_tweets = re.findall(u'\u5fae\u535a\[(\d+)\]', text0)  # 微博数
            num_follows = re.findall(u'\u5173\u6ce8\[(\d+)\]', text0)  # 关注数
            num_fans = re.findall(u'\u7c89\u4e1d\[(\d+)\]', text0)  # 粉丝数
            if num_tweets:
                informationItem["Num_Tweets"] = int(num_tweets[0])
            if num_follows:
                informationItem["Num_Follows"] = int(num_follows[0])
            if num_fans:
                informationItem["Num_Fans"] = int(num_fans[0])

            informationItem["_id"] = response.meta['ID']
            url_information1 = "https://weibo.cn/{}/info".format(response.meta['ID'])
            yield Request(url_information1, callback=self.parse1, meta={"item":informationItem})

    def parse1(self, response):
        """抓取个人信息2"""

        informationItem = response.meta['item']
        sel = Selector(response)
        text1 = ";".join(sel.xpath('body/div[@class="c"]/text()').extract())

        nickname = re.findall(u'\u6635\u79f0[:|\uff1a](.*?);', text1)  # 昵称
        gender = re.findall(u'\u6027\u522b[:|\uff1a](.*?);', text1)  # 性别
        place = re.findall(u'\u5730\u533a[:|\uff1a](.*?);', text1)  # 地区（包括省份和城市）
        signature = re.findall(u'\u7b80\u4ecb[:|\uff1a](.*?);', text1)  # 简介
        birthday = re.findall(u'\u751f\u65e5[:|\uff1a](.*?);', text1)  # 生日
        sexorientation = re.findall(u'\u6027\u53d6\u5411[:|\uff1a](.*?);', text1)  # 性取向
        marriage = re.findall(u'\u611f\u60c5\u72b6\u51b5[:|\uff1a](.*?);', text1)  # 婚姻状况
        url = re.findall(u'\u4e92\u8054\u7f51[:|\uff1a](.*?);', text1)  # 首页链接

        if nickname:
            informationItem["NickName"] = nickname[0]
        if gender:
            informationItem["Gender"] = gender[0]
        if place:
            place = place[0].split(" ")
            informationItem["Province"] = place[0]
            if len(place) > 1:
                informationItem["City"] = place[1]
        if signature:
            informationItem["Signature"] = signature[0]
        if birthday:
            try:
                birthday = datetime.datetime.strptime(birthday[0], "%Y-%m-%d")
                informationItem["Birthday"] = birthday - datetime.timedelta(hours=8)
            except Exception:
                pass
        if sexorientation:
            if sexorientation[0] == gender[0]:
                informationItem["Sex_Orientation"] = "Homosexual"
            else:
                informationItem["Sex_Orientation"] = "Heterosexual"
        if marriage:
            informationItem["Marriage"] = marriage[0]
        if url:
            informationItem["URL"] = url[0]
        yield informationItem


    def parse2(self, response):
        """抓取微博数据"""

        sel = Selector(response)
        tweets = sel.xpath('body/div[@class="c" and @id]')
        for tweet in tweets:
            tweetsItem = TweetsItem()
            _id = tweet.xpath('@id').extract_first()
            content = tweet.xpath('div/span[@class="ctt"]/text()').extract_first()
            cooridinates = tweet.xpath('div/a/@href').extract_first()

            like = re.findall(u'\u8d5e\[(\d+)\]', tweet.extract())  # 点赞数
            transfer = re.findall(u'\u8f6c\u53d1\[(\d+)\]', tweet.extract())  # 转载数
            comment = re.findall(u'\u8bc4\u8bba\[(\d+)\]', tweet.extract())  # 评论数
            others = tweet.xpath('div/span[@class="ct"]/text()').extract_first()  # 求时间和使用工具（手机或平台）

            tweetsItem["ID"] = response.meta["ID"]
            tweetsItem["_id"] = response.meta["ID"] + "-" + _id
            if content:
                tweetsItem["Content"] = content.strip(u"[\u4f4d\u7f6e]")
            if cooridinates:
                cooridinates = re.findall('center=([\d|.|,]+)', cooridinates)
                if cooridinates:
                    tweetsItem["Co_oridinates"] = cooridinates[0]
            if like:
                tweetsItem["Like"] = int(like[0])
            if transfer:
                tweetsItem["Transfer"] = int(transfer[0])
            if comment:
                tweetsItem["Comment"] = int(comment[0])
            if others:
                others = others.split(u"\u6765\u81ea")
                tweetsItem["PubTime"] = others[0]
                if len(others) == 2:
                    tweetsItem["Tools"] = others[1]
            yield tweetsItem
        url_next = sel.xpath(u'body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
        if url_next:
            next_url = self.host + url_next[0]
            yield Request(next_url, callback=self.parse2, meta={"ID": response.meta["ID"]})

    def parse3(self, response):
        """抓取关注或粉丝"""

        items = response.meta["item"]
        sel = Selector(response)
        text2 = sel.xpath(
            u'body//table/tr/td/a[text()="\u5173\u6ce8\u4ed6" or text()="\u5173\u6ce8\u5979"]/@href').extract()
        for elem in text2:
            elem = re.findall('uid=(\d+)', elem)
            if elem:
                response.meta["result"].append(elem[0])
                ID = int(elem[0])
                if ID not in self.finish_ID:
                    self.scrawl_ID.add(ID)
        url_next = sel.xpath(
            u'body//div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
        if url_next:
            next_url = self.host + url_next[0]
            yield Request(next_url, callback=self.parse3, meta={"item": items, "result": response.meta["result"]})
        else:
            yield items