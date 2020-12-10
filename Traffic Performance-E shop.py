# import pymsql
import pymysql

# import Numpy, Pandas
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

# import matplotlib
import matplotlib.pyplot as plt
# 解決中文亂碼問題
plt.rcParams['font.sans-serif'] = ['SimHei']

# import url address split
from urllib.parse import urlsplit, parse_qs

# import apriori(Association Rule; Market Basket Analysis)
from efficient_apriori import apriori

# connect with the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             port=3306,                       
                             db='demo_shop_logs',
                             charset='utf8mb4',
                             # 使用 dict 取代 tuple 當作回傳資料格式
                             cursorclass=pymysql.cursors.DictCursor)

try:
    # 程式正確執行下，自動關閉資料庫連線
    with connection.cursor() as cursor:
        
        # 使用者瀏覽網頁的次數分析(Page View, Unique Page View, Unique User)
        sql_access_logs = """
            SELECT url,
            COUNT(DISTINCT cookie_id) AS unique_user,
            COUNT(DISTINCT session_id) AS unique_page_views,
            COUNT(*) AS page_views
            FROM user_access_logs
            GROUP BY url;
         """

        # 數位行銷管道的成效分析(UTM標籤來追蹤流量來源)
        sql_purchase_logs = 'SELECT * FROM user_purchase_logs'

        # 關鍵字的排行分析(未搜尋到)
        sql_search_logs = """
        SELECT
            keyword,
            COUNT(*) AS search_count
        FROM user_search_logs
        WHERE action = 'SEARCH' AND result_num = 0
        GROUP BY keyword
        ORDER BY search_count DESC;
        """
        # 購物籃分析
        sql_order_logs = 'SELECT * FROM user_order_logs'
        

        sqls = [sql_access_logs, sql_purchase_logs, sql_search_logs, sql_order_logs]
        query_names = ["access", "purchase", "search", "order"]
        results = {}
        for i, sql in enumerate(sqls):
            # 執行 SQL 敘述查詢資料
            cursor.execute(sql)
            # 取出多筆結果
            df = pd.DataFrame(cursor.fetchall())
            results[query_names[i]] = df
                       
finally:
    # 程式錯誤執行下，仍執行關閉資料庫連線
    connection.close()


# 使用者瀏覽網頁的次數分析
# Sum up Page View, Unique Page View, Unique User
print(results['access'])
# 造成 Unique User & Unique Page View 的差異原因:
# 同一個user可能關掉瀏覽器後(結束此次的工作階段)，再次開啟目標網頁(變成新的Session)


# 數位行銷管道的成效分析
def split_url(address):
    # 使用 urlsplit 將網址分解，取出 ?後面的Query String
    query_str = urlsplit(address).query
    # 將Query String所接的參數轉為 dict：{}
    query_dict = parse_qs(query_str)
    return query_dict

# 使用者是透過column:referrer的網址進入到目標頁面
utm_dict = results['purchase']['referrer'].apply(split_url)

# UTM tag 統計資料
utm_stats = {
'utm_source': {},
'utm_medium': {},
'utm_campaign': {}
}
    
for row_index in range(len(utm_dict)):
    # 將Query String一一取出
    for query_key, query_value in utm_dict[row_index].items():
        utm_value = query_value[0]
        # 若utm_value曾出現在 dict 的 key 中，則+1
        if utm_value in utm_stats[query_key]:
            utm_stats[query_key][utm_value] += 1
        # 否則設成1 (避免key error)
        else:
            utm_stats[query_key][utm_value] = 1

print(utm_stats)

import operator
# getting key with max or min value in dictionary
for key, value in utm_stats.items():
    best_way = max(value.items(), key=operator.itemgetter(1))[0]
    pending_way = min(value.items(), key=operator.itemgetter(1))[0]
    print('%s 最成功的數位行銷方式是 %s, 尚待改善的是%s' %(key, best_way, pending_way))
    print(pending_way)


# 關鍵字的排行分析
# 搜尋結果=0的關鍵字，各自搜尋累計次數
search = results['search'].set_index('keyword')

search.plot.pie(y='search_count', title='Search No Match keyword', legend=False, autopct='%1.1f%%')
plt.show()


# 購物籃分析
# 交易訂單的商品集合(相同商品只紀錄一次)
# 同一筆訂單ID 所購買的商品合併在同一列
product_sets= results['order'].groupby('order_id').agg({'product_id':''.join})
order_sets=[]
for products in product_sets['product_id']:
    order_sets.append(set(products))

# 所有商品的集合
all_sets = set(results['order']['product_id'])

# 關聯式分析(min_support=0.4, min_confidence=0.7)
all_sets, rules = apriori(order_sets, min_support=0.4, min_confidence=0.7)

print(rules)
# 推測: B 和 C 商品可以擺放在相同區域