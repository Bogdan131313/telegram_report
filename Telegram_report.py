import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from read_db.CH import Getch
from datetime import datetime, date, timedelta

group_chat_id = *********
my_chat_id = 267912614

def my_test_report(chat=None, message=None, graph_list=None, report_date=None):
    chat_id = chat
    bot = telegram.Bot(token='5351849446:AAFMF9leWqJU7qY5IINEceJnjiGPWddZI0k')
    #report_date = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')


    bot.sendMessage(chat_id=chat_id, text=message)

    # Will gather buffer index values for every files created and send together
    for graph in graph_list:
        bot.sendPhoto(chat_id=chat_id, photo=graph)

    for doc in docs_list:
         bot.sendDocument(chat_id=chat_id, document=doc)


# Getting report date
report_date = datetime.strftime(datetime.now() - timedelta(days=1, hours=-3), '%d-%m-%Y')


# Yesterday scalar values (Block)

# Feed Active Users
q = """ SELECT count(DISTINCT user_id) AS "Unique users"
FROM simulator_20220420.feed_actions
WHERE toDate(now()) - 1 = toDate(time); """

DAU_feed =  Getch(query=q).df.iloc[0,0]


# Messenger Active Users
q_1 = """ SELECT count(DISTINCT user_id) AS "Unique users"
FROM simulator_20220420.message_actions
WHERE toDate(now()) - 1 = toDate(time); """

DAU_messenger =  Getch(query=q_1).df.iloc[0,0]


# View/Like CTR
q_0 = """ SELECT round(countIf(action = 'like')*100/countIf(action = 'view'), 2) as CTR 
FROM simulator_20220420.feed_actions
WHERE toDate(now()) - 1 = toDate(time); """

CTR = Getch(query=q_0).df.iloc[0,0]


#  Views
q_2 = """ SELECT count(*) AS "Views"
FROM simulator_20220420.feed_actions
WHERE toDate(now()) - 1 = toDate(time)
AND action = 'view'; """

Views = Getch(query=q_2).df.iloc[0,0]


# Likes
q_3 = """ SELECT count(*) AS "Likes"
FROM simulator_20220420.feed_actions
WHERE toDate(now()) - 1 = toDate(time)
AND action = 'like'; """

Likes = Getch(query=q_3).df.iloc[0,0]


# Messages
q_4 = """ SELECT count(*) AS "Messages"
FROM simulator_20220420.message_actions
WHERE toDate(now()) - 1 = toDate(time); """

Messages = Getch(query=q_4).df.iloc[0,0]


# Feed and Messenger
q_5 = """ SELECT countIf(use_type = 'Both'
               and date1 = toDate(now()) - 1) AS "Feed and Messenger"
FROM
  (SELECT distinct feed_user,
                   messenger_user,
                   date1,
                   use_type
   FROM
     (SELECT f.user_id feed_user,
             m.user_id messenger_user,
             f.fdate,
             m.mdate,
             multiIf (f.fdate >= m.mdate, toDate(f.fdate), toDate(m.mdate)) date1,
             multiIf(messenger_user = 0
                     AND feed_user > 0, 'Feed only', messenger_user > 0
                     AND feed_user = 0, 'Messenger only', messenger_user > 0
                     AND feed_user > 0, 'Both', 'OutOfType') use_type
      FROM
        (SELECT DISTINCT user_id,
                         toDate(time) fdate
         FROM simulator_20220420.feed_actions) f
      FULL JOIN
        (SELECT DISTINCT user_id,
                         toDate(time) mdate
         FROM simulator_20220420.message_actions) m ON f.user_id = m.user_id
      AND f.fdate = m.mdate
      ORDER BY date1 DESC)) AS virtual_table; """

Feed_Message = Getch(query=q_5).df.iloc[0,0]


# Retention
q_6 = """ SELECT date_, start_date, Duration, users_number/first_day_num AS Retention 
FROM (

    SELECT  start_date, 
            date_, 
            COUNT(DISTINCT user_id) AS users_number, 
            Duration, MAX(users_number) OVER(PARTITION BY start_date) AS first_day_num 
      FROM (

                WITH start AS (
                                SELECT DISTINCT user_id, 
                                        MIN(toDate(time)) OVER(PARTITION BY user_id) AS start_date 
                                FROM simulator_20220420.feed_actions
                                ),
                data AS (
                            SELECT DISTINCT user_id, 
                                            toDate(time) AS date_ 
                            FROM simulator_20220420.feed_actions 
                            ORDER BY user_id ASC
                            )

                SELECT t1.user_id, start_date, date_, (date_ - toDate(start_date) + 1) AS Duration
                FROM data t1 JOIN start t2 ON t1.user_id = t2.user_id
                WHERE Duration <=7
    
          ) GROUP BY date_, start_date, Duration ORDER BY start_date, Duration
      )
  ORDER BY  start_date, Duration; """


# Output a list with retention values for users who first appeared in the feed a week ago
Retention = Getch(query=q_6).df
Retention_pivoted = pd.pivot(index='start_date', columns='Duration', data=Retention.iloc[:, 1:])


days = ['Day 1', 'Day 2', 'Day 3', 'Day 4', 'Day 5', 'Day 6', 'Day 7']
Retention_pivoted.columns.set_levels(days, level=1, inplace=True)

#day7 = (pd.to_datetime('now') -pd.Timedelta(hours=-3, days=7)).strftime("%Y-%m-%d")
day7 = Retention_pivoted.index[-8] # date of the first day for users who's first week ended yesterday
Retention_values = Retention_pivoted.loc[Retention_pivoted.index == day7].values.reshape((7)).round(2)

last_week_retention = {k:v for k, v in zip(days, Retention_values)} # dictionary: {Day_№#:Retention}



message = f'Report №-2 for {report_date}:\nFeed DAU = {DAU_feed}\n\
            Messenger DAU = {DAU_messenger}\nViews = {Views}\n\
            Likes = {Likes}\nCTR = {CTR}%\nMessages = {Messages}\n\
            Engaged in both feed and messenger = {Feed_Message}\n\
            Last week retention = {last_week_retention}'


# Charts (Block)
graph_list = []


# Views per day
q_7 = """ SELECT toDate(time) AS Date, countIf(action = 'view') AS "Quantity"
FROM simulator_20220420.feed_actions
WHERE Date BETWEEN toDate(now()) - 7 AND toDate(now()) - 1 
GROUP BY Date; """


# Likes per day
q_8 = """ SELECT toDate(time) AS Date, countIf(action = 'like') AS "Quantity"
FROM simulator_20220420.feed_actions
WHERE Date BETWEEN toDate(now()) - 7 AND toDate(now()) - 1 
GROUP BY Date; """


font_label = {"fontname" : "Times New Roman", "fontsize" : 18} # dictionary with parameters for titles


sns.set_style('whitegrid')

fig, axes = plt.subplots(2,1, figsize=(12,10), subplot_kw={'frame_on': True})

start_report_date = datetime.strftime(datetime.now() - timedelta(7), '%d-%m-%Y')
end_report_date = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')

fig.suptitle(f'Views/Likes for  "{start_report_date} - {end_report_date}"', fontsize = 20)


sns.lineplot(ax=axes[0], data = Getch(query=q_7).df, x='Date', y='Quantity', marker='o', color='navy')
axes[0].set_ylabel('Quantity', fontsize=14) # axes titles
axes[0].set_xlabel('', fontsize=14)
axes[0].tick_params(labelsize=12) # values parameters
axes[0].set_title('Views', fontdict=font_label) #  chart title

# Cause of variability of values it is not easy to choose place for values sometimes
#  But I think text values make chart easy to understand
"""for i,j in zip(Getch(query=q_7).df['Date'], Getch(query=q_7).df['Unique users']):
    axes[0].text(i+timedelta(hours=2),j+200,f'{j}')"""


sns.lineplot(ax=axes[1], data = Getch(query=q_8).df, x='Date', y='Quantity', marker='o', color='violet')
axes[1].set_ylabel('Quantity', fontsize=14)
axes[1].set_xlabel('Date', fontsize=14)
axes[1].tick_params(labelsize=12)
axes[1].set_title('Likes', fontdict=font_label)

"""for i,j in zip(Getch(query=q_8).df['Date'], Getch(query=q_8).df['Unique users']):
    axes[1].text(i+timedelta(hours=2), j+20, f'{j}')"""


fig.tight_layout()

plot_DAU = io.BytesIO()
plt.savefig(plot_DAU)
plot_DAU.seek(0)
plot_DAU.name = 'plot_DAU.png'
plt.close()
graph_list.append(plot_DAU)


# View/Like CTR
q_9 = """ SELECT toDate(time) AS Date, round(countIf(action = 'like')*100/countIf(action = 'view'), 2) as CTR 
FROM simulator_20220420.feed_actions
WHERE Date BETWEEN toDate(now()) - 7 AND toDate(now()) - 1 
GROUP BY Date; """


fig, axes = plt.subplots(figsize=(12,10))

start_report_date = datetime.strftime(datetime.now() - timedelta(7), '%d-%m-%Y')
end_report_date = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')

fig.suptitle(f'CTR for  "{start_report_date} - {end_report_date}"', fontsize = 20)


sns.lineplot(data = Getch(query=q_9).df, x='Date', y='CTR', marker='o', color='black')
axes.set_ylabel('CTR, %', fontsize=14)
axes.set_xlabel('Date', fontsize=14)
axes.tick_params(labelsize=12)

"""for i,j in zip(Getch(query=q_9).df['Date'], Getch(query=q_9).df['CTR']):
    plt.text(i+timedelta(hours=2), j+1000, f'{j}')"""

fig.tight_layout()

plot_CTR = io.BytesIO()
plt.savefig(plot_CTR)
plot_CTR.seek(0)
plot_CTR.name = 'plot_CTR.png'
plt.close()
graph_list.append(plot_CTR)


# DAU Feed/Messenger
# Feed
q_10 = """ SELECT toDate(time) AS Date, count(DISTINCT user_id) AS "Unique users"
FROM simulator_20220420.feed_actions
WHERE Date BETWEEN toDate(now()) - 7 AND toDate(now()) - 1 
GROUP BY Date; """

#Messenger
q_11 = """ SELECT toDate(time) AS Date, count(DISTINCT user_id) AS "Unique users"
FROM simulator_20220420.message_actions
WHERE Date BETWEEN toDate(now()) - 7 AND toDate(now()) - 1 
GROUP BY Date; """


fig, axes = plt.subplots(2,1, figsize=(12,10), subplot_kw={'frame_on': True})

start_report_date = datetime.strftime(datetime.now() - timedelta(7), '%d-%m-%Y')
end_report_date = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')

fig.suptitle(f'DAU for  "{start_report_date} - {end_report_date}"', fontsize=20)


sns.lineplot(ax=axes[0], data = Getch(query=q_10).df, x='Date', y='Unique users', marker='o', color='purple')
axes[0].set_ylabel('Unique users', fontsize=14)
axes[0].set_xlabel('', fontsize=14)
axes[0].tick_params(labelsize=12)
axes[0].set_title('Feed', fontdict=font_label)

"""for i,j in zip(Getch(query=q_10).df['Date'], Getch(query=q_10).df['Unique users']):
    axes[0].text(i+timedelta(hours=2),j+200,f'{j}')"""


sns.lineplot(ax=axes[1], data = Getch(query=q_11).df, x='Date', y='Unique users', marker='o', color='goldenrod')
axes[1].set_ylabel('Unique users', fontsize=14)
axes[1].set_xlabel('Date', fontsize=14)
axes[1].tick_params(labelsize=12)
axes[1].set_title('Messenger', fontdict=font_label)

"""for i,j in zip(Getch(query=q_11).df['Date'], Getch(query=q_11).df['Unique users']):
    axes[1].text(i+timedelta(hours=2), j+20, f'{j}')"""


fig.tight_layout()

plot_DAU = io.BytesIO()
plt.savefig(plot_DAU)
plot_DAU.seek(0)
plot_DAU.name = 'plot_DAU.png'
plt.close()
graph_list.append(plot_DAU)


# Messages per day
q_12 = """ SELECT toDate(time) AS Date, count(*) AS "Messages"
FROM simulator_20220420.message_actions
WHERE Date BETWEEN toDate(now()) - 7 AND toDate(now()) - 1 
GROUP BY Date; """


fig, axes = plt.subplots(figsize=(12,10))

start_report_date = datetime.strftime(datetime.now() - timedelta(7), '%d-%m-%Y')
end_report_date = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')

fig.suptitle(f'Messages for  "{start_report_date} - {end_report_date}"', fontsize=20)


sns.lineplot(data = Getch(query=q_12).df, x='Date', y='Messages', marker='o', color='black', label='Messages')
axes.set_ylabel('Quantity', fontsize=14)
axes.set_xlabel('Date', fontsize=14)
axes.tick_params(labelsize=12)

"""for i,j in zip(Getch(query=q_12).df['Date'], Getch(query=q_12).df['Messages']):
    plt.text(i+timedelta(hours=2), j+1000, f'{j}')"""

fig.tight_layout()

plot_CTR = io.BytesIO()
plt.savefig(plot_CTR)
plot_CTR.seek(0)
plot_CTR.name = 'plot_CTR.png'
plt.close()
graph_list.append(plot_CTR)


# Tables for docs
docs_list = []


# Top 100 posts
q_13 = """ SELECT post_id AS post_id,
       countIf(action = 'view') AS "Views",
       countIf(action = 'like') AS "Likes",
       countIf(action = 'like')/ countIf(action = 'view') AS "Like/View CTR for TOP",
       count(DISTINCT user_id) AS "Unique users"
FROM simulator_20220420.feed_actions
GROUP BY post_id
ORDER BY "Views" DESC
LIMIT 100; """


Top_100_posts = Getch(query=q_13).df
file_object = io.StringIO()
Top_100_posts.to_csv(file_object)
file_object.name = 'Top_100_posts.csv'
file_object.seek(0)
docs_list.append(file_object)


# Top 100 mailers
q_14 = """ SELECT user_id AS user_id,
       count(user_id) AS "Sent msgs"
FROM simulator_20220420.message_actions
GROUP BY user_id
ORDER BY "Sent msgs" DESC
LIMIT 100; """


Top_100_mailers = Getch(query=q_14).df
file_object = io.StringIO()
Top_100_mailers.to_csv(file_object)
file_object.name = 'Top_100_mailers.csv'
file_object.seek(0)
docs_list.append(file_object)


# Top 100 receivers
q_15 = """ SELECT reciever_id AS reciever_id,
       count(reciever_id) AS "Received msgs"
FROM simulator_20220420.message_actions
GROUP BY reciever_id
ORDER BY "Received msgs" DESC
LIMIT 100; """


Top_100_receivers = Getch(query=q_15).df
file_object = io.StringIO()
Top_100_receivers.to_csv(file_object)
file_object.name = 'Top_100_receivers.csv'
file_object.seek(0)
docs_list.append(file_object)


# Top 100 feed
q_16 = """ SELECT user_id AS user_id,
       count(user_id) AS "Actions"
FROM simulator_20220420.feed_actions
GROUP BY user_id
ORDER BY "Actions" DESC
LIMIT 100; """


Top_100_feed = Getch(query=q_16).df
file_object = io.StringIO()
Top_100_feed.to_csv(file_object)
file_object.name = 'Top_100_feed.csv'
file_object.seek(0)
docs_list.append(file_object)


try:
    my_test_report(chat=my_chat_id, message=message, graph_list=graph_list )
except Exception as e:
    print(e)
