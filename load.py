from kafka import KafkaConsumer
import mysql.connector

TOPIC='twitter'
DATABASE = 'twitter'
USERNAME = 'root'
PASSWORD = '123'

print("Connecting to the database")
try:
    connection = mysql.connector.connect(host='localhost', database=DATABASE, user=USERNAME, password=PASSWORD)
except Exception:
    print("Could not connect to database. Please check credentials")
else:
    print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    message = msg.value.decode("utf-8")

    try:
        (now,tweet_id,created_at,tweet_text_sent,retweet_count,fav_count,media_source,link,user_id,user_name,user_location) = message.split(",")
    except:
        print("Fix location")
        split = message.split(",")
        user_location = split[-2] + " -"+ split[-1]
        (now,tweet_id,created_at,tweet_text_sent,retweet_count,fav_count,media_source,link,user_id,user_name) = split[:10]
    print(message)

    # Insert value to table user
    sql = "insert into user values(%s,%s,%s)"
    try:
        result = cursor.execute(sql, (user_id, user_name, user_location))
        print(f"A {user_name} was inserted into the database")
    except Exception as e: print(e)

    # Insert value to table tweet
    sql = "insert into tweet values(%s,%s,%s,%s,%s,%s,%s)"

    try:
        result = cursor.execute(sql, (tweet_id, created_at, tweet_text_sent, retweet_count, media_source, user_id, link))
        print(f"A {tweet_id} tweet was inserted into the database")
    except Exception as e: print(e)

    # Insert value to table tweet_favorite_count
    sql = "insert into tweet_favorite_count values(%s,%s,%s)"

    result = cursor.execute(sql, (now, tweet_id, fav_count))
    print(f"A {tweet_id} was updated new favorite count")



    connection.commit()

connection.close()
