from kafka import KafkaProducer
from datetime import datetime, timedelta
import json
from flatten_json import flatten
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import pandas as pd
from textblob import TextBlob
import re

TOPIC = 'twitter'
        
tracklist=["#ronaldo, #messi, #m10, #cr7"]

month = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12"
}

def get_key(key_file_name):
    file1 = open(key_file_name, 'r')
    key_list = []
    Lines = file1.readlines()
    for line in Lines:
        key_list.append(line.strip())
    return key_list


def sentiment_score(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 1
    elif analysis.sentiment.polarity == 0:
        return 0
    else:
        return -1

def remove_url(txt):
    return " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", txt).split())

def get_link(text):
    text_spl = text.split(' ')
    link = ""
    for value in text_spl:
        if "https://" in value:
            link = link + " " + value
    return link

def getData(data):
    json_data = json.loads(data)
    flat = flatten(json_data)
    #User
    user_name = flat['user_name']
    user_id = flat['user_id_str']
    user_location = flat['user_location']
    
    #Tweet
    try:
        tweet_text = flat['retweeted_status_extended_tweet_full_text']
    except:
        tweet_text = flat['text']
    tweet_id = flat['id_str']
    created_at_spl = flat['created_at'].split(' ')
    created_at_string =  created_at_spl[5] + '-' + month[created_at_spl[1]] + '-' + created_at_spl[2] + ' ' + created_at_spl[3]
    created_at = datetime.strptime(created_at_string, "%Y-%m-%d %H:%M:%S") + timedelta(hours = 7)
    tweet_text_sent = tweet_text
    retweet_count = flat['retweet_count']
    try:
        fav_count = flat['quoted_status_favorite_count']
    except:
        try:
            fav_count = flat['retweeted_status_favorite_count']
        except:
            fav_count = flat['favorite_count']
    media_source = flat['source']
    link = get_link(tweet_text_sent)
    print(tweet_text_sent)
    print("Transform to:")
    tweet_text_sent = remove_url(tweet_text_sent)
    print(tweet_text_sent)
    print("")
    result_score = sentiment_score(tweet_text_sent)
    
    now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    message = f"{now},{tweet_id},{created_at},{tweet_text_sent},{retweet_count},{fav_count},{media_source},{link},{user_id},{user_name},{user_location}"
    message = bytearray(message.encode("utf-8"))
    producer.send(TOPIC, message)    
    
class StdOutListener(StreamListener):
      
    def on_data(self, data):
        getData(data)
        return True

    def on_error(self, status):
        print(status)

if __name__ == "__main__":
    consumer_key, consumer_secret, access_token, access_token_secret = get_key('key.txt')
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=tracklist)
