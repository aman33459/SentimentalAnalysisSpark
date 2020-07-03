import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
ACCESS_TOKEN = '1464757897-fFwBQQOPFp6uIiBO7RAKEvr1gLLxwYWVb4ZQSjG'
ACCESS_SECRET = 'SXFfvVqxu0ARMADFBSXit4muWuTS37DZlKNGsv8ulgv4m'
CONSUMER_KEY = 's3NZKvRW75Ec1n5Amppe4zjLv'
CONSUMER_SECRET = 'E20pKAcxUrqArrvhoLHj2W8teCcCEiAtlGLtFKeg2sRFMu7r9i'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            full_tweet = full_tweet['text']
            full_tweet = full_tweet + '\n'
            tweet_text = full_tweet.encode() # pyspark can't accept stream, add '\n'
            #print("Tweet Text: " + tweet_text)
            #print ("------------------------------------------")
            tcp_connection.send(tweet_text );
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    #query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_data = [('locations', '68.116667 ,8.066667,97.416667,37.100000'), ('track', "#")] #this location value is San Francisco & NYC
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
'''
print(resp)
for line in resp.iter_lines():
    try:
        full_tweet = json.loads(line)
        tweet_text = full_tweet['text']
        print(tweet_text)
        tweet_text = tweet_text.encode() # pyspark can't accept stream, add '\n'
        print(tweet_text)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
'''
send_tweets_to_spark(resp,conn)
