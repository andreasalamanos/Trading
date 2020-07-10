
import socket
import TwitterAccess as ta
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time




HASHTAGS_FILE = './hashtags.txt'
hash_tags = [tag.split('\n')[0] for tag in open(HASHTAGS_FILE).readlines()]
print(hash_tags)



def stream( c_socket):
  auth = OAuthHandler(ta.API_key, ta.API_secret_key)
  auth.set_access_token(ta.Access_token, ta.Access_token_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track = hash_tags, languages = ['en'])



class TweetsListener(StreamListener):

    def __init__(self, csocket):
        super(TweetsListener, self).__init__()
        self.num_tweets = 0
        self.client_socket = csocket

    def on_data(self, data):
        try:
            jtweet = json.loads(data)
            if not jtweet['text'].strip().startswith('RT '):
                data = jtweet['text']
                data = (" ").join(data.split() + ['mydelimiter'])
                self.num_tweets += 1
                if self.num_tweets < 20:
                    if len(data.split())>4:
                        print(data)
                        time.sleep(0.03)
                        self.client_socket.send(data.encode('utf-8') )
                        return True
                else:
                    print('False')
                    return False
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            return True


    def on_error(self, status):
      print(status)
      return True




if __name__ == "__main__":
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  host,port  = "localhost",  9990
  s.bind((host, port))
  s.listen(5)
  c, addr = s.accept()
  print("Start streaming")
  stream(c)












