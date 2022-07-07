import socket
import sys
import json
ENCODING = 'UTF-8'
HOST = sys.argv[1]  # The server's hostname or IP address
PORT = sys.argv[2]  # The port used by the server
command = None
conn = None


def parser():
  global command
  command = sys.argv[3]
  if command == 'subscribe' or command == 'publish' or command == 'ping':
    return sys.argv[4:]
  else:
    print("WRONG INPUT")
    exit(-1)


def subscribe(topics):
  msg = {"command": command, "topics": topics}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))


def publish(topic, massage):
  msg = {"command": command, "topic": topic, "massage": massage}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))


def ping():
  msg = {"command": 'ping'}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))


# subscriber
def message_ack(topic):
  msg = {"command": "messageack", "topic": topic}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))


def pong():
  msg = {"command": "pong"}
  j_msg = json.dumps(msg)
  # conn.send(bytes(j_msg, encoding=ENCODING))


def connect():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((HOST, int(PORT)))
  return s


def execute_cmd():
  inputs = parser()
  if command == "subscribe":
    subscribe(inputs)
  elif command == "publish":
    publish(inputs[0], inputs[1])
  elif command == "ping":
    ping()
  try:
    listen()
  except socket.error:
    print("TIMEOUT: No response from server")


def listen():
  conn.settimeout(10.0)
  while True:
    data = conn.recv(1024)
    # data = data.decode("utf-8")
    if not data:
      # break or try catch
      continue
    conn.settimeout(None)
    data = data.decode("utf-8")
    data = json.loads(data)
    if data['command'] == 'SubAck':
      success_topics = data['topics']
      print("Subscribing on " + ' '.join(str(x) for x in success_topics))
    elif data['command'] == 'Message':
      print(data['topic'] + ": " + data['message'])
      message_ack(data['topic'])
    elif data['command'] == 'PubAck':
      print("your message published successfully")
      break
    elif data['command'] == 'pong':
      print("pong received")
      break
    elif data['command'] == 'ping':
      pong()
    else:
      print("INVALID MESSAGE FROM SERVER")


conn = connect()
execute_cmd()
