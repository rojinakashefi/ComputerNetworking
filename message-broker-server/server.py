import socket
import threading
import json
import time

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 1373  # Port to listen on (non-privileged ports are > 1023)
ENCODING = 'UTF-8'
topics_clients = dict()  # key topic value list of clients
all_clients = dict()
clients_ping_pong = dict()  # Dic for pinging all clients after three times not respondinclose the connection
publish_queue = dict()


def client_handler(conn, addr):
  print('client connected : ', addr)
  while True:
    try:
      data = conn.recv(1024)
      # EOF
      if not data:
        continue
      data = data.decode(ENCODING)
      data = json.loads(data)
      execute_cmd(data, conn)
    except:
      print('client disconnected :', addr)
      remove_client(conn)
      break


# data command/topic/message
def execute_cmd(data, conn):
  if data["command"] == "subscribe":
    subscribe(data["topics"], conn)
  if data["command"] == "publish":
    publish(data["topic"], data["massage"], conn)
  if data["command"] == "ping":
    pong(conn)
  if data['command'] == 'messageack':
    print("topic queue cleared")
    publish_queue[data['topic']] = []
  if data["command"] == "pong":
    # if we get a pong we see the client is available = true
    all_clients[conn] = 0
    clients_ping_pong[conn] = True


def subscribe(topics, conn):
  for topic in topics:
    if topic in topics_clients:
      if conn not in topics_clients[topic]:
        topics_clients[topic].append(conn)
    else:
      topics_clients[topic] = [conn]
  print(topics_clients)
  msg = {"command": "SubAck", "topics": topics}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))


def publish(topic, message, conn):
  publish_queue[topic] = [message]

  msg = {"command": "PubAck"}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))

  msg = {"command": "Message", "topic": topic, "message": message}
  j_msg = json.dumps(msg)
  for client in topics_clients[topic]:
    try:
      client.send(bytes(j_msg, encoding=ENCODING))
    except:
      remove_client(client)



def ping(conn):
  msg = {"command": "ping"}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))


def pong(conn):
  msg = {"command": "pong"}
  j_msg = json.dumps(msg)
  conn.send(bytes(j_msg, encoding=ENCODING))


def remove_client(client):
  client.close()
  for topic in topics_clients:
    if client in topics_clients[topic]:
      topics_clients[topic].remove(client)
  if client in all_clients:
    del all_clients[client]



def ping_all_clients():
  # clients_ping_pong a dict of key: client value: got a pong or not
  while True:
    for client in list(all_clients):
      if all_clients[client] == 3:
        remove_client(client)
      else:
        all_clients[client] += 1
        clients_ping_pong[client] = False
        ping(client)
    time.sleep(3)
    print("ping all clients for one round done. the ones we didn't got pong:")
    for client in all_clients:
      if not clients_ping_pong[client]:
        print(client)



def main():
  # network service ipv4
  # TCP
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind((HOST, PORT))
  s.listen()
  # Bonus3 : another thread for checking if clients are available or not
  threading.Thread(target=ping_all_clients).start()
  return s


s = main()

while True:
  conn, addr = s.accept()
  # Bonus3 : if a client doesn't respond 3 times, the connection must be closed.
  all_clients[conn] = 0
  # responed to ping or not
  clients_ping_pong[conn] = False
  threading.Thread(target=client_handler, args=(conn, addr)).start()
