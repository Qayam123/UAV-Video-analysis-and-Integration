import socket 
import json

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost',5050))
server.listen(1)
print("Server Started")
print("Waiting for Client")


while True:
   clientConnection, clientAddress = server.accept()
#print("Client Connected:" ,clientAddress)

   data = b''
   while True:
      tmp = clientConnection.recv(1024)
      if len(tmp) <=0:
         break
      data += tmp
      print(data.decode("utf-8"))

