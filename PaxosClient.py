import sys
import SocketServer
import socket
import json
from message import *
import time

class PaxosClient(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

    timeout = 10
    leader = 1
    lock = set()
    command_id = 0
    daemon_threads = True
    allow_reuse_address = True
    executed_commands = set()

    def __init__(self, server_address, RequestHandlerClass, setting):
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)
        self.command_list = setting['command_list']
        self.client_id = setting['client_id']
        if len(self.command_list) > 0:
            self.execute_command()

    def log(self, log):
        print "Client %s: %s" % (str(self.client_id), log)

    def sendToServer(self, server_id, msg_type, params):
        params["type"] = msg_type
        params["client_id"] = self.client_id
        msg = params.copy()
        msgStr = json.dumps(msg)

        self.log("sendToServer %s: %s" % (str(server_id), msgStr))

        HOST, PORT = "localhost", 9000 + server_id
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            # Connect to server and send data
            sock.connect((HOST, PORT))
            sock.sendall(msgStr + "\n")
        finally:
            sock.close()

    def execute_command(self):
        if self.command_id >= len(self.command_list):
            return #do nothing

        cmdStr = self.command_list[self.command_id]
        cmd = cmdStr.split('_')
        if cmd[0] == 'lock':
            #send message to leader that it wants to get the lock
            params = {}
            params["client_command_id"] = self.command_id
            params["command"] = cmdStr
            self.sendToServer(self.leader, CLIENT_REQUEST, params)
        elif cmd[0] == 'unlock':
            #check if we get the lock
            if cmd[1] in self.lock:
                #let leader know that it wants to unlock
                params = {}
                params["client_command_id"] = self.command_id
                params["command"] = cmdStr
                self.sendToServer(self.leader, CLIENT_REQUEST, params)
        elif cmd[0] == 'sleep':
            #sleep only when it gets the lock (for testing)
            self.log("===========  SLEEP %s  =========== " % cmd[1])
            time.sleep(int(cmd[1]))
            self.log("===========  WAKEUP %s  =========== " % cmd[1])
            #del self.command_list[0]
            self.command_id += 1
            self.execute_command()

    def handle_timeout(self):
        #ask the leader's neighbor to see who is the new leader
        self.leader = (self.leader + 1) % 9
        self.execute_command()


    def handle_message(self, msg):

        if msg['type'] == EXECUTED:
            if msg['client_command_id'] in self.executed_commands:
                return
            else:
                self.executed_commands.add(msg['client_command_id'])
            cmdStr = self.command_list[msg['client_command_id']]
            cmd = cmdStr.split('_')
            if cmd[0] == 'lock':
                self.lock.add(cmd[1])
                print ''
                self.log("===========  LOCK %s  =========== " % cmd[1])
                print ''
            elif cmd[0] == 'unlock':
                self.lock.remove(cmd[1])
                print ''
                self.log("=========== UNLOCK %s =========== " % cmd[1])
                print ''
            #del self.command_list[0]
            self.command_id +=1
            #if len(self.command_list) > 0:
            self.execute_command()
        #get response from the server about the current leader
        #if the leader is not the same, change the leader id and resend the message
        elif msg['type'] == PLEASE_ASK_LEADER:

            if self.leader != msg['current_leader_id']:
                self.leader = msg['current_leader_id']
                self.execute_command()


class MsgHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        msgStr = self.request.recv(1024).strip()
        self.server.log("Receive data: %s" % msgStr)
        msg = json.loads(msgStr)
        self.server.handle_message(msg)

def initialize_server():
    server_setting = {}
    client_id = int(sys.argv[1])
    server_setting['client_id'] = client_id
    server_setting['command_list'] = sys.argv[2].split(' ')
    return PaxosClient(('localhost', 8000 + client_id), MsgHandler, server_setting)

def running(server):
    try:
        while True:
            server.handle_request()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: PaxosClient.py client_id command_list"
        sys.exit(0)
    server = initialize_server()
    running(server)
