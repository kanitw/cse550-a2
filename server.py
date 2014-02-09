import sys
import SocketServer


class SimpleServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

    timeout = 10

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass):
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)

    def handle_timeout(self):
        print 'Timeout!'

#TODO move this to another file
class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        print "{} wrote:".format(self.client_address[0])
        print self.data
        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())

def running():
    server = SimpleServer(('localhost', 9999), MyTCPHandler)
    try:
        while True:
            server.handle_request()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    running()
    #HOST, PORT = "localhost", 9999

    # Create the server, binding to localhost on port 9999
    #server = SimpleServer((HOST, PORT), MyTCPHandler)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    #server.serve_forever()