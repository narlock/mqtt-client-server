"""
client.py
Author: Anthony Narlock

Purpose: Serves as a client MQTT application. A multi-threaded
client can both receive messages from the server and send messages
to it.
"""

import threading
import socket
import sys

class Client:
    """
    Constructor for client. The user can give a command-line argument for
    determining the localhost port to connect to. If none is specified, the
    default port will be 8080.
    """
    def __init__(self, port = 8092):
        """
        Initialize the client socket using TCP
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        """
        Attempt to connect to the server, if the connection fails, then handle
        the exception.
        """
        try:
            self.sock.connect( ('localhost', port) )
        except socket.error:
            print("Unable to connect to server. Server may be down or on a different port")
            sys.exit(1)

        """
        Begin the receive thread, allowing to receive messages from server
        Begin the write thread, allowing to send messages to the server
        """
        self.receive_thread = threading.Thread(target=self.receive)
        self.write_thread = threading.Thread(target=self.write)

        """
        Ready to disconnect flag will indicate when the client is ready to disconnect.
        This will be used to confirm to stop the write thread properly.
        """
        self.ready_to_disconnect = False

        """
        Start both receive and write threads
        """
        self.receive_thread.start()
        self.write_thread.start()
    
    """
    receive()

    This function serves the purpose of handling received messages from the server.
    Depending on the message, the client will perform some action, like sending
    acknoledgement to the server, or printing the message.

    Error handling is added in case the receive function does not work, in which
    the socket will close and the terminate safely.
    """
    def receive(self):
        while not self.ready_to_disconnect:
            try:
                message = self.sock.recv(1024).decode('utf-8')
                # print("debug: message=" + message)

                if message == 'CONN_ACK':
                    # Connection accepted, let's acknoledge it
                    print("CONN_ACK received from server")
                    self.sock.send("CONN_ACK accepted by client".encode('utf-8'))
                elif message == 'DISC_ACK':
                    print("DISC_ACK received from server, press enter to close socket")
                    self.sock.send("DISC_ACK accepted by client".encode('utf-8'))
                    self.ready_to_disconnect = True
                else:
                    print(message)
            except:
                print("An error occurred.")
                self.sock.close()
                sys.exit(1)

    """
    write()

    This function serves the purpose of writing messages (commands) to the server.
    This thread will run concurrently with the receive thread. Upon disconnecting,
    the user will acknoledge disconnection after half-closed state has been reached.
    """
    def write(self):
        while not self.ready_to_disconnect:
            message = input()
            if self.ready_to_disconnect:
                self.stop()
            else:
                self.sock.send(message.encode('utf-8'))
        print("Closed write thread")
    
    """
    stop()

    This function safely closes the socket and exits.
    """
    def stop(self):
        self.sock.close()
        print("socket closed")
        sys.exit(0)

client = Client()