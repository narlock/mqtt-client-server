"""
server.py
Author: Anthony Narlock

Purpose: Serves as an MQTT server-side application. A multi-threaded
server for handling clients that can publish and subscribe to messages.
"""

import threading
import socket
import re

# Server setup
host = 'localhost'
port = 8092

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # TCP
server.bind( (host, port) ) # Binding to port and localhost
server.listen()

TOPICS = ['WEATHER', 'NEWS', 'HEALTH', 'SECURITY', 'WEATHER/MINNESOTA', 'WEATHER/WISCONSIN/NINE','WEATHER/MINNESOTA/NINE'] # Topics available to use
RETAINED_TOPIC_MESSAGE = ["","","","","","",""]
clients = [] #List of (client, address) tuples
client_subscriptions = [] # List of topic lists corresponding to each client index.

"""
e.g., if there is a client at index 0, and they subscribe to WEATHER,
then client_subscriptions[0] = ['WEATHER']. If they are subscribed to both WEATHER and NEWS,
it will be ['WEATHER', 'NEWS']. Order will not matter, the only thing that should matter
is if the index of the client contains a topic within their own list of subscriptions,
then we will publish a message to them...
"""

"""
handle(client)

This is the method that handles the client messages
that are sent to the server.
"""
def handle(client):
    while True:
        try:
            #Receive message command from client
            message = client.recv(1024).decode('utf-8')
            # print(message) # For debugging

            # Message command conditional statements
            if '/DISC' in message:
                """
                /DISC is the DISCONNECT command.
                When this command is received from the client, the client
                wants to disconnect. The server will acknoledge disconnect
                message and close the socket with the client.
                """
                client.send('DISC_ACK'.encode('utf-8'))
                disconnect_accepted = client.recv(1024).decode('utf-8')
                print(disconnect_accepted)
                index = clients.index( (client) )
                del clients[index]
                del client_subscriptions[index]
                client.close()
                break # Required or an exception will be thrown.
            elif (message[:4] == '/SUB'):
                """
                /SUB <TOPIC>
                When this command is received from the client, the client
                wants to subscribe to a topic. The server will add
                the client to the topic they wish to connect. The client
                must enter a valid topic. A client is also not able to
                subscribe to a topic they are already subscribed to.

                If the retained message for the topic is not the empty string,
                then there is no retained message. Thus, no message will be
                sent to the client. If there is a retained message, it will
                be sent to the client.
                """
                subscription_split = message.split(" ")
                if(len(subscription_split) != 2):
                    client.send(f'Invalid syntax: /SUB <TOPIC>'.encode('utf-8'))
                else:
                    if '/#' == subscription_split[1][-2:]:
                        subscribe_multilevel(subscription_split[1], client)
                    elif '+' in subscription_split[1] and subscription_split[1].count('+') == 1:
                        subscribe_singlelevel(subscription_split[1], client)
                    elif subscription_split[1] in TOPICS:
                        if subscription_split[1] in client_subscriptions[clients.index( (client) )]:
                            client.send(f'You are already subscribed to this topic!'.encode('utf-8'))
                        else:
                            client_subscriptions[clients.index( (client) )].append(subscription_split[1])
                            print(client_subscriptions[clients.index( (client) )])
                            client.send(f'Subscribed to [{subscription_split[1]}] {RETAINED_TOPIC_MESSAGE[TOPICS.index(subscription_split[1])]}'.encode('utf-8'))
                    else:
                        if '+' not in subscription_split[1] and '#' not in subscription_split[1]:
                            TOPICS.append(subscription_split[1])
                            RETAINED_TOPIC_MESSAGE.append("")
                            client_subscriptions[clients.index( (client) )].append(subscription_split[1])
                            print(TOPICS)
                            client.send(f'Subscribed to [{subscription_split[1]}] {RETAINED_TOPIC_MESSAGE[TOPICS.index(subscription_split[1])]}'.encode('utf-8'))
                        else:
                            client.send(f'Cannot create topic with +, # symbol.'.encode('utf-8'))

            elif (message[:4] == '/PUB' and message[:5] != '/PUBR'):
                """
                /PUB <TOPIC> <MESSAGE BODY>
                When this command is received from the client, the client
                wants to publish a message to a topic. They must enter
                a valid topic for the message to send. The client must
                also belong to that topic in order to send messages to it.
                Publishing a message to a valid topic will broadcast
                the message to each client that is subscribed to the
                respective topic.
                """
                subscription_split = re.split(r'\s+', message, 2)
                if len(subscription_split) == 3:
                    if '/#' == subscription_split[1][-2:] and subscription_split[1][:len(subscription_split[1])-2] in TOPICS:
                        broadcast_multilevel(subscription_split[1], subscription_split[2], client)
                    elif '+' in subscription_split[1] and subscription_split[1].count('+') == 1:
                        broadcast_singlelevel(subscription_split[1], subscription_split[2], client)
                    elif subscription_split[1] not in TOPICS:
                        client.send(f'Invalid topic.'.encode('utf-8'))
                    else:
                        if subscription_split[1] in client_subscriptions[clients.index( (client) )]:
                            broadcast(subscription_split[1], subscription_split[2])
                        else:
                            client.send(f'You are not subscribed to this topic.'.encode('utf-8'))
                else:
                    client.send(f'Invalid syntax: /PUB <TOPIC> <MESSAGE>'.encode('utf-8'))
            elif (message[:5] == '/PUBR'):
                """
                /PUBR <TOPIC> <MESSAGE BODY>

                Similar to publish, but this will put published message
                to be retained.
                This will send the message into the topic's retained message.
                This means that all connected users will receive the published message
                and this message will be retained. This means that clients that newly
                subscribe to this topic will receive this retained message.
                """
                subscription_split = re.split(r'\s+', message, 2)
                if len(subscription_split) == 3:
                    if '/#' == subscription_split[1][-2:] and subscription_split[1][:len(subscription_split[1])-2] in TOPICS:
                        broadcast_multilevel_retain(subscription_split[1], subscription_split[2], client)
                    elif '+' in subscription_split[1] and subscription_split[1].count('+') == 1:
                        broadcast_singlelevel_retain(subscription_split[1], subscription_split[2], client)
                    elif subscription_split[1] not in TOPICS:
                        client.send(f'Invalid topic.'.encode('utf-8'))
                    else:
                        if subscription_split[1] in client_subscriptions[clients.index( (client) )]:
                            retain_message(subscription_split[1], subscription_split[2])
                            broadcast(subscription_split[1], subscription_split[2])
                        else:
                            client.send(f'You are not subscribed to this topic.'.encode('utf-8'))
                else:
                    client.send(f'Invalid syntax: /PUBR <TOPIC> <MESSAGE>'.encode('utf-8'))
            elif (message[:6] == '/UNSUB'):
                """
                /UNSUB <TOPIC>
                When this command is received from the client, the client
                wants to unsubscribe from a topic. They must enter a topic
                in which they are already subscribed to. If they are not
                subscribed to the topic, the server will display an error.
                On success, the server will send a message to the user saying
                they have successfully unsubscribed from topic x.
                """
                subscription_split = message.split(" ")
                if(len(subscription_split) != 2):
                    client.send(f'Invalid syntax: /UNSUB <TOPIC>'.encode('utf-8'))
                else:
                    if '/#' == subscription_split[1][-2:] and subscription_split[1][:len(subscription_split[1])-2] in TOPICS:
                        unsubscribe_multilevel(subscription_split[1], client)
                    elif '+' in subscription_split[1] and subscription_split[1].count('+') == 1:
                        unsubscribe_singlelevel(subscription_split[1], client)
                    elif subscription_split[1] in client_subscriptions[clients.index( (client) )]:
                        client_subscriptions[clients.index( (client) )].remove(subscription_split[1])
                        client.send(f'Successfully unsubscribed from {subscription_split[1]}!'.encode('utf-8'))
                    else:
                        client.send(f'You are not subscribed to that topic.'.encode('utf-8'))
            elif (message[:5] == '/LIST'):
                """
                /LIST
                When this command is received from the client, the client
                wants to query the topics they are subscribed to. This will
                also display how many topics they are subscribed to.
                """
                client_topic_list = client_subscriptions[clients.index( (client) )]
                topic_string = ", ".join(str(topic) for topic in client_topic_list)
                client.send(f'Subscribed to {str(len(client_topic_list))} topics. {topic_string}'.encode('utf-8'))
            else:
                """
                If the user enters an invalid command, it is not
                supported by this system. The server will indicate
                that it is an invalid command.
                """
                client.send('Invalid command'.encode('utf-8'))
        except:
            """
            Under a sudden disconnection, we will handle closing the client
            """
            index = clients.index( (client) )
            del clients[index]
            del client_subscriptions[index]
            client.close()
            break

"""
broadcast(topic, message)

Broadcasts a message to clients in topic.
"""
def broadcast(topic, message):
    for client in clients:
        if topic in client_subscriptions[clients.index( (client) )]:
                client.send(f'[{topic}]: {message}'.encode('utf-8'))

"""
retain_message(topic, message)


Retains the message for the specified topic.
"""
def retain_message(topic, message):
    RETAINED_TOPIC_MESSAGE[TOPICS.index(topic)] = message
    print("Retaining Message!")

# QUERY WILD CARD TOPICS

"""
multilevel_topics(topic_input)

Returns a list of topics that match multilevel wildcard
"""
def multilevel_topics(topic_input):
    # Input is assumed to be valid, that is, it ends with /#
    topic_input = topic_input[:-1]
    topic_input_length = len(topic_input)
    topics = []
    # topics.append(topic_input[:-1])
    for topic in TOPICS:
        if topic_input == topic[:topic_input_length]:
            topics.append(topic)
    return topics

"""
singlelevel_topics(topic_input)

Returns a list of topics that match singlelevel wildcard
"""
def singlelevel_topics(topic_input):
    # Input is assumed to be valid
    front_back = topic_input.split("+")
    topics = []
    # topics.append(front_back[0][:-1])
    for topic in TOPICS:
        if front_back[0] == topic[:len(front_back[0])] and front_back[1] == topic[-len(front_back[1]):]:
            topics.append(topic)
    return topics

"""
client_topics(topic_list)

Returns a new list of topics that the client is subscribed to
"""
def client_topics(topic_list, client):
    client_topics = []
    for topic in topic_list:
        if topic in client_subscriptions[clients.index( (client) )]:
            client_topics.append(topic)
    return client_topics

# SUBSCRIBE WILD CARD

"""
subscribe_multilevel(topic_input, client)

Subscribes to topics that match multilevel wildcard
Does not re-subscribe to new topics.
"""
def subscribe_multilevel(topic_input, client):
    # This will not create any new topics, but only sub to ones that exist
    topics = multilevel_topics(topic_input)
    if len(topics) != 0:
        topics_to_string = "Subscribed to: \n"
        for topic in topics:
            if topic not in client_subscriptions[clients.index( (client) )]:
                topics_to_string += f'[{topic}] {RETAINED_TOPIC_MESSAGE[TOPICS.index(topic)]} \n'

        for topic in topics:
            if topic not in client_subscriptions[clients.index( (client) )]:
                client_subscriptions[clients.index( (client) )].append(topic)
        client.send(f'{topics_to_string}'.encode('utf-8'))
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

"""
subscribe_singlelevel(topic_input, client)

Subscribes to topics that match multilevel wildcard
"""
def subscribe_singlelevel(topic_input, client):
    # This will not create any new topics, but only sub to ones that exist
    topics = singlelevel_topics(topic_input)
    if len(topics) != 0:
        topics_to_string = "Subscribed to: "
        for topic in topics:
            if topic not in client_subscriptions[clients.index( (client) )]:
                topics_to_string += f'[{topic}] {RETAINED_TOPIC_MESSAGE[TOPICS.index(topic)]} \n'

        for topic in topics:
            if topic not in client_subscriptions[clients.index( (client) )]:
                client_subscriptions[clients.index( (client) )].append(topic)
        client.send(f'{topics_to_string}'.encode('utf-8'))
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

# UNSUBSCRIBE WILD CARD
"""
unsubscribe_multilevel(topic_input, client)

Unsubscribes to topics that match multilevel wildcard
"""
def unsubscribe_multilevel(topic_input, client):
    # This will not create any new topics, but only ones that exist
    topics = multilevel_topics(topic_input)
    if len(topics) != 0:
        topics_to_string = "Unsubscribed to: "
        for topic in topics:
            if topic in client_subscriptions[clients.index( (client) )]:
                topics_to_string += topic + ", "
        topics_to_string = topics_to_string[:-2] # Removes last , "

        for topic in topics:
            if topic in client_subscriptions[clients.index( (client) )]:
                client_subscriptions[clients.index( (client) )].remove(topic)
        client.send(f'{topics_to_string}'.encode('utf-8'))
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

"""
unsubscribe_singlelevel(topic_input, client)

Unsubscribes to topics that match singlelevel wildcard
"""
def unsubscribe_singlelevel(topic_input, client):
    # This will not create any new topics, but only ones that exist
    topics = singlelevel_topics(topic_input)
    if len(topics) != 0:
        topics_to_string = "Unsubscribed to: "
        for topic in topics:
            if topic in client_subscriptions[clients.index( (client) )]:
                topics_to_string += topic + ", "
        topics_to_string = topics_to_string[:-2] # Removes last , "

        for topic in topics:
            if topic in client_subscriptions[clients.index( (client) )]:
                client_subscriptions[clients.index( (client) )].remove(topic)
        client.send(f'{topics_to_string}'.encode('utf-8'))
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

# PUBLISH WILD CARD

"""
broadcast_multilevel(topic_input, message)

Broadcasts a message to many topics by multilevel wildcard
Only publishes to topics that are subscribed to by the client
"""
def broadcast_multilevel(topic_input, message, client):
    topics = multilevel_topics(topic_input)
    topics = client_topics(topics, client)
    if len(topics) != 0:
        for topic in topics:
            broadcast(topic, message)
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

"""
broadcast_singlelevel(topic_input, message)

Broadcasts a message to many topics by multilevel wildcard
Only publishes to topics that are subscribed to by the client
"""
def broadcast_singlelevel(topic_input, message, client):
    topics = singlelevel_topics(topic_input)
    topics = client_topics(topics, client)
    if len(topics) != 0:
        for topic in topics:
            broadcast(topic, message)
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

# PUBLISH RETAIN WILD CARD

"""
broadcast_multilevel_retain(topic_input, message)

Broadcasts a message to many topics by multilevel wildcard
Only publishes to topics that are subscribed to by the client
"""
def broadcast_multilevel_retain(topic_input, message, client):
    topics = multilevel_topics(topic_input)
    topics = client_topics(topics, client)
    if len(topics) != 0:
        for topic in topics:
            retain_message(topic, message)
            broadcast(topic, message)
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

"""
broadcast_singlelevel_retain(topic_input, message)

Broadcasts a message to many topics by multilevel wildcard
Only publishes to topics that are subscribed to by the client
"""
def broadcast_singlelevel_retain(topic_input, message, client):
    topics = singlelevel_topics(topic_input)
    topics = client_topics(topics, client)
    if len(topics) != 0:
        for topic in topics:
            retain_message(topic, message)
            broadcast(topic, message)
    else:
        client.send(f'No topic matches for {topic_input}.'.encode('utf-8'))

"""
receive()

This is the function that accepts client connections.
Upon receiving a connection, the client will be added
to a list of clients, and also given a list of subscriptions.
"""
def receive():
    while True:
        try:
            client, address = server.accept()
            # print(client)

            #Send connection acknoledgement message
            client.send('CONN_ACK'.encode('utf-8')) 
            connection_accepted = client.recv(1024).decode('utf-8')

            print(connection_accepted)
            clients.append(client)
            client_subscriptions.append([])

            # adding the client to a thread so we can have multiple clients on server concurrently
            thread = threading.Thread(target=handle, args=(client,))
            thread.start() 
        except KeyboardInterrupt:
            print("hello world")
            break

print(f'Server is listening on port {str(port)}')
receive()