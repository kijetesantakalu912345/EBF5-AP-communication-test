from basic_socket import BasicSocket

HOST = "localhost"
PORT = 4999


def client_test():
    client = BasicSocket()
    client.connect(HOST, PORT)
    client.send_utf8("4 byte length and then UTF-8 text string packet send from the client.")
    server_message = client.receive_utf8()
    print(f'server reply: "{server_message}"')
    client.close()


if __name__ == "__main__":
    client_test()
