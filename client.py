import grpc
import dht_pb2
import dht_pb2_grpc

def run():
    # Connect to the server
    with grpc.insecure_channel('localhost:50000') as channel:
        stub = dht_pb2_grpc.NodeStub(channel)
        
        # The integer to send
        number = 42  # Replace with your integer
        
        # Send the integer to the server
        response = stub.SendInt(dht_pb2.IntRequest(number=number))
        print(f"Server response: {response.message}")

if __name__ == '__main__':
    run()