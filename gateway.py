import grpc
from concurrent import futures
import dht_pb2
import dht_pb2_grpc
import hashlib

class NodeServicer(dht_pb2_grpc.NodeServicer):
    def SendInt(self, request, context):
        # Extract the integer sent by the client
        number = request.number
        print(f"Received number: {number}")
        
        # Calculate the SHA-256 hash of the integer
        hash_object = hashlib.sha256(str(number).encode())
        hash_hex = hash_object.hexdigest()
        print(f"Calculated hash: {hash_hex}")
        self.send_hash_to_another_server(number, hash_hex)
        
        # Respond to the client
        return dht_pb2.IntResponse(message=f"Number {number} received successfully with hash {hash_hex}")
    
    def send_hash_to_another_server(self, number, hash_hex):
        # Connect to the other gRPC server running on port 55005
        with grpc.insecure_channel('localhost:55000') as channel:
            stub = dht_pb2_grpc.NodeStub(channel)
            
            # Send the hash to the other server
            response = stub.ReceiveHash(dht_pb2.HashRequest(value = number, hash=hash_hex))
            print(f"Hash sent to port 55000, server response: {response.message}")

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Register the servicer to the server
    dht_pb2_grpc.add_NodeServicer_to_server(NodeServicer(), server)
    
    # Bind the server to the port 50000
    server.add_insecure_port('[::]:50000')
    server.start()
    print("Server started on port 50000")
    
    # Keep the server running
    server.wait_for_termination()

if __name__ == '__main__':
    serve()