import grpc
from concurrent import futures
import time
import dht_pb2
import dht_pb2_grpc

# Define the Node class which represents a single node in the DHT
class Node(dht_pb2_grpc.NodeServicer):
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.port = port
        self.successor = -1
        self.predecessor = -1
        self.successor_port = -1
        self.predecessor_port = -1
        # Dicionario contento os inteiros e seus hashes
        self.data = {}
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        dht_pb2_grpc.add_NodeServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'[::]:{port}')
        self.server.start()

    def Join(self, request, context):
        print(f"request port: {request.port}")
        print(f"request id: {request.node_id}")
        if request.node_id == self.node_id:
            print("Nó já existente.")
            return dht_pb2.JoinResponse(success=False)
        elif self.successor == 0 and request.node_id > self.node_id:
            print("Fim do anel!")
            # atualizar predecessor e sucessor do nó que entrou no anel e agora é o ultimo
            with grpc.insecure_channel(f'localhost:{request.port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = self.node_id))
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = 0))
            #####
            # atualizar predecessor e sucessor dos nós que já estão no anel
            with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id=request.node_id))         
            self.successor = request.node_id
            self.successor_port = 55000 + self.successor
            print(f"meu novo sucessor é {self.successor} na porta {self.successor_port}")
            #####
        elif request.node_id > node_id and self.successor != -1 :
            print(f"Forwarding request to node {self.successor} on port {self.successor_port}")
            with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.Join(dht_pb2.JoinRequest(node_id=request.node_id, port=request.port))
        elif request.node_id < self.node_id:
            # atualizar predecessor e sucessor do nó que entrou no anel
            with grpc.insecure_channel(f'localhost:{request.port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = self.predecessor))
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = self.node_id))
            #####
            # atualizar predecessor e sucessor dos nós que já estão no anel
            with grpc.insecure_channel(f'localhost:{self.predecessor_port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id=request.node_id))         
            self.predecessor = request.node_id
            self.predecessor_port = 55000 + request.node_id
            print(f"meu novo predecessor é {self.predecessor} na porta {self.predecessor_port}")
            #####
        return dht_pb2.JoinResponse(success=True)

    def QueryNode(self, request, context):
        # Return the successor and predecessor for the queried node
        return dht_pb2.NodeQueryResponse(successor=self.successor, predecessor=self.predecessor)

    def UpdateNode(self, request, context):
        # Update the successor and predecessor based on the provided node ID
        if self.node_id == 0:
            new_id = request.node_id
            if self.predecessor == -1 and self.successor == -1:
                self.successor = new_id
                self.predecessor = new_id
                self.successor_port = 55000 + new_id
                self.predecessor_port = 55000 + new_id
                print(f"Node 0 updated: Successor set to {new_id} with port {self.successor_port}")
                print(f"Node 0 updated: Predecessor set to {new_id} with port {self.predecessor_port}")
            else:
                # If node 0 was already part of the ring, update successor and predecessor accordingly
                successor_port = 55000 + self.successor
                predecessor_port = 55000 + self.predecessor

                # Notify current successor to update its predecessor
                with grpc.insecure_channel(f'localhost:{successor_port}') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    stub.UpdateNode(dht_pb2.UpdateNodeRequest(node_id=new_id))

                # Notify current predecessor to update its successor
                ## trocar localhost
                with grpc.insecure_channel(f'localhost:{predecessor_port}') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    stub.UpdateNode(dht_pb2.UpdateNodeRequest(node_id=new_id))

                # Update node 0's successor and predecessor to the new node
                self.successor = new_id
                self.predecessor = new_id
                self.successor_port = 55000 + new_id
                self.predecessor_port = 55000 + new_id

                print(f"Node 0 updated: Successor set to {new_id} with port {self.successor_port}")
                print(f"Node 0 updated: Predecessor set to {new_id} with port {self.predecessor_port}")

        return dht_pb2.UpdateNodeResponse(success=True)
    
    def UpdatePredecessor(self, request, context):
        self.predecessor = request.node_id
        self.predecessor_port = 55000 + request.node_id
        print(f"meu novo predecessor é {self.predecessor} na porta {self.predecessor_port}")
        return dht_pb2.UpdatePredecessorResponse(success=True)
    def UpdateSucessor(self, request, context):
        self.successor = request.node_id
        self.successor_port = 55000 + request.node_id
        print(f"meu novo sucecessor é {self.successor} na porta {self.successor_port}")
        return dht_pb2.UpdateSucessorResponse(success=True)
    
    def ReceiveHash(self, request, context):
        print(f"Recebi o hash: {request.hash} do valor {request.value}")
        
        self.data[request.value] = request.hash
        return dht_pb2.HashResponse(message="Hash buscado.")

def start_node(node_id):
    port = 55000 + node_id
    node = Node(node_id, port)
    print(f"Node {node_id} running on port {port}")

    # Print predecessor and successor
    print(f"My predecessor is: {node.predecessor}")
    print(f"My successor is: {node.successor}")
    print(f"My predecessor port is: {node.predecessor_port}")
    print(f"My successor port is: {node.successor_port}")

    # If the node is not node 0, send a join request to node 0
    if node_id != 0:
        with grpc.insecure_channel('localhost:55000') as channel:
            stub = dht_pb2_grpc.NodeStub(channel)
            # Query node 0 for its successor and predecessor
            query_response = stub.QueryNode(dht_pb2.NodeQueryRequest(node_id=0))
            successor = query_response.successor
            predecessor = query_response.predecessor

            print(f"Query response: Successor = {successor}, Predecessor = {predecessor}")

            # Check if node 0's successor and predecessor are -1
            ## nao utilizar mais updateNode
            if successor == -1 and predecessor == -1:
                print("I am the first to join this DHT!")
                # Notify node 0 to update its successor and predecessor
                update_response = stub.UpdateNode(dht_pb2.UpdateNodeRequest(node_id=node_id))
                print(f"Update response: {update_response.success}")

                # Update the joining node's successor and predecessor
                node.successor = 0
                node.predecessor = 0
                node.successor_port = 55000 + 0
                node.predecessor_port = 55000 + 0
                print(f"Node {node_id} updated: Successor = {node.successor} with port {node.successor_port}")
                print(f"Node {node_id} updated: Predecessor = {node.predecessor} with port {node.predecessor_port}")
            else:
                with grpc.insecure_channel('localhost:55000') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    response = stub.Join(dht_pb2.JoinRequest(node_id=node_id, port=port))
                    print(f"Join request sent, response: {response.success}")
                 

                # new_successor = successor
                # new_predecessor = predecessor

                # # Update the successor of the new node
                # new_successor_port = 55000 + new_successor
                # with grpc.insecure_channel(f'localhost:{new_successor_port}') as channel:
                #     stub = dht_pb2_grpc.NodeStub(channel)
                #     stub.UpdateNode(dht_pb2.UpdateNodeRequest(node_id=node_id))

                # # Update the predecessor of the new node
                # new_predecessor_port = 55000 + new_predecessor
                # with grpc.insecure_channel(f'localhost:{new_predecessor_port}') as channel:
                #     stub = dht_pb2_grpc.NodeStub(channel)
                #     stub.UpdateNode(dht_pb2.UpdateNodeRequest(node_id=node_id))

                # # Update the joining node's successor and predecessor
                # node.successor = new_successor
                # node.predecessor = new_predecessor
                # node.successor_port = 55000 + new_successor
                # node.predecessor_port = 55000 + new_predecessor

                # print(f"Node {node_id} updated: Successor = {node.successor} with port {node.successor_port}")
                # print(f"Node {node_id} updated: Predecessor = {node.predecessor} with port {node.predecessor_port}")

    try:
        while True:
            time.sleep(86400)  # Node runs indefinitely
    except KeyboardInterrupt:
        print("Shutting down node.")
        node.server.stop(0)

if __name__ == '__main__':
    # Ask for node ID input
    node_id = int(input("Insert the node ID: "))
    start_node(node_id)
