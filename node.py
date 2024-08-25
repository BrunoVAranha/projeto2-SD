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
            # atualizar predecessor e sucessor do nó que entrou na DHT e agora é o ultimo
            with grpc.insecure_channel(f'localhost:{request.port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = self.node_id))
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = 0))
            #####
            # atualizar predecessor e sucessor dos nós que já estão na DHT
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
            # atualizar predecessor e sucessor do nó que entrou na DHT
            with grpc.insecure_channel(f'localhost:{request.port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = self.predecessor))
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = self.node_id))
            #####
            # atualizar predecessor e sucessor dos nós que já estão na DHT
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
    
    def AddCode(self, request, context):
        # se o conteudo for menor ou igual ao identificador do nó, incluir nos seus dados
        if request.number <= self.node_id:
            self.data[request.number] = "test"

        elif self.node_id == 0:
            if request.number > self.predecessor:
                #se o conteudo for maior que o antecesor de zero, adicionar a zero
                self.data[request.number] = "test"
            else:
                ## enviar para o próximo nó
                with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    stub.AddCode(dht_pb2.AddCodeRequest(number=request.number))
            
        elif request.number > self.node_id :
            ## por fim, se nenhuma consição for atingida, repasse par o próximo nó enviar
            with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.AddCode(dht_pb2.AddCodeRequest(number=request.number))

        print(f"Sou o nó {self.node_id} e sou responsavel por: {self.data}")
        return dht_pb2.AddCodeResponse(message="Conteudo inserido")

    
def popular_dht():
        http_status_codes = [100, 101, 102, 200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 409, 418, 500, 501, 502, 503, 504, 505]
        for code in http_status_codes:
            with grpc.insecure_channel(f'localhost:55000') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                print(f"enviando {code}")
                print(f"enviado: {stub.AddCode(dht_pb2.AddCodeRequest(number=code))}")        
                

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
                # Nó zero tem o novo nó como sucessor e predecessor
                with grpc.insecure_channel(f'localhost:55000') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = node_id))
                    stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = node_id))

                # O primeiro nó a entrar na DHT tem zero como sucessor e predecessor
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

    try:
        while True:
            time.sleep(86400)  # Node runs indefinitely
    except KeyboardInterrupt:
        print("Shutting down node.")
        node.server.stop(0)

if __name__ == '__main__':
    # Ask for node ID input
    operacao = input("Digite C para criar um novo nó na dht, ou P para popular a DHT.")
    if operacao == 'C' or operacao == 'c':
        node_id = int(input("Insert the node ID: "))
        start_node(node_id)
    elif operacao == 'P' or operacao == 'p':
        popular_dht()
