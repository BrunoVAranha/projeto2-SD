import grpc
from concurrent import futures
import time
import dht_pb2
import dht_pb2_grpc
import hashlib
import random

# Define a classe Node, que representa um único nó na DHT
class Node(dht_pb2_grpc.NodeServicer):
    def __init__(self, node_num, port, node_id):
        self.node_num = node_num
        self.node_id  = node_id
        self.port = port
        self.successor = 'empty'
        self.predecessor = 'empty'
        self.successor_port = 'empty'
        self.predecessor_port = 'empty'
        # Dicionário contendo os inteiros e seus hashes
        self.data = {}
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        dht_pb2_grpc.add_NodeServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'[::]:{port}')
        self.server.start()

    def Join(self, request, context):
        print(f"request port: {request.port}")
        print(f"request id: {request.node_id}")
        print("aqui")
        if request.node_id == self.node_id:
            print("Nó já existente.")
            return dht_pb2.JoinResponse(success=False)
        elif self.successor == sha256_to_range(0) and hex_string_to_int(request.node_id) > hex_string_to_int(self.node_id):
            print("Fim do anel!")
            # atualizar predecessor e sucessor do nó que entrou na DHT e agora é o último
            with grpc.insecure_channel(f'localhost:{request.port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = self.node_id, node_num = self.node_num))
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id= sha256_to_range(0), node_num = 0))
            #####
            # atualizar predecessor e sucessor dos nós que já estão na DHT
            with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id=request.node_id, node_num=request.node_num))         
            self.successor = request.node_id
            self.successor_port = 55000 + request.node_num
            print(f"meu novo sucessor é {self.successor} na porta {self.successor_port}")
            #####
        elif hex_string_to_int(request.node_id) > hex_string_to_int(self.node_id) and self.successor != 'empty':
            print(f"Encaminhando pedido para o nó {self.successor} na porta {self.successor_port}")
            with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.Join(dht_pb2.JoinRequest(node_id = request.node_id, node_num=request.node_num, port=request.port))
        elif hex_string_to_int(request.node_id) < hex_string_to_int(self.node_id):
            print("aquiiii")
            # atualizar predecessor e sucessor do nó que entrou na DHT
            with grpc.insecure_channel(f'localhost:{request.port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = self.predecessor, node_num = self.predecessor_port - 55000))
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = self.node_id, node_num = self.node_num))
            #####
            # atualizar predecessor e sucessor dos nós que já estão na DHT
            with grpc.insecure_channel(f'localhost:{self.predecessor_port}') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = request.node_id, node_num=request.node_num))         
            self.predecessor = request.node_id
            self.predecessor_port = 55000 + request.node_num
            print(f"meu novo predecessor é {self.predecessor} na porta {self.predecessor_port}")
            #####
        return dht_pb2.JoinResponse(success=True)

    def QueryNode(self, request, context):
        # Retorna o sucessor e o predecessor do nó consultado
        return dht_pb2.NodeQueryResponse(successor=self.successor, predecessor=self.predecessor)
    
    def UpdatePredecessor(self, request, context):
        self.predecessor = request.node_id
        self.predecessor_port = 55000 + request.node_num
        print(f"meu novo predecessor é {self.predecessor} na porta {self.predecessor_port}")
        return dht_pb2.UpdatePredecessorResponse(success=True)
    
    def UpdateSucessor(self, request, context):
        self.successor = request.node_id
        self.successor_port = 55000 + request.node_num
        print(f"meu novo sucessor é {self.successor} na porta {self.successor_port}")
        return dht_pb2.UpdateSucessorResponse(success=True)
    
    def AddCode(self, request, context):
        print(f"meu id é {self.node_id}")
        print(request)
        print(hex_string_to_int(request.code_id))
        print(hex_string_to_int(self.node_id))
        # se o conteúdo for menor ou igual ao identificador do nó, incluir nos seus dados
        if hex_string_to_int(request.code_id) == hex_string_to_int(self.node_id):
            print("Sou igual ao nó. Fico aqui.")
            self.data[request.code_num] = request.code_id

        elif hex_string_to_int(request.code_id) < hex_string_to_int(self.node_id):
            print("Sou menor do que o nó.")
            if hex_string_to_int(request.code_id) <= hex_string_to_int(self.predecessor):
                print("Sou menor do que o predecessor deste nó.")
                if hex_string_to_int(self.node_id) < hex_string_to_int(self.predecessor):
                    print("Eu fico aqui entre o último nó e o primeiro.")
                    self.data[request.code_num] = request.code_id
                else:
                    ## enviar request para o predecessor
                    with grpc.insecure_channel(f'localhost:{self.predecessor_port}') as channel:
                        stub = dht_pb2_grpc.NodeStub(channel)
                        stub.AddCode(dht_pb2.AddCodeRequest(code_id =request.code_id, code_num = request.code_num))
            else:
                print("Sou menor do que o nó, mas maior que o predecessor. Fico aqui")
                self.data[request.code_num] = request.code_id
        ## joinId maior do que o nó
        else :
            print("Sou maior do que o nó.")
            ## O nó atual tem id menor do que seu predecessor. Isso só acontece para o primeiro nó do anel.
            if hex_string_to_int(self.node_id) < hex_string_to_int(self.predecessor) and hex_string_to_int(request.code_id) > hex_string_to_int(self.predecessor):
                print("Sou maior que todos. Fico no primeiro nó.")
                self.data[request.code_num] = request.code_id
            else:
                ## por fim, se nenhuma condição for atingida, repasse para o sucessor
                print("Sou maior que o nó atual. Mas ainda não cheguei ao fim do anel, pode haver alguem maior que eu.")
                with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    stub.AddCode(dht_pb2.AddCodeRequest(code_id =request.code_id, code_num = request.code_num))

        print(f"Sou o nó {self.node_num} com id {self.node_id} e sou responsável por: {self.data}")
        return dht_pb2.AddCodeResponse(message="Conteúdo inserido")
    
    
    def Ping(self, request, context):
            return dht_pb2.PingResponse(is_alive=True)
    
    
    def LookUp(self, request, context):
        ## apenas o primeiro a receber o codigo a ser buscado deve fazer o hash do valor, o restante já recebe o valor em hash
        if self.port == 55000:
            lookUp = sha256_to_range(int(request.code))
        else: 
            lookUp = request.code
        ###########
        if hex_string_to_int(lookUp) == hex_string_to_int(self.node_id):
            for key, value in self.data.items():
                    if value == lookUp:
                        print(f"Sou o responsável pela chave: {key}")
                        url = f"https://http.cat/status/{key}"
                        print(url)
                        return dht_pb2.LookUpResponse(url=url)

        elif hex_string_to_int(lookUp) < hex_string_to_int(self.node_id):
            if hex_string_to_int(lookUp) <= hex_string_to_int(self.predecessor):
                if hex_string_to_int(self.node_id) < hex_string_to_int(self.predecessor):
                    for key, value in self.data.items():
                        if value == lookUp:
                            print(f"Sou o responsável pela chave: {key}")
                            url = f"https://http.cat/status/{key}"
                            print(url)
                            return dht_pb2.LookUpResponse(url=url)
                else:
                    ## enviar request ASSINCRONO para o predecessor
                    with grpc.insecure_channel(f'localhost:{self.predecessor_port}') as channel:
                        stub = dht_pb2_grpc.NodeStub(channel)
                        return stub.LookUp(dht_pb2.LookUpRequest(code=lookUp))
            else:
                for key, value in self.data.items():
                    if value == lookUp:
                        print(f"Sou o responsável pela chave: {key}")
                        url = f"https://http.cat/status/{key}"
                        print(url)
                        return dht_pb2.LookUpResponse(url=url)
        ## joinId maior do que o nó
        else :
            print("Sou maior do que o nó.")
            ## O nó atual tem id menor do que seu predecessor. Isso só acontece para o primeiro nó do anel.
            if hex_string_to_int(self.node_id) < hex_string_to_int(self.predecessor) and hex_string_to_int(lookUp) > hex_string_to_int(self.predecessor):
                for key, value in self.data.items():
                    if value == lookUp:
                        print(f"Sou o responsável pela chave: {key}")
                        url = f"https://http.cat/status/{key}"
                        print(url)
                        return dht_pb2.LookUpResponse(url=url)
            else:
                ## por fim, se nenhuma condição for atingida, fazer chamada ASSINCRONA para sucessor
                with grpc.insecure_channel(f'localhost:{self.successor_port}') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    return stub.LookUp(dht_pb2.LookUpRequest(code=lookUp))

    
def popular_dht():
        http_status_codes = [100, 101, 102, 200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 409, 418, 500, 501, 502, 503, 504, 505]
        for code in http_status_codes:
            with grpc.insecure_channel(f'localhost:55000') as channel:
                stub = dht_pb2_grpc.NodeStub(channel)
                print(f"enviando {code}")
                print(f"hash: {sha256_to_range(code)}")
                print(f"enviado: {stub.AddCode(dht_pb2.AddCodeRequest(code_id=sha256_to_range(code), code_num = code))}")
def ping():
    try:
        with grpc.insecure_channel(f'localhost:55000') as channel:
            stub = dht_pb2_grpc.NodeStub(channel)
            return stub.Ping(dht_pb2.PingRequest())
    except grpc.RpcError as e:
        return False
    
def hex_string_to_int(hex_string: str) -> int:
    # Remove o prefixo '0x' se presente
    cleaned_hex_string = hex_string.lstrip('0x')
    return int(cleaned_hex_string, 16)
    
def sha256_to_range(value: int) -> str:
    # Converte o inteiro para bytes
    value_bytes = str(value).encode('utf-8')
    
    # Calcula o hash SHA-3-256
    hash_object = hashlib.sha3_256(value_bytes)
    hash_hex = hash_object.hexdigest()

    if not hash_hex:
        raise ValueError("A string hexadecimal gerada está vazia.")
    
    # Converte o hash para um inteiro
    hash_int = int(hash_hex, 16)
    
    # Mapeia o inteiro para o intervalo desejado
    mapped_value = hash_int % 512
    
    # Converte o valor mapeado para uma string hexadecimal
    hex_value = hex(mapped_value)[2:]

    return hex_value
                
def start_node():
    # pingar a porta 55000 para saber se já existe uma DHT
    if not ping():
        node_num = 0
        port = 55000
        node_id = sha256_to_range(0)
        node = Node(node_num, port, node_id)
        print(f"Sou o primeiro nó")
        print(f"Nó {node_num} rodando na porta {port}")
        print(f"ID: {node_id}")
        print(node.predecessor)
        print(node.successor)
    else:
        node_num = random.randint(1, 500)
        port = 55000 + node_num
        node_id = sha256_to_range(node_num)
        node = Node(node_num, port, node_id)

        print(f"Nó {node_num} rodando na porta {port}")
        print(f"ID: {sha256_to_range(node_num)}")

        # verificar se o nó 0 possui 'empty' como sucessor/predecessor
        with grpc.insecure_channel('localhost:55000') as channel:
            stub = dht_pb2_grpc.NodeStub(channel)
            # Consultar o nó 0 para seu sucessor e predecessor
            query_response = stub.QueryNode(dht_pb2.NodeQueryRequest())
            successor = query_response.successor
            predecessor = query_response.predecessor

            print(f"Resposta da consulta: Sucessor = {successor}, Predecessor = {predecessor}")

            # Verificar se o sucessor e o predecessor do nó 0 estão vazios
            if successor == 'empty' and predecessor == 'empty':
                print("Eu sou o primeiro a entrar nesta DHT!")
                # Nó zero tem o novo nó como sucessor e predecessor
                with grpc.insecure_channel(f'localhost:55000') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)
                    stub.UpdatePredecessor(dht_pb2.UpdatePredecessorRequest(node_id = node_id, node_num = node_num))
                    stub.UpdateSucessor(dht_pb2.UpdateSucessorRequest(node_id = node_id, node_num = node_num))

                # O primeiro nó a entrar na DHT tem zero como sucessor e predecessor
                node.successor = sha256_to_range(0)
                node.predecessor = sha256_to_range(0)
                node.successor_port = 55000
                node.predecessor_port = 55000
                print(f"Nó {node_num} atualizado: Sucessor = {node.successor} com porta {node.successor_port}")
                print(f"Nó {node_num} atualizado: Predecessor = {node.predecessor} com porta {node.predecessor_port}")
            # enviar solicitação de junção para o nó 0
            else:
                print("entrei")
                print(node_num)
                with grpc.insecure_channel('localhost:55000') as channel:
                    stub = dht_pb2_grpc.NodeStub(channel)

                    response = stub.Join(dht_pb2.JoinRequest(node_id = node_id, node_num = node_num, port = port))
                    print(f"Solicitação de junção enviada, resposta: {response.success}")

    try:
        while True:
            time.sleep(86400)  # Nó roda indefinidamente
    except KeyboardInterrupt:
        print("Desligando o nó.")
        node.server.stop(0)

if __name__ == '__main__':
    # Solicitar entrada de ID do nó
    operacao = input("Digite C para criar um novo nó na DHT, ou P para popular a DHT.")
    if operacao == 'C' or operacao == 'c':
        start_node()
    elif operacao == 'P' or operacao == 'p':
        popular_dht()