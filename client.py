import grpc
import dht_pb2
import dht_pb2_grpc
import webbrowser
import os
####remover logs do grpc
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GRPC_TRACE'] = ''
#####################

def lookup_value(code):
    with grpc.insecure_channel('localhost:55000') as channel:
        stub = dht_pb2_grpc.NodeStub(channel)
        
        urlResponse = stub.LookUp(dht_pb2.LookUpRequest(code=code))
        
        print(urlResponse.url)
        webbrowser.open(urlResponse.url)

if __name__ == '__main__':
    code = input("Codigo de erro: ")  # Set your lookup code here
    lookup_value(code)