create:
	protoc --proto_path=pb/proto pb/proto/*.proto --go_out=pb/apipb/
	protoc --proto_path=pb/proto pb/proto/*.proto --go-grpc_out=pb/apipb/

clean:
	rm pb/apipb/*.go