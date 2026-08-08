module human_validation_service

go 1.26.4

require (
	github.com/AndrewK4758/shared_protos v0.0.0-20260709011136-f55ddc746c56
	github.com/AndrewK4758/shared_utils v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/nats-io/nats.go v1.52.0
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260720211330-0afa2a65878a // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

replace github.com/AndrewK4758/shared_utils => ../shared_utils

replace github.com/AndrewK4758/shared_protos => ../shared_protos
