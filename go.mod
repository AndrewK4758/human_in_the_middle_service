module human_validation_service

go 1.26.4

require (
	github.com/AndrewK4758/shared_protos v0.0.0-20260815145145-c2c7e8d11702
	github.com/AndrewK4758/shared_utils v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/nats-io/nats.go v1.53.1
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260810153831-ec0a7760b754 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

replace github.com/AndrewK4758/shared_utils => ../shared_utils

replace github.com/AndrewK4758/shared_protos => ../shared_protos
