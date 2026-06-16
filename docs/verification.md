# Verificacao

## Build, Test e Vet

Como a raiz do repositorio e um workspace Go e nao um modulo, use os modulos listados no `go.work`:

```bash
go test ./service1/... ./service2/... ./service3/...
go vet ./service1/... ./service2/... ./service3/...
```

Resultado observado:

```text
service1: ok, sem arquivos de teste
service2: ok, sem arquivos de teste
service3: ok, sem arquivos de teste
go vet: sem achados
```

O comando abaixo falha na raiz neste layout:

```bash
go test ./...
```

Motivo:

```text
pattern ./...: directory prefix . does not contain modules listed in go.work or their selected dependencies
```

## Smoke Test Local

Subir NATS com JetStream:

```bash
nats-server -js
```

Subir consumidores:

```bash
go -C service2 run .
go -C service3 run .
```

Subir API:

```bash
go -C service1 run .
```

Health check:

```bash
curl http://localhost:3000/ping
```

Resposta esperada:

```json
{"message":"pong"}
```

Criar pedido:

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"Vinni","product":"Go + Gin + NATS JetStream","quantity":1}'
```

Resposta esperada:

```text
HTTP 202
```

Com corpo contendo:

- `published.event_to_service2.stream = SERVICE_EVENTS`
- `published.event_to_service2.subject = service2.orders.created`
- `published.message_to_service3.stream = SERVICE_EVENTS`
- `published.message_to_service3.subject = service3.messages`
- `order_event`
- `service3_message`

Logs esperados:

- `service2`: `event received: order_created ...`
- `service3`: `message received: ...`

