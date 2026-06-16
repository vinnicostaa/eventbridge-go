# JetStream

## Stream

Nome da stream:

```text
SERVICE_EVENTS
```

Configuracao atual criada pelos servicos:

```text
Storage:   FileStorage
Retention: LimitsPolicy
MaxAge:    24h
Subjects:
  - service2.orders.created
  - service3.messages
```

Todos os servicos possuem uma funcao `ensureStream` equivalente. Ela:

1. Consulta `StreamInfo("SERVICE_EVENTS")`.
2. Cria a stream se ela nao existir.
3. Atualiza a lista de subjects se a stream existir sem algum subject esperado.

## Subjects

### service2.orders.created

Evento publicado pelo `service1` quando um pedido e aceito.

Payload:

```json
{
  "id": "ord_...",
  "customer": "Vinni",
  "product": "Go + Gin + NATS JetStream",
  "quantity": 1,
  "created_at": "2026-06-16T05:28:29Z"
}
```

Consumidor:

```text
Servico:         service2
Tipo:            push consumer
Durable:         service2-order-worker
Deliver subject: deliver.service2.orders.created
Queue group:     order-workers
ACK:             manual / explicit
Deliver:         all
MaxDeliver:      5
IdleHeartbeat:   30s
```

### service3.messages

Mensagem publicada pelo `service1` apos criar o evento de pedido.

Payload:

```json
{
  "id": "msg_...",
  "order_id": "ord_...",
  "text": "Novo pedido ord_... criado para Vinni",
  "sent_at": "2026-06-16T05:28:29Z"
}
```

Consumidor:

```text
Servico:         service3
Tipo:            push consumer
Durable:         service3-message-consumer
Deliver subject: deliver.service3.messages
ACK:             manual / explicit
Deliver:         all
MaxDeliver:      5
IdleHeartbeat:   30s
```

## Semantica Atual

- JetStream persiste as mensagens na stream.
- `jetstream.New` cria a interface JetStream a partir da conexao NATS.
- `CreateOrUpdateStream` cria ou atualiza a stream.
- `Publish` e sincrono e aguarda `PubAck`.
- `jetstream.WithMsgID` e usado em cada publicacao.
- `CreateOrUpdatePushConsumer` cria ou atualiza os consumers.
- Os consumidores fazem ACK manual com `msg.Ack()`.
- Payload invalido tambem recebe ACK, entao a mensagem e descartada apos log.
- Se o consumidor nao confirmar dentro da janela de ACK configurada pelo servidor, a mensagem pode ser redeliverada ate `MaxDeliver(5)`.
