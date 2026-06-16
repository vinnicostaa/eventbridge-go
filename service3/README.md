# service3

`service3` e um consumidor JetStream responsavel por receber mensagens no subject `service3.messages`.

## Responsabilidade

- Conectar ao NATS.
- Garantir a existencia da stream `SERVICE_EVENTS`.
- Criar ou reutilizar um durable consumer.
- Processar mensagens criadas pelo `service1`.
- Confirmar mensagens manualmente com ACK.

## Entrada

Subject consumido:

```text
service3.messages
```

Payload esperado:

```json
{
  "id": "msg_...",
  "order_id": "ord_...",
  "text": "Novo pedido ord_... criado para Vinni",
  "sent_at": "..."
}
```

## Configuracao do Consumer

```text
Stream:     SERVICE_EVENTS
Subject:    service3.messages
Type:            push consumer
Durable:         service3-message-consumer
Deliver subject: deliver.service3.messages
ACK:             manual / explicit
Deliver:         all
MaxDeliver:      5
IdleHeartbeat:   30s
```

## Fluxo Atual

1. Conecta ao NATS usando `NATS_URL` ou `nats://localhost:4222`.
2. Cria a interface JetStream com `jetstream.New`.
3. Garante a existencia da stream `SERVICE_EVENTS` com `CreateOrUpdateStream`.
4. Cria ou atualiza um push consumer com `CreateOrUpdatePushConsumer`.
5. Usa durable `service3-message-consumer`.
6. Ao receber mensagem, tenta desserializar JSON para `service3Message`.
7. Se o payload for invalido, registra erro e faz ACK para descartar.
8. Se o payload for valido, registra os dados da mensagem e faz ACK.
9. Fica bloqueado aguardando `SIGINT` ou `SIGTERM`.

## Como Rodar

Com o NATS JetStream ativo:

```bash
go -C service3 run .
```

## Pontos de Atencao

- `DeliverAllPolicy` pode entregar mensagens antigas quando o durable ainda nao existe.
- Payload invalido recebe ACK e nao sera redeliverado.
- Nao ha queue group neste servico; se precisar escalar processamento horizontal, avalie pull consumer compartilhado ou `DeliverGroup`.
- A funcao `ensureStream` esta duplicada em relacao aos outros servicos.
