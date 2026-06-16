# service2

`service2` e um worker JetStream responsavel por consumir eventos de pedidos criados no subject `service2.orders.created`.

## Responsabilidade

- Conectar ao NATS.
- Garantir a existencia da stream `SERVICE_EVENTS`.
- Criar ou reutilizar um durable consumer.
- Participar do queue group `order-workers`.
- Processar eventos de pedido criado.
- Confirmar mensagens manualmente com ACK.

## Entrada

Subject consumido:

```text
service2.orders.created
```

Payload esperado:

```json
{
  "id": "ord_...",
  "customer": "Vinni",
  "product": "Go + Gin + NATS JetStream",
  "quantity": 1,
  "created_at": "..."
}
```

## Configuracao do Consumer

```text
Stream:      SERVICE_EVENTS
Subject:     service2.orders.created
Durable:     service2-order-worker
Type:            push consumer
Deliver subject: deliver.service2.orders.created
Queue group:     order-workers
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
5. Usa durable `service2-order-worker`.
6. Usa queue group `order-workers`.
7. Ao receber mensagem, tenta desserializar JSON para `orderCreatedEvent`.
8. Se o payload for invalido, registra erro e faz ACK para descartar.
9. Se o payload for valido, registra os dados do pedido e faz ACK.
10. Fica bloqueado aguardando `SIGINT` ou `SIGTERM`.

## Como Rodar

Com o NATS JetStream ativo:

```bash
go -C service2 run .
```

## Escalabilidade

O uso de queue group permite subir multiplas instancias do `service2`. Dentro do grupo `order-workers`, apenas uma instancia deve processar cada mensagem entregue ao grupo.

## Pontos de Atencao

- `DeliverAllPolicy` pode reprocessar mensagens antigas quando o durable ainda nao existe.
- Payload invalido recebe ACK e nao sera redeliverado.
- `MaxDeliver(5)` limita redeliveries, mas nao ha DLQ configurada.
- A funcao `ensureStream` esta duplicada em relacao aos outros servicos.
- Push consumer com `DeliverGroup` preserva a semantica de queue group, mas a documentacao do `nats.go` recomenda pull consumers para novos fluxos de processamento continuo.
