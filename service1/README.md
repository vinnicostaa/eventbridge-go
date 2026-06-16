# service1

`service1` e a API HTTP do projeto. Ela recebe pedidos via Gin e publica dados persistidos no NATS JetStream para os outros servicos.

## Responsabilidade

- Expor endpoints HTTP.
- Validar o payload de criacao de pedido.
- Criar um evento de pedido criado.
- Criar uma mensagem destinada ao `service3`.
- Publicar os dois registros na stream `SERVICE_EVENTS`.
- Responder ao cliente com os dados publicados e as sequencias retornadas pelo JetStream.

## Endpoints

### GET /ping

Health check simples.

Resposta:

```json
{"message":"pong"}
```

### POST /orders

Recebe um pedido e publica duas mensagens no JetStream.

Payload:

```json
{
  "customer": "Vinni",
  "product": "Go + Gin + NATS JetStream",
  "quantity": 1
}
```

Validacao atual:

- `customer`: obrigatorio.
- `product`: obrigatorio.
- `quantity`: obrigatorio e minimo `1`.

Resposta de sucesso:

```text
HTTP 202 Accepted
```

## Fluxo Atual

1. Conecta ao NATS usando `NATS_URL` ou `nats://localhost:4222`.
2. Cria a interface JetStream com `jetstream.New`.
3. Garante a existencia da stream `SERVICE_EVENTS` com `CreateOrUpdateStream`.
4. Inicia o servidor HTTP em `HTTP_ADDR` ou `:3000`.
5. Ao receber `POST /orders`, valida o JSON com `ShouldBindJSON`.
6. Normaliza `customer` e `product` com `strings.TrimSpace`.
7. Cria um `orderCreatedEvent`.
8. Cria uma `service3Message`.
9. Publica o evento em `service2.orders.created` com `Publish` e `jetstream.WithMsgID`.
10. Publica a mensagem em `service3.messages` com `Publish` e `jetstream.WithMsgID`.
11. Retorna o status e os `PubAck` recebidos.

## Publicacoes

### service2.orders.created

Evento consumido pelo `service2`.

```json
{
  "id": "ord_...",
  "customer": "Vinni",
  "product": "Go + Gin + NATS JetStream",
  "quantity": 1,
  "created_at": "..."
}
```

### service3.messages

Mensagem consumida pelo `service3`.

```json
{
  "id": "msg_...",
  "order_id": "ord_...",
  "text": "Novo pedido ord_... criado para Vinni",
  "sent_at": "..."
}
```

## Variaveis de Ambiente

```text
NATS_URL=nats://localhost:4222
HTTP_ADDR=:3000
```

## Como Rodar

Com o NATS JetStream ativo:

```bash
go -C service1 run .
```

## Pontos de Atencao

- As duas publicacoes nao sao atomicas. Se a primeira funcionar e a segunda falhar, o `service2` pode processar o evento mesmo com resposta de erro ao cliente.
- A validacao `binding:"required"` ocorre antes do `TrimSpace`; strings com apenas espacos podem passar.
- Os IDs usam `time.Now().UnixNano()`, adequado para demo, mas limitado para uso distribuido.
- O servidor HTTP nao possui shutdown gracioso.
