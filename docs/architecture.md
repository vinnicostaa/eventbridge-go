# Arquitetura Atual

## Visao Geral

O workspace contem tres modulos Go independentes, conectados por uma instancia local do NATS com JetStream habilitado.

```text
Cliente HTTP
    |
    | POST /orders
    v
service1 Gin API
    |
    | publish service2.orders.created
    | publish service3.messages
    v
NATS JetStream: SERVICE_EVENTS
    |
    |-- service2 consome service2.orders.created
    |
    `-- service3 consome service3.messages
```

## Componentes

### service1

Responsavel pela borda HTTP do exemplo.

- Expoe `GET /ping`.
- Expoe `POST /orders`.
- Valida payload JSON com Gin binding.
- Cria um evento de pedido.
- Cria uma mensagem para o `service3`.
- Publica ambos no JetStream.
- Retorna `202 Accepted` quando as duas publicacoes recebem `PubAck`.

### service2

Worker de eventos de pedido.

- Conecta ao NATS.
- Garante que a stream `SERVICE_EVENTS` exista.
- Assina `service2.orders.created`.
- Usa push consumer duravel `service2-order-worker`.
- Usa queue group `order-workers`.
- Usa deliver subject `deliver.service2.orders.created`.
- Faz ACK manual apos processar ou descartar payload invalido.

### service3

Consumidor de mensagens.

- Conecta ao NATS.
- Garante que a stream `SERVICE_EVENTS` exista.
- Assina `service3.messages`.
- Usa push consumer duravel `service3-message-consumer`.
- Usa deliver subject `deliver.service3.messages`.
- Faz ACK manual apos processar ou descartar payload invalido.

## Fluxo de Criacao de Pedido

1. Cliente envia `POST /orders` para `service1`.
2. `service1` valida o JSON recebido.
3. `service1` cria um `orderCreatedEvent`.
4. `service1` cria uma `service3Message`.
5. `service1` publica o evento no subject `service2.orders.created`.
6. JetStream retorna `PubAck` da primeira publicacao.
7. `service1` publica a mensagem no subject `service3.messages`.
8. JetStream retorna `PubAck` da segunda publicacao.
9. `service1` responde `202 Accepted`.
10. `service2` recebe, desserializa, registra e confirma o evento.
11. `service3` recebe, desserializa, registra e confirma a mensagem.

## Observacoes Tecnicas

- A stream e criada automaticamente por qualquer servico ao iniciar.
- O storage da stream e `FileStorage`.
- A retention policy e `LimitsPolicy`.
- O `MaxAge` configurado e de 24 horas.
- Os consumers usam `DeliverAllPolicy`, entao um durable criado pela primeira vez pode receber mensagens antigas ainda retidas.
- O publish usa `jetstream.WithMsgID`, permitindo deduplicacao pelo JetStream dentro da janela configurada no servidor.
- A implementacao usa o pacote recomendado `github.com/nats-io/nats.go/jetstream`, com stream e consumers criados explicitamente via `context.Context`.

## Pontos de Atencao

- Se a primeira publicacao em `service1` funcionar e a segunda falhar, o sistema pode ficar em estado parcial.
- Strings compostas somente por espacos passam pela validacao `binding:"required"` antes do `strings.TrimSpace`.
- IDs baseados em `time.Now().UnixNano()` sao suficientes para demo, mas nao sao ideais para uso concorrente ou distribuido.
- `service1` usa `router.Run`, sem shutdown gracioso HTTP.
- Os consumers atuais sao push consumers para preservar o comportamento anterior. A documentacao do `nats.go` recomenda pull consumers como padrao para novos fluxos de processamento continuo.
