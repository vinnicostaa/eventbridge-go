# Documentacao do Projeto

Este diretorio centraliza a documentacao tecnica do workspace `nats-gin`.

O projeto demonstra um fluxo assincrono simples com tres servicos Go, Gin e NATS JetStream:

- `service1`: API HTTP responsavel por receber pedidos e publicar eventos/mensagens.
- `service2`: worker que consome eventos de pedidos criados.
- `service3`: consumidor que recebe mensagens geradas a partir do pedido.

## Documentos

- [architecture.md](./architecture.md): visao geral da arquitetura, fluxo atual e componentes.
- [jetstream.md](./jetstream.md): configuracao atual da stream, subjects, publishers e consumers.
- [verification.md](./verification.md): comandos usados para validar build, vet e smoke test local.

