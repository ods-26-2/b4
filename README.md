## B4 — Testes de integração (TRA-2)

As suítes Kafka desta pasta atuam como produtores de teste: publicam mensagens nos
tópicos de entrada da especificação 6c e consomem as respostas/eventos dos
componentes implantados. A validação dos envelopes e payloads é feita localmente
por `tests/integration/contract_validation.py`; os componentes não são iniciados
nem importados pelo executor de integração.

### Executar

Instale o cliente Kafka e configure o endereço do broker de testes:

```powershell
python -m pip install -r requirements.txt
$env:ODS_KAFKA_BOOTSTRAP_SERVERS = "<host:porta>"
$env:ODS_TEST_SOURCE_ID = "<source_id de teste cadastrado no ZONES do S1>"
python run_integration.py
```

Para Kafka gerenciado com SASL/TLS, configure as variáveis comuns usadas pela
arquitetura:

```powershell
$env:KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
$env:KAFKA_SASL_MECHANISM = "PLAIN"
$env:KAFKA_SASL_USERNAME = "<usuario>"
$env:KAFKA_SASL_PASSWORD = "<senha>"
# Opcional, se o broker exigir uma CA específica:
$env:KAFKA_SSL_CA_LOCATION = "C:\certs\kafka-ca.pem"
```

Também é aceito `KAFKA_BOOTSTRAP_SERVERS`. Os tópicos de entrada e saída devem
existir e as credenciais precisam de permissão de escrita/leitura. O executor não
cria tópicos nem altera configurações do broker. `ODS_KAFKA_READY_TIMEOUT_SECONDS`
e `ODS_KAFKA_REPLY_TIMEOUT_SECONDS` ajustam os tempos de espera (padrões: 30 e
45 segundos).

Por segurança, execute contra um broker/namespace de teste ou staging, nunca
contra produção: os eventos sintéticos de I2/I9/S1 são publicados nos tópicos
compartilhados e podem ser vistos por outros consumidores. O `source_id` do S1
deve ser dedicado aos testes para evitar reset de sessão de uma câmera ativa.
Os testes de comandos S2/S3/P6/P7/S6 usam entradas inválidas conhecidas e esperam
replies de erro para evitar alterações de estado, criação de alertas, movimento
do atuador ou síntese.

### Cobertura

O executor roda as suítes Kafka de I2, I9, P6, P7, S1, S2, S3, S4, S5 e S6. B2 e
I3 são registrados como bloqueados: B2 usa MQTT e não tem contrato Kafka 6c; I3
publica calibração, mas a especificação não define mensagem Kafka de entrada
para iniciar uma calibração. I6 tem uma consulta no contrato, mas o runtime do
workspace não expõe o handler Kafka correspondente. Componentes acionados por
mídia/stream, como I1, I4, I5 e I7, também não podem ser iniciados por uma
mensagem do barramento no contrato atual. Não se inventam comandos para esses
componentes.

Sem `ODS_KAFKA_BOOTSTRAP_SERVERS` ou `KAFKA_BOOTSTRAP_SERVERS`, ou sem
`ODS_TEST_SOURCE_ID` para S1, as suítes dependentes são registradas como
`BLOQUEADO`, não como aprovadas. O relatório
`..\relatorio_integracao.csv` é atualizado pelo executor com resultados e
bloqueios.
