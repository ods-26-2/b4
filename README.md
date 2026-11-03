## B4 — Testes de integração (TRA-2)

Este repositório contém testes de integração orientados aos contratos da
Especificação de interfaces 1.0 em [../integracao.txt](../integracao.txt). Cada suíte fica em
`tests/integration/<componente>/`; os testes enviam entradas aos pontos de
execução reais disponíveis no workspace e validam as saídas com
`tests/integration/contract_validation.py`.

### Executar

No PowerShell, na raiz deste repositório:

```powershell
python run_integration.py
```

O comando executa todas as suítes aplicáveis e atualiza
`..\relatorio_integracao.csv`, planilha CSV com separador `;` e BOM UTF-8,
compatível com Excel. Um componente que não tem código, não recebe entradas,
não produz saídas ou depende de hardware/modelos/brokers indisponíveis aparece
com o estado e a justificativa no relatório; esses casos não são contabilizados
como aprovados. P1 e P3 também aparecem no relatório como não aplicáveis: P1
não consta na especificação 6c nem tem implementação neste workspace, e P3 é
uma imagem base de sistema sem interface de mensagens descrita no manual.
P7 e S6 têm contratos de mensagem definidos, mas não oferecem a implementação
compatível necessária para executar os fluxos; seus testes B4 foram criados e
registram esses bloqueios. B2 e P6 têm atualizações disponíveis nas referências
locais `origin/develop`, mas os checkouts de trabalho permanecem em `main`.
O callback MQTT de B2 foi testado isoladamente com cliente falso e produziu
reply UTC correlacionada; broker real e compatibilidade com o contrato 6c
permanecem pendentes. O publicador de replay de P6 foi testado isoladamente
com produtor Kafka falso e reprovou por ausência de campos obrigatórios do
envelope; handlers de gravação/replay ainda não foram comprovados. I9 possui implementação e fixture,
e sua suíte B4 foi adicionada; a execução dela está pendente nesta atualização
para respeitar o pedido de não repetir os testes.
As frentes transversais B1–B6 também são listadas no relatório: B1–B3 estão
bloqueadas pela falta de runtimes testáveis, B5 e B6 têm validações pendentes
que não foram executadas, e B4 é identificada como a ferramenta de verificação,
não como um componente produtor/consumidor da arquitetura.

Instale previamente as dependências de cada componente conforme o seu README.
Quando a dependência de runtime estiver ausente, a suíte registra o teste como
`BLOQUEADO` em vez de confundir a falta de ambiente com falha contratual.

As suítes S1, S3 e S5 usam seus caminhos locais em memória/simulados e não
precisam do broker compartilhado. I2 e I3 são executados pelo CLI documentado;
seus testes comparam a saída real com os schemas da especificação 6c, portanto
identificam divergências entre a implementação de cada repositório e o manual.
P2 gera um pacote MODEL ONNX temporário e testa a compilação CPU e a inferência
pela API pública; para executá-lo, instale `p2[cpu]` e `pytest` no ambiente
Python usado pelo executor.
I4 também tem um fluxo offline com vídeo e checkpoint locais; a suíte valida a
saída real contra o evento `ods.i4.sign` do manual, sem simular publicação no
barramento.
I1 executa o detector ONNX real no serviço Docker `i1-degrau-a` e valida os
eventos JSONL publicados contra `ods.i1.detections`; exige Docker Engine ativo,
imagem construível e os artefatos/fixtures incluídos no repositório I1.
I5 executa Whisper tiny em CPU com `i5/data/audio6.ogg`, quando o modelo já
está no cache local; I7 executa YOLO e Fast-ALPR em CPU com checkpoint, fixture
de imagem e modelo OCR locais. Esses testes não baixam modelos durante a
execução. Em ambos, o teste valida a saída real contra o contrato do manual e
registra como reprovada qualquer divergência, mesmo quando a inferência local
funciona.

O plano de verificação por camada está em `docs/`. A execução operacional de
componentes que dependem de artefatos, câmeras, microfones, toolchains ou
serviços externos exige os pré-requisitos próprios descritos em cada componente.