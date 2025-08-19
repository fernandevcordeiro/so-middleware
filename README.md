# Pipeline de Ingestão de Dados do IBGE – CNEFE 2022

## Introdução
Este projeto apresenta a implementação de um pipeline de ingestão de dados do Instituto Brasileiro de Geografia e Estatística (IBGE), com foco no Cadastro Nacional de Endereços para Fins Estatísticos (CNEFE) do Censo Demográfico 2022.  

A arquitetura proposta utiliza tecnologias modernas de processamento de dados, contemplando containerização, orquestração, mensageria e armazenamento distribuído. O objetivo é demonstrar uma solução robusta e escalável, seguindo boas práticas de sistemas distribuídos e arquiteturas orientadas a eventos.  

Os principais componentes são:  
- **load-app**: responsável por carregar os dados e enviá-los para o sistema de mensageria.  
- **store-app**: responsável por consumir as mensagens e armazenar os dados processados em formato otimizado.  

A solução é sustentada pelos seguintes elementos:  
- **Apache Kafka** → middleware de mensageria, garantindo alta disponibilidade e escalabilidade.  
- **MinIO** → armazenamento distribuído compatível com S3, utilizado para persistência em formato Parquet.  
- **Kubernetes** → orquestração, auto-scaling e resiliência.  
- **Grafana e Jaeger** → monitoramento de métricas e rastreamento distribuído.  

---

## Visão Geral da Arquitetura

### Padrões Arquiteturais
- Sistemas distribuídos modernos.  
- **Event-Driven Architecture (EDA)**.  
- Microserviços independentes.  
- Observabilidade nativa.  
- Processamento assíncrono e desacoplado.  

### Componentes
- **Apache Kafka**: backbone de mensageria, implementando log distribuído com garantia de ordem e durabilidade.  
- **MinIO**: camada de armazenamento persistente, com suporte a formato **Parquet** para compressão e consultas analíticas eficientes.  

### Fluxo de Dados
- **load-app**: monitora diretórios de entrada, processa arquivos CSV do CNEFE, aplica validações e metadados, serializa em JSON e envia para tópicos no Kafka (chaveados por município).  
- **store-app**: consome mensagens em lotes, converte os dados para Parquet com PyArrow e organiza os arquivos no MinIO em estrutura hierárquica, utilizando particionamento temporal.  

### Observabilidade e Monitoramento
- **Prometheus**: coleta de métricas de todos os componentes.  
- **Grafana**: dashboards customizados para análise em tempo real.  
- **Jaeger (OpenTelemetry)**: rastreamento distribuído com spans detalhados para cada transação.  

### Estratégias de Resiliência
- **Kafka**: replicação automática entre brokers.  
- **Agentes**: políticas de retry com backoff exponencial.  
- **Kubernetes**: health checks, reinício automático e redistribuição de carga.  
- **Circuit breakers**: prevenção de falhas em cascata.  
- **Degradação controlada** em cenários de indisponibilidade.  

### Escalabilidade
- **Kafka**: adição de brokers e partições para maior throughput.  
- **Agentes (load-app e store-app)**: replicação independente.  
- **MinIO**: clustering nativo para balanceamento de I/O e expansão de armazenamento.  
- **Crescimento modular** com provisionamento just-in-time de recursos.  
