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


### Evidências
docker compose up -d
<img width="1065" height="197" alt="image" src="https://github.com/user-attachments/assets/f36b8b0d-83db-402e-9cb8-ebfceccf1a4d" />
<img width="1038" height="537" alt="image" src="https://github.com/user-attachments/assets/31bb42e7-6146-4257-b079-1409d3a4656c" />
<img width="1077" height="602" alt="image" src="https://github.com/user-attachments/assets/3614818d-1500-4336-8ce9-287f53d8b20f" />

run-demo.sh:


<img width="476" height="672" alt="image" src="https://github.com/user-attachments/assets/aeef4ac9-76e0-45e6-9a95-435fd37a798d" />

python3 simple-store-app.py:
<img width="1067" height="385" alt="image" src="https://github.com/user-attachments/assets/a6696d12-0caa-429a-b2de-64cb348ab604" />


python3 simple-load-app.py:
<img width="1068" height="250" alt="image" src="https://github.com/user-attachments/assets/e22f00c5-7206-4c4e-9088-7ae52f1a58ff" />

python3 test-pipeline.py:
<img width="1070" height="482" alt="image" src="https://github.com/user-attachments/assets/54f6f07a-09d7-4beb-9cbf-8d5ac67456cf" />


MinIO:
<img width="1072" height="392" alt="image" src="https://github.com/user-attachments/assets/7a1c5467-91f3-454a-80e9-05e19732baaf" />
<img width="1080" height="402" alt="image" src="https://github.com/user-attachments/assets/9bc5b23f-af43-43d8-952c-0f2b70932a05" />


python3 load-app.py:
<img width="1071" height="217" alt="image" src="https://github.com/user-attachments/assets/212d83bd-93c3-474b-8f93-fb478164606b" />


python3 store-app.py:
<img width="1072" height="335" alt="image" src="https://github.com/user-attachments/assets/74485298-8ac6-4572-b7f4-60637fbf5a7c" />


![Evidencia-SO-01](https://github.com/user-attachments/assets/287da231-a58d-48af-acc6-8f6db195ef0c)
![Evidencia-SO-02](https://github.com/user-attachments/assets/92d10726-2f82-4bbe-99c7-2e53495f0427)
![Evidencia-SO-03](https://github.com/user-attachments/assets/4bd1b49e-f7a8-44c9-914b-05585d0f46b6)
![Evidencia-SO-04](https://github.com/user-attachments/assets/c78f8f54-880c-4a93-b1f6-952fa2546a34)
![Evidencia-SO-05](https://github.com/user-attachments/assets/74605c9c-f4ca-48ab-9319-0b3b81374b0e)
![Evidencia-SO-06](https://github.com/user-attachments/assets/798a977c-7c58-40ac-baae-0fd7a90fdce8)


