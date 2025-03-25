# Projeto BEES

## Visão Geral do Projeto

O projeto consiste em criar um um docker compose relativamente simples contendo os serviços necessarios para rodar um ETL que coleta dados da pagina: https://www.openbrewerydb.org/breweries transforma eles no formato delta para a camada Silver, e consolida as informações gerando duas tabelas finais para a versão gold.

## Prerequisitos

- [Docker](https://www.docker.com/products/docker-desktop) (including Docker CLI)
- [Docker Compose](https://docs.docker.com/compose/install/)
- Git

## Rodando o Projeto

1. ** Clonando repositorio

   ```bash
   git clone https://github.com/oholoko/teste_BEES.git
   cd teste_BEES
   ```

2. **Iniciando o projeto**:

   ```bash
   docker compose up --scale spark-worker=1
   ```
  Scale pode ser aumentado para ter mais workers do spark

  ### Interfaces visuais
   ```
   sparkUI 127.0.0.1:8081
   Minio 127.0.0.1:9001
   Airflow 127.0.0.1:8080   
   ```

3. **Parando o Projeto**

```bash
docker-compose down
```
4. **Resetando o Projeto**

```bash
docker-compose down --volumes --remove-orphans
docker images prune -all
```

## Pipelines e explicação:

1. **Detalhes do projeto**

## Bronze
  
  O projeto foi criado com base nos dados vindos da API https://www.openbrewerydb.org/breweries eu decidi iniciar com a
  ingestão usando o endpoint que contem os metadados para indicar o numero de paginas e depois iterando cada pagina utilizando a
  biblioteca requests isso terminou gerando um gargalo pois a biblioteca não aceita ingestão de forma asynchrona. Porem no momento
  que notei percebi que a ingestão levava menos de um minuto no total decidi apenas deixar de forma synchrona.
  Tratei as falhas colocando um bloco logo em seguida e repetindo todos as ingestões de falharam, e no final conferindo se todos
  os estados tem a mesma contagem de registros que a API de metadados externa ja que os ids não são sequenciais.

  Eu decidi salvar todos eles com o mesmo formato de entrada ja que ele era razoavelmente bem definido.
  Notei algumas falhas nesse passo mas decidi concertar apenas na silver ja que na bronze seria interessante ter os dados de forma crua,
  eu fiz toda ingestão ser total por que sem um ID sequencial fica bem dificil saber quantos registros estão presentes nos dados, eu decidi
  particionar os dados por ID criando um json por cervejaria ficando mais facil deletar uma caso seja necessario por algum motivo, isso gera um
  problema caso alguma fosse removida num futuro mas decidi deixar dessa forma.

## Silver

  Os unicos tratamentos que se provaram necessarios foram: Um trim nos campos de região(alguns estados tinha espaço no inicio e fim) transformação
  de coordenadas geograficas para float. Fora isso decidi criar delta pois assim consigo manter o historico dos dados e ver quem entrou em cada data.
  O particionamento foi feito pelos campos geograficos (pais,cidade,cidade) como foi pedido. 
  Não teve muitos segredos nessa etapa.

## Gold

  Para a gold eu decidi criar duas tabelas diferentes uma ja com todos os valores pre calculados e outra tendo apenas os municipios pre-calculados,
  ambas exercem o mesmo papel porem precalculando mais valores. Eu tive algumas experiencias onde quanto mais mastigado em forma de linhas e não 
  calcular nenhum valor durante a execução deixava o dashboard de forma muito mais rapida.

### Possiveis melhorias do projeto
  Primeiro mmudar a biblioteca de requisições para uma biblioteca asynchrona, criar um schema para o json antes de criar a camada silver,
  atualmente eu estou contando que o schema não vai sofrer mudanças porem jsons são conhecidos por ter uma natureza volatil e isso pode quebrar
  a ingestão da silver sem motivo claro ou reajustar as colunas da Delta sem motivo. Credenciais abertas, eu coloquei algumas credenciais no codigo
  a senha e o usuario do Minio. As portas internas do spark e do Minio. Caso o projeto fosse ser modularizado de alguma forma talvez fosse interessante
  colocar elas dentro do airflow ou como variaveis do ambiente.

2. **Monitoria e qualidade de dados**
   
   Primeiramente eu forcei alguns erros na pipeline bronze caso os registros entrem de forma incompleta, e alem disso coloquei uma segunda tentativa
   caso aconteça falha em alguma pagina. Porem eu gostaria de fazer o envio para a plataforma de comunicação da equipe, em geral muitos webhooks tem
   sua implementação de forma simples(discord,slack) um pra cada pipeline em caso de falha. Depois disso implementaria uma politica
   de retentar a coleta da bronze com uma folga caso nosso IP fosse banido ou adicionassem um throttle por muitas requisições.
   Para problemas de qualidade de dados, eu gostaria de implementar algumas validações principalmente de geolocalização verificando
   se o pais esta dentro da coordenada daquela região.


   
