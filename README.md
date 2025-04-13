# Distributed Systems Microservices for Minecraft

A distributed system for trading data analysis with microservices architecture using data from Hypixel Skyblock.

> **Note**: I know this wildly overengineered when SQLite + Celery could probably do the job, but where is the fun in that? I built this to fool around with some things mentioned on Designing Data-intensive applications. 

## System Components

- **Data Ingestion Service**: Fetches trading data from  my API to mimic market data.
- **Database Service**: Stores data in PostgreSQL. 
- **Database Gateway**: Provides API for database operations
- **LLM Service**: AI-powered trading analysis using DeepSeek.
- **Fuzzy Search**: Fuzzy Searching with a sliding  window

## Infrastructure

- Kafka for messaging
- PostgreSQL for data storage
- EFK stack for logging
- Prometheus/Grafana for monitoring
- Docker & Docker Compose for containerization

## Getting Started


1. Run all services:
docker-compose up 


## API Endpoints

- Fuzzy Search: `http://localhost:8000/fuzzy?query=your_search_term`
- Database: `http://localhost:8001/api/v1/read_data/?data=item_name`
- DB Gateway: `http://localhost:8002/api/v1/get_cache`
- LLM Service: `http://localhost:8004/process` (POST with JSON `{"query": "something"}`)

## Monitoring

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- Kibana: http://localhost:5601

## Future Plans

- Improve Readme
- Added Cassandra to match prod, should be easy because all I need to do is add a topic for Cassandra as a sub to apiquery which is the topic.
- Add Discord Webhook support
- Add better instructions to running this. 

## PR Requests
- If you would like to add more absurd stuff such as blockchain (????) or god help me (two phase committing) to postgres and cassandra when I do add it sure go ahead. 


## Disclaimer

Do NOT use this to actually trade on Minecraft Skyblock, you will lose coins. This is more a less a sandbox for distributed systems.
