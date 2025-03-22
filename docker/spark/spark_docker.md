# Spark Cluster on Docker

Quick start guide for setting up a distributed Spark cluster using Docker.

## Architecture Overview

```mermaid
graph TD
    subgraph Cluster
        master[Spark Master] <--> worker[Spark Worker]
        master <--> jupyter[JupyterLab]
        worker <--> jupyter
    end
    
    subgraph Infrastructure
        vol[Shared Volume]
        net[Network: ${PROJECT_NAME}-network]
        gcs[GCS Connector]
    end
    
    Cluster --- Infrastructure
```

## Spark Cluster Components
- **Master Node**: Coordinates work distribution
- **Worker Node**: Executes tasks
- **JupyterLab**: Development environment
- **GCS Connector**: Google Cloud Storage access

## Docker Images
- **cluster-base**: Foundation image with Java 17 and Python 3
- **spark-base**: Spark 3.3.1 with Hadoop 3 support
- **spark-master**: Configured Spark master node
- **spark-worker**: Configured Spark worker node
- **jupyterlab**: JupyterLab 3.6.1 with PySpark support

## Quick Setup

1. **Build Images**
   ```bash
   ./build.sh   # Creates all required Docker images
   ```

2. **Create Network & Volume**
   ```
   docker network create ${PROJECT_NAME}-network
   docker volume create --name=hadoop-distributed-file-system
   ```

3. **Start Services**
   ```bash
   docker-compose -f ./docker/spark/docker-compose.yml up -d
   ```
   Tip: Run `start.sh` to automatically set up the entire environment.

4. **Stop Services**
   ```bash
   docker-compose down
   ```

5. **Utility Commands**
   ```bash
   # Remove all containers
   docker rm -f $(docker ps -a -q)
   
   # Remove all volumes
   docker volume rm $(docker volume ls -q)
   ```
