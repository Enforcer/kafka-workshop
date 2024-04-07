# Kafka for Python developers
## Prerequisites
- Install [docker](https://docs.docker.com/engine/install/) (or similar solution for containerization, e.g. colima or podman)
- Have daemon up and running so that `docker ps` runs ok

## Setup and verify

First, start basic services (Apache Kafka and console) using a following command:

```bash
docker compose up
```

Wait a few seconds (5-10) then run second command in another terminal:

```bash
docker compose -f docker-compose.verify.yml up --build
```

After the build succeeds, you should see at a very end a message identical or very similar to:
```
kafka-training-faust_instance-1  | [2023-10-22 16:26:32,790] [1] [INFO] google.com has now 1 clicks!
```

If you got the message, you are all set!
