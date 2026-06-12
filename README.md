# IoT Monitoring

## Описание проекта

Проект реализует backend-прототип IoT-платформы для сбора и обработки телеметрии портовой инфраструктуры: кранов, погрузчиков и грузового транспорта.

Архитектура соответствует событийно-ориентированному подходу из `Этап_№1.docx`: данные принимаются через API Gateway, валидируются FastAPI-сервисом, сохраняются в MongoDB и асинхронно передаются через RabbitMQ в Rule Engine. Для краткоживущего состояния правил используется Redis, для метрик - Prometheus/Grafana, для логов - Filebeat/Logstash/Elasticsearch/Kibana.

## Архитектура

```text
Data Simulator
      ↓ HTTP/JSON
Gateway (Nginx)
      ↓ HTTP
IoT Controller (FastAPI)
      ↓ MongoDB: raw telemetry
RabbitMQ (fanout exchange)
      ↓ AMQP
Rule Engine
      ↓ MongoDB: alerts/events
Redis: short-lived rule state

Prometheus → Grafana
Filebeat → Logstash → Elasticsearch → Kibana
```

## Используемые технологии

| Компонент | Технология |
| --- | --- |
| Источник данных | Python data simulator |
| API Gateway | Nginx |
| API | FastAPI |
| Message Broker | RabbitMQ |
| Rule Engine | Python |
| Основное хранилище | MongoDB |
| Состояние правил | Redis |
| Метрики | Prometheus |
| Визуализация | Grafana |
| Логирование | Filebeat, Logstash, Elasticsearch, Kibana |
| Развертывание | Docker Compose |

## Запуск

```bash
docker compose up --build
```

Первый запуск может занять несколько минут из-за загрузки Docker-образов.

Для локального Python-окружения:

```bash
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Доступ к сервисам

| Сервис | URL | Логин / пароль |
| --- | --- | --- |
| Gateway / API | http://localhost:8000 | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | `admin / admin` |
| RabbitMQ UI | http://localhost:15672 | `guest / guest` |
| Kibana | http://localhost:5601 | - |
| Elasticsearch | http://localhost:9200 | - |

Grafana автоматически получает Prometheus datasource и dashboard `IoT Port Platform`.

## Rule Engine

Реализованные правила:

| Rule ID | Условие | Severity |
| --- | --- | --- |
| `CRANE_OVERLOAD` | `device_type=crane` и `load_weight > 20t` | HIGH |
| `OVERHEAT_RISK_10` | `temperature > 60C` 10 сообщений подряд | MEDIUM |
| `ERROR_STATUS_DURATION` | `status=error` 5 сообщений подряд | HIGH |
| `DEVICE_CONNECTION_LOST` | нет телеметрии от устройства более 30 секунд | HIGH |
| `ANOMALOUS_TELEMETRY` | аномальная температура или масса груза | MEDIUM |

Redis хранит краткоживущее состояние для последовательных правил и `last_seen` устройств. Это позволяет масштабировать `rule-engine` без хранения state в памяти конкретного контейнера.

## Нагрузка симулятора

Дефолтная нагрузка в Docker Compose сделана безопасной для локального запуска:

```text
DEVICE_COUNT=20
MSG_RATE_PER_DEVICE=1
```

То есть примерно 20 сообщений в секунду. Нагрузку можно поднять через env-переменные в `docker-compose.yml`.

## Метрики

Ключевые метрики:

- `telemetry_messages_processed_total`
- `alerts_created_total`
- `rules_triggered_total{rule_id}`
- `rule_engine_messages_total{status}`
- `rule_engine_processing_seconds`
- `rule_engine_overheat_streak{device_id}`
- `rule_engine_error_status_streak{device_id}`

Prometheus также загружает alert rules из `observability/prometheus/alerts.rules.yml`.

## Логирование

Основные сервисы пишут структурированные JSON-логи. Filebeat собирает container logs, передает их в Logstash, после чего записи индексируются в Elasticsearch и доступны в Kibana.

## Проверка

После запуска контейнеров:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\healthcheck.ps1
```

Скрипт проверяет gateway, ingest, RabbitMQ UI, MongoDB, Prometheus, загрузку alert rules, Grafana, Elasticsearch и Kibana. Метрики `rule-engine` проверяются через Prometheus, потому что порт `9101` не публикуется наружу и не мешает масштабированию.

## Масштабирование

`iot-controller` и `rule-engine` не публикуют фиксированные host-порты, поэтому их можно масштабировать:

```bash
docker compose up -d --scale iot-controller=3 --scale rule-engine=3
```

Входной трафик идет через Nginx на `localhost:8000`, а Prometheus использует Docker DNS discovery для поиска реплик.
