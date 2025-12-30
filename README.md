# IoT Monitoring

## Описание проекта

Данный проект представляет собой IoT-платформу для сбора телеметрии от устройств, обработки данных в реальном времени, применения бизнес-правил (Rule Engine), генерации алертов и наблюдаемости системы (Observability).

Система моделирует работу портовой IoT-инфраструктуры (краны, погрузчики, грузовики) и предназначена для демонстрации:

- event-driven архитектуры
- потоковой обработки телеметрии
- применения правил в реальном времени
- мониторинга и логирования микросервисов

---

## Архитектура

```
Data Simulator
      ↓ HTTP
Gateway (Nginx)
      ↓
IoT Controller (FastAPI)
      ↓ RabbitMQ (fanout)
Rule Engine
      ↓
MongoDB (alerts)

+ Prometheus → Grafana (метрики)
+ Filebeat → Logstash → Elasticsearch → Kibana (логи)
```

---

## Используемые технологии

| Компонент | Технология |
|---------|------------|
| API | FastAPI (Python) |
| Rule Engine | Python |
| Message Broker | RabbitMQ |
| Database | MongoDB |
| Metrics | Prometheus |
| Visualization | Grafana |
| Logging | Filebeat, Logstash |
| Search | Elasticsearch |
| Logs UI | Kibana |
| Reverse Proxy | Nginx |
| Containerization | Docker, Docker Compose |

---

## Быстрый старт

### Требования

- Docker >= 24
- Docker Compose v2
- Windows / Linux / macOS

---

### Запуск проекта

В корне репозитория выполните:

```bash
docker compose up --build
```

Первый запуск может занять 3–5 минут (скачивание образов).

---

### Проверка состояния контейнеров

```bash
docker compose ps
```

Все сервисы должны быть в состоянии `running` или `healthy`.

---

## Доступ к сервисам

| Сервис | URL | Логин / Пароль |
|------|-----|----------------|
| Gateway / API | http://localhost:8000 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin / admin` |
| RabbitMQ UI | http://localhost:15672 | `guest / guest` |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |

Grafana при первом входе запросит смену пароля.

---
## Rule Engine — реализованные правила

### CRANE_OVERLOAD
- Условие: `load_weight > 20 тонн`
- Тип: мгновенное
- Severity: **HIGH**

---

### OVERHEAT_RISK_10
- Условие: `temperature > 60°C` **10 сообщений подряд**
- Тип: длящееся
- Severity: **MEDIUM**
- Срабатывает один раз за эпизод (без спама)

---

## Метрики (Prometheus)

Основные метрики:

- `telemetry_messages_processed_total`
- `rules_triggered_total{rule_id}`
- `rule_engine_processing_seconds`
- `rule_engine_overheat_streak{device_id}`

---

## Логирование (ELK stack)

- Логи всех контейнеров собираются через **Filebeat**
- Обработка логов — **Logstash**
- Хранение — **Elasticsearch**
- Просмотр — **Kibana**
- Формат логов — JSON

---

## Тестирование и проверка

Проект был протестирован вручную:

- API `/ingest` — корректно принимает данные
- RabbitMQ — сообщения доставляются
- Rule Engine — правила срабатывают корректно
- MongoDB — алерты сохраняются
- Prometheus — метрики собираются
- Grafana — дашборды отображаются
- Kibana — логи доступны

---

## Структура репозитория

```
.
├── data-simulator/
├── iot-controller/
├── rule-engine/
├── gateway/
├── observability/
│   ├── prometheus/
│   ├── logstash/
│   └── filebeat/
├── docker-compose.yml
└── README.md
```

---

## Масштабирование (scale) `iot-controller` и `rule-engine`

> Важно: у **масштабируемых** сервисов не должно быть фиксированных `ports:` на хост (иначе будет конфликт портов).
> Для доступа к API используем **gateway (nginx)** на `localhost:8000`, поэтому `iot-controller` можно масштабировать без публикации портов наружу.

Команда масштабирования:

```bash
docker compose up -d --scale iot-controller=3 --scale rule-engine=3
```

Проверка, что реплики поднялись:

```bash
docker compose ps
```


