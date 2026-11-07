# control-plane

Оркестратор распределённого расчёта денежных потоков и PV по портфелю кредитных сделок.
Принимает портфель, режет его на батчи, раздаёт задачи сервисам стадий `prep → cashflow → pv`
через NATS JetStream, держит барьеры между стадиями, ретраит упавшие батчи и запускает
агрегацию. Всё состояние расчёта — в Postgres.

## HTTP API

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/v1/runs` | создать ран, ответ `202` с `run_id` |
| `GET` | `/v1/runs/{run_id}` | статус рана и прогресс по стадиям |
| `GET` | `/v1/runs/{run_id}/batches?stage=&status=` | батчи и их состояние на стадиях |
| `POST` | `/v1/runs/{run_id}/cancel` | отменить ран |
| `GET` | `/healthz` | процесс жив |

Ошибки отдаются в формате `{"error": {"code": "run_not_found", "message": "..."}}`.

## Сообщения очереди

| Subject | Направление |
|---|---|
| `data-prep.tasks` / `data-prep.events` | подготовка данных |
| `cashflow.tasks` / `cashflow.events` | расчёт денежных потоков |
| `pv.tasks` / `pv.events` | расчёт PV |
| `aggregator.tasks` / `aggregator.events` | агрегация в отчёт |

