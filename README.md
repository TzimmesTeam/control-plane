# Control Plane

Машина состояний для расчёта портфеля. PostgreSQL хранит запуск,
текущее состояние батчей и задачи на отправку. NATS JetStream доставляет задачи
воркерам и возвращает результаты. Финансовые данные остаются в S3.

## Состояния и хранение

Запуск: `waiting → preparing → running → aggregating → succeeded`.
Ошибки после исчерпания попыток переводят запуск в `failed`, отмена — в
`canceled`. Подготовленный Parquet с нулём принятых сделок даёт `failed`
с явной причиной; пустой отчёт автоматически не считается успехом.

Батч: `cashflow → pv → done`, при окончательном сбое — `failed`.
Это текущая стадия батча, а не подтверждение, что воркер уже начал вычисления.
Единица состояния — батч; отдельных статусов сделок нет.

Три таблицы:

- `cp_runs`: уникальный `snapshot_id`, параметры и состояние запуска;
- `cp_batches`: идентификатор, номер и типизированная JSONB-запись текущего батча;
- `cp_tasks`: неизменяемое сообщение задачи и состояние доставки
  `pending → publishing → sent → done`, срок исполнения и срок захвата отправителем.

Run и Batch сериализуются из Go-структур в JSONB, чтобы не дублировать десятки
полей в SQL-сканерах. Ключи, связи и сроки доставки — обычные SQL-колонки;
состояния индексируются. Списков финансовых строк внутри записей БД нет.
История всех стадий отдельной таблицей не ведётся; сообщения попыток сохраняются.

Обработчик результата блокирует строку запуска, проверяет актуальную задачу,
обновляет батч и добавляет следующую задачу **одной транзакцией**. Последний
PV-батч переводит запуск в `aggregating`. Планировщик пишет JSON-манифест
результатов в ARTIFACTS и атомарно регистрирует задачу агрегатору.

Отправитель берёт задачи из PostgreSQL с `FOR UPDATE SKIP LOCKED`. Сетевых
вызовов под блокировкой запуска нет. При рестарте используются те же записи;
отдельной очереди в памяти и процедуры восстановления нет.

## HTTP и регистрация поставки

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/v1/runs` | необязательно зарегистрировать параметры заранее |
| POST | `/snapshots/prepared` | callback Data Prep: `{"snapshot_id":"ab12"}` |
| GET | `/v1/runs/{run_id}` | параметры, состояние и ошибка запуска |
| GET | `/v1/runs/{run_id}/batches` | текущие состояния батчей и ссылки на результаты |
| POST | `/v1/runs/{run_id}/cancel` | прекратить дальнейшее продвижение запуска |
| GET | `/healthz` | жив ли HTTP-сервер |
| GET | `/readyz` | доступны ли PostgreSQL и NATS |

Для запуска достаточно одного callback Data Prep:

```json
{"snapshot_id":"ab12"}
```

`portfolio_id`, `report_dttm` и `curves_uri` необязательны. Если Data Prep знает
эти параметры, он может передать их в том же callback. Например:

```json
{
  "snapshot_id": "ab12",
  "portfolio_id": "portfolio-1",
  "report_dttm": "2026-11-17",
  "curves_uri": "s3://curated/curve_snapshots/base.parquet"
}
```

```json
{
  "snapshot_id": "ab12",
  "portfolio_id": "portfolio-1",
  "report_dttm": "2026-11-16",
  "curves_uri": "s3://curated/curve_snapshots/base.parquet",
  "prepared_uri": "s3://curated/source=core_banking/ingest_date=2026-11-16/snapshot_id=ab12/deals.parquet",
  "engine_version": "agreed-engine-version"
}
```

`prepared_uri` и `engine_version` также необязательны. `portfolio_id` — непрозрачный
идентификатор, не обязательно UUID. Переданные дата и S3 URI проверяются по формату.

Регистрация необязательна: один callback создаёт запуск с `prepared=true`,
заполняет отсутствующие параметры дефолтами и делает запуск доступным планировщику.
Регистрация без callback только сохраняет параметры и ждёт готовности Data Prep.
Повторный callback безопасен и возвращает тот же `run_id`. Один глобально
уникальный `snapshot_id` соответствует одному запуску одного портфеля.
Новый пересчёт регистрируется с новым `snapshot_id`.

## Соглашение о S3

Если `prepared_uri` задан, используется он. Иначе:

1. При заполненном `s3.prepared_key` подставляется `{snapshot_id}`:
   например `snapshots/{snapshot_id}/deals.parquet`.
2. При пустом `prepared_key` сервис ищет ровно один объект под
   `s3.prepared_prefix`, заканчивающийся на
   `/snapshot_id=<id>/deals.parquet`. Это поддерживает текущий путь Data Prep
   с `source=.../ingest_date=.../snapshot_id=...` без знания даты из callback.
   Если совпадений несколько, требуется явный `prepared_uri`.

Для большого бакета лучше настроить прямой шаблон или передать URI при
регистрации: обход префикса — совместимость с текущим минимальным callback.

Control Plane пишет только служебные артефакты:

```text
s3://artifacts/runs/<run_id>/plans/<token>/batches/seq=<n>/ids.parquet
s3://artifacts/runs/<run_id>/manifests/batches.json
```

`ids.parquet` содержит одну обязательную колонку `deal_id: int64`.
Разные попытки планирования имеют разные префиксы, поэтому зависший старый
планировщик не перезаписывает опубликованные списки нового.
Версия и ETag подготовленного объекта закрепляются при чтении и уходят воркерам.
Кривые должны приходить по неизменяемому URI: их версионирование здесь не реализовано.

Воркерам предлагаются отдельные места записи для каждой задачи:

```text
s3://curated/cf_projection/run_id=<run_id>/task_id=<task_id>/
s3://curated/pv_results/run_id=<run_id>/task_id=<task_id>/
s3://artifacts/reports/run_id=<run_id>/task_id=<task_id>/
```

Control Plane не создаёт таблицы Iceberg и не управляет их metadata-файлами.
Если вычислители используют Iceberg, публикация снимков таблиц и интерпретация
ссылок на результаты остаются их обязанностью. RAW, QUARANTINE и SCRATCH
этот сервис не изменяет. Незавершённые артефакты планирования автоматически
не удаляются; срок хранения настраивается инфраструктурой.

## Протокол очереди

По умолчанию stream `CONTROL_PLANE`, subjects:

| Стадия | Задачи | Результаты |
|---|---|---|
| cashflow | `calc.cashflow.tasks` | `calc.cashflow.events` |
| pv | `calc.pv.tasks` | `calc.pv.events` |
| aggregate | `calc.aggregate.tasks` | `calc.aggregate.events` |

Stream и префикс настраиваются. Все сообщения задачи самодостаточны:

```json
{
  "task_id": "uuid",
  "run_id": "uuid",
  "batch_id": "uuid",
  "snapshot_id": "ab12",
  "stage": "cashflow",
  "attempt": 1,
  "portfolio_id": "portfolio-1",
  "report_dttm": "2026-11-16",
  "source_uri": "s3://curated/.../deals.parquet",
  "source_version_id": "optional-s3-version",
  "source_etag": "etag",
  "curves_uri": "s3://curated/curve_snapshots/base.parquet",
  "deal_ids_link": "s3://artifacts/.../ids.parquet",
  "output_prefix": "s3://curated/cf_projection/run_id=.../task_id=.../"
}
```

У PV дополнительно `input_links`: подтверждённые `output_links` cashflow
своего батча. У агрегатора нет `batch_id`/`deal_ids_link`, вместо них —
`batches_link`, URI JSON-массива записей Batch с `batch_id`, `seq`,
`deal_count`, `cashflow_links` и `pv_links`.

Воркер читает подготовленные сделки, фильтруя по IDs, закрепляет чтение
`source_version_id` или условием If-Match по `source_etag`, записывает
результаты и публикует событие:

```json
{"task_id":"uuid","status":"success","output_links":[{"uri":"s3://curated/.../result.parquet"}]}
```

Ошибка: `{"task_id":"uuid","status":"error","error":"описание"}`.
Успех агрегатора: `{"task_id":"uuid","status":"success","report_link":"s3://artifacts/.../report.pdf"}`.
Для cashflow/PV непустые `output_links` обязательны; для агрегатора обязателен
`report_link`. Событие должно прийти на subject соответствующей стадии.

Ошибка воркера или истечение `lease_ttl` создаёт новую задачу с увеличенным
`attempt` и отдельным префиксом результата. Старые события игнорируются.
После `max_attempts` весь запуск становится `failed`. Срок исполнения начинается
после подтверждённой публикации и включает ожидание воркера в очереди;
настройте его с учётом нагрузки.

Control Plane подтверждает события после коммита БД. При временной ошибке
обработки NAK с задержкой, без ограничения числа повторов. Некорректные события
терминируются и логируются. Отмена прекращает новые переходы и захват задач
отправителем. Уже захваченная публикация или запущенный воркер могут завершить
работу; поздний результат отменённого запуска игнорируется.