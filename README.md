# 4차시 과제: Silver Layer / Gold Layer 테이블 설계

## 1. 과제 개요

본 과제에서는 광고 이벤트 데이터를 대상으로 Silver Layer와 Gold Layer의 Iceberg 테이블을 설계하였다. Raw 데이터는 Kafka에서 수집되어 S3에 Parquet 형식으로 저장된 Bronze 영역이며, 본 과제의 설계 대상은 다음 두 개 테이블이다.

| Layer | Table | 설명 |
|---|---|---|
| Silver | `ad_lake.processed_events` | raw impression/click/conversion 이벤트를 `event_id` 기준으로 결합한 이벤트 단위 정제 테이블 |
| Gold | `ad_lake.campaign_summary` | `processed_events`를 날짜 및 캠페인 기준으로 집계한 분석용 요약 테이블 |

Athena의 `AwsDataCatalog`와 `ad_lake` 데이터베이스에서 다음 테이블들이 확인되었다.

```text
campaign_summary
processed_events
raw_clicks
raw_conversions
raw_impressions
```

---

## 2. Silver Layer: `processed_events`

### 2.1 테이블 설계 목적

`processed_events` 테이블은 Raw 영역에 이벤트 타입별로 분리 저장된 `raw_impressions`, `raw_clicks`, `raw_conversions` 데이터를 `event_id` 기준으로 조립한 Silver Layer 테이블이다.

설계 기준은 다음과 같다.

- `impression` 이벤트를 기준 데이터로 사용한다.
- `click`, `conversion` 이벤트는 `event_id` 기준으로 left join한다.
- 동일한 `event_id`에 대해 click 또는 conversion이 나중에 도착할 수 있으므로 단순 `INSERT` 대신 `MERGE INTO`를 사용한다.
- `event_date` 기준으로 파티셔닝하여 날짜별 조회 및 집계가 가능하도록 설계하였다.

---

### 2.2 Table DDL

아래 DDL은 `raw_to_processed_iceberg.py`의 `ensure_processed_table()`에서 생성하는 Spark Iceberg 테이블 DDL을 기준으로 정리하였다.

```sql
CREATE TABLE IF NOT EXISTS glue_catalog.ad_lake.processed_events (
    event_id STRING,
    event_date DATE,
    uid STRING,
    campaign INT,
    click INT,
    conversion INT,
    conversion_delay_sec BIGINT,
    cost DOUBLE,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
);
```

---

### 2.3 Insert / Merge Query

본 테이블은 지연 도착하는 click/conversion 이벤트를 같은 `event_id` row에 반영해야 하므로, `INSERT` 대신 `MERGE INTO`를 사용하였다.

```sql
MERGE INTO glue_catalog.ad_lake.processed_events t
USING source_processed_events s
ON t.event_id = s.event_id

WHEN MATCHED THEN
  UPDATE SET
    t.event_date = s.event_date,
    t.uid = s.uid,
    t.campaign = s.campaign,
    t.click = s.click,
    t.conversion = s.conversion,
    t.conversion_delay_sec = s.conversion_delay_sec,
    t.cost = s.cost,
    t.updated_at = s.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    event_id,
    event_date,
    uid,
    campaign,
    click,
    conversion,
    conversion_delay_sec,
    cost,
    updated_at
  )
  VALUES (
    s.event_id,
    s.event_date,
    s.uid,
    s.campaign,
    s.click,
    s.conversion,
    s.conversion_delay_sec,
    s.cost,
    s.updated_at
  );
```

---

### 2.4 테이블 검증 쿼리 및 결과

#### 2.4.1 전체 row 수 검증

```sql
SELECT COUNT(*) AS processed_event_count
FROM ad_lake.processed_events;
```

Athena 실행 결과:

| processed_event_count |
|---:|
| 1000 |

`processed_events` 테이블에 총 1,000건의 이벤트가 적재되었음을 확인하였다.

---

#### 2.4.2 날짜별 row 수 검증

```sql
SELECT
    event_date,
    COUNT(*) AS cnt
FROM ad_lake.processed_events
GROUP BY event_date
ORDER BY event_date;
```

Athena 실행 결과:

| event_date | cnt |
|---|---:|
| 2026-03-31 | 712 |
| 2026-04-01 | 288 |

`processed_events`는 `event_date` 기준으로 파티셔닝되어 있으며, 실제 데이터가 두 날짜에 걸쳐 분포한 것을 확인하였다.

---

#### 2.4.3 Null key 검증

```sql
SELECT COUNT(*) AS null_event_id_count
FROM ad_lake.processed_events
WHERE event_id IS NULL;
```

예상 결과:

| null_event_id_count |
|---:|
| 0 |

---

#### 2.4.4 중복 key 검증

```sql
SELECT COUNT(*) AS duplicated_event_id_count
FROM (
    SELECT event_id
    FROM ad_lake.processed_events
    GROUP BY event_id
    HAVING COUNT(*) > 1
);
```

예상 결과:

| duplicated_event_id_count |
|---:|
| 0 |

---

#### 2.4.5 기본 집계 검증

```sql
SELECT
    COUNT(*) AS impressions,
    SUM(click) AS clicks,
    SUM(conversion) AS conversions,
    SUM(cost) AS total_cost
FROM ad_lake.processed_events;
```

검증 목적은 다음과 같다.

- `COUNT(*)`: impression 기준 이벤트 수
- `SUM(click)`: click 이벤트가 결합된 수
- `SUM(conversion)`: conversion 이벤트가 결합된 수
- `SUM(cost)`: 전체 광고 비용 합계

---

### 2.5 Snapshot 비교 결과

Iceberg snapshot metadata table을 Athena에서 조회하였다.

```sql
SELECT *
FROM ad_lake."processed_events$snapshots"
ORDER BY committed_at;
```

Athena 실행 결과 `processed_events` 테이블에서 총 2개의 snapshot이 확인되었다.

| committed_at | operation | 설명 |
|---|---|---|
| 2026-05-01 05:13:46.278 UTC | overwrite | 최초 테이블 생성 및 적재 시점의 snapshot |
| 2026-05-01 05:25:42.508 UTC | overwrite | 재실행 또는 갱신 이후 생성된 snapshot |

이를 통해 `processed_events` 테이블의 변경 이력이 Iceberg snapshot 단위로 관리되고 있음을 확인하였다.

---

## 3. Gold Layer: `campaign_summary`

### 3.1 테이블 설계 목적

`campaign_summary` 테이블은 Silver Layer의 `processed_events`를 날짜와 캠페인 기준으로 집계한 Gold Layer 테이블이다.

설계 기준은 다음과 같다.

- `summary_date`, `campaign` 조합을 집계 기준으로 사용한다.
- `impressions`, `clicks`, `conversions`, `total_cost`를 집계한다.
- 광고 성과 지표인 `ctr`, `cvr`, `cpa`를 계산한다.
- 동일한 `summary_date`, `campaign` 조합은 재집계 시 갱신되어야 하므로 `MERGE INTO`를 사용한다.
- `summary_date` 기준으로 파티셔닝한다.

---

### 3.2 Table DDL

아래 DDL은 `processed_to_campaign_summary.py`의 `ensure_summary_table()`에서 생성하는 Spark Iceberg 테이블 DDL을 기준으로 정리하였다.

```sql
CREATE TABLE IF NOT EXISTS glue_catalog.ad_lake.campaign_summary (
    summary_date DATE,
    campaign INT,
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    total_cost DOUBLE,
    ctr DOUBLE,
    cvr DOUBLE,
    cpa DOUBLE,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);
```

---

### 3.3 Insert / Merge Query

`campaign_summary`는 `summary_date`, `campaign` 기준 집계 결과를 저장한다. 동일한 날짜와 캠페인 조합이 이미 존재하면 기존 집계 row를 갱신하고, 존재하지 않으면 신규 row를 삽입하기 위해 `MERGE INTO`를 사용하였다.

```sql
MERGE INTO glue_catalog.ad_lake.campaign_summary t
USING (
  SELECT
    event_date AS summary_date,
    campaign,
    COUNT(*) AS impressions,
    SUM(click) AS clicks,
    SUM(conversion) AS conversions,
    SUM(cost) AS total_cost,
    current_timestamp() AS updated_at
  FROM glue_catalog.ad_lake.processed_events
  WHERE event_date >= current_date() - INTERVAL 3650 DAYS
  GROUP BY event_date, campaign
) s
ON t.summary_date = s.summary_date
AND t.campaign = s.campaign

WHEN MATCHED THEN
  UPDATE SET
    t.impressions = s.impressions,
    t.clicks = s.clicks,
    t.conversions = s.conversions,
    t.total_cost = s.total_cost,
    t.ctr = CASE
              WHEN s.impressions > 0
              THEN s.clicks * 100.0 / s.impressions
              ELSE 0
            END,
    t.cvr = CASE
              WHEN s.clicks > 0
              THEN s.conversions * 100.0 / s.clicks
              ELSE 0
            END,
    t.cpa = CASE
              WHEN s.conversions > 0
              THEN s.total_cost / s.conversions
              ELSE NULL
            END,
    t.updated_at = s.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    summary_date,
    campaign,
    impressions,
    clicks,
    conversions,
    total_cost,
    ctr,
    cvr,
    cpa,
    updated_at
  )
  VALUES (
    s.summary_date,
    s.campaign,
    s.impressions,
    s.clicks,
    s.conversions,
    s.total_cost,
    CASE WHEN s.impressions > 0 THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
    CASE WHEN s.clicks > 0 THEN s.conversions * 100.0 / s.clicks ELSE 0 END,
    CASE WHEN s.conversions > 0 THEN s.total_cost / s.conversions ELSE NULL END,
    s.updated_at
  );
```

---

### 3.4 테이블 검증 쿼리 및 결과

#### 3.4.1 Gold 테이블 샘플 조회

```sql
SELECT *
FROM ad_lake.campaign_summary
ORDER BY summary_date, campaign
LIMIT 20;
```

Athena 실행 결과 `campaign_summary` 테이블에서 날짜 및 캠페인별 집계 결과가 조회되었다.

예시 결과:

| summary_date | campaign | impressions | clicks | conversions | total_cost | ctr | cvr | cpa |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-03-31 | 73328 | 2 | 0 | 0 | 0.0000839546 | 0.0 | 0.0 | NULL |
| 2026-03-31 | 83677 | 2 | 1 | 0 | 0.0022866237 | 50.0 | 0.0 | NULL |
| 2026-03-31 | 408759 | 5 | 3 | 1 | 0.0005432013 | 60.0 | 33.3333 | 0.0005432013 |

이를 통해 Gold Layer에서 `impressions`, `clicks`, `conversions`, `total_cost`, `ctr`, `cvr`, `cpa`가 정상적으로 산출되었음을 확인하였다.

---

#### 3.4.2 Gold row 수 검증

```sql
SELECT COUNT(*) AS campaign_summary_count
FROM ad_lake.campaign_summary;
```

검증 기준:

```text
campaign_summary_count > 0
```

`campaign_summary`는 `summary_date`, `campaign` 조합 단위의 집계 테이블이므로 row 수는 전체 이벤트 수가 아니라 날짜와 캠페인 조합 수에 해당한다.

---

#### 3.4.3 Silver / Gold 집계 일치 검증

Silver 전체 집계:

```sql
SELECT
    COUNT(*) AS silver_impressions,
    SUM(click) AS silver_clicks,
    SUM(conversion) AS silver_conversions,
    SUM(cost) AS silver_total_cost
FROM ad_lake.processed_events;
```

Gold 전체 집계:

```sql
SELECT
    SUM(impressions) AS gold_impressions,
    SUM(clicks) AS gold_clicks,
    SUM(conversions) AS gold_conversions,
    SUM(total_cost) AS gold_total_cost
FROM ad_lake.campaign_summary;
```

검증 기준:

```text
silver_impressions = gold_impressions
silver_clicks = gold_clicks
silver_conversions = gold_conversions
silver_total_cost = gold_total_cost
```

위 결과가 일치하면 `campaign_summary`가 `processed_events`를 기준으로 정상 집계되었음을 의미한다.

---

### 3.5 Snapshot 비교 결과

Iceberg snapshot metadata table을 Athena에서 조회하였다.

```sql
SELECT *
FROM ad_lake."campaign_summary$snapshots"
ORDER BY committed_at;
```

Athena 실행 결과 `campaign_summary` 테이블에서 총 2개의 snapshot이 확인되었다.

| committed_at | operation | 설명 |
|---|---|---|
| 2026-05-01 05:34:44.767 UTC | overwrite | 최초 Gold 집계 테이블 생성 및 적재 시점의 snapshot |
| 2026-05-01 05:42:16.111 UTC | overwrite | 재실행 또는 갱신 이후 생성된 snapshot |

이를 통해 `campaign_summary` 테이블의 변경 이력이 Iceberg snapshot 단위로 관리되고 있음을 확인하였다.

---

## 4. 최종 검증 요약

| 항목 | 결과 |
|---|---|
| Database | `ad_lake` |
| 확인된 테이블 | `processed_events`, `campaign_summary`, `raw_impressions`, `raw_clicks`, `raw_conversions` |
| Silver row 수 | 1,000건 |
| Silver 날짜 분포 | 2026-03-31: 712건, 2026-04-01: 288건 |
| Gold 집계 결과 | 날짜 및 캠페인 기준 집계 row 생성 |
| Silver snapshot | 2개 확인 |
| Gold snapshot | 2개 확인 |

---

## 5. 결론

본 과제에서는 Silver Layer와 Gold Layer에 각각 하나의 Iceberg 테이블을 설계하였다.

Silver Layer의 `processed_events`는 이벤트 타입별 raw 데이터를 `event_id` 기준으로 결합하여 이벤트 단위 정제 데이터를 구성하였다. 지연 도착 이벤트를 반영하기 위해 `event_id` 기준 `MERGE INTO`를 사용하였다.

Gold Layer의 `campaign_summary`는 `processed_events`를 날짜와 캠페인 기준으로 집계하여 광고 성과 분석에 필요한 `impressions`, `clicks`, `conversions`, `total_cost`, `ctr`, `cvr`, `cpa`를 계산하였다. 집계 결과의 재계산 및 갱신을 위해 `summary_date`, `campaign` 기준 `MERGE INTO`를 사용하였다.

Athena 검증 결과 두 테이블 모두 정상적으로 조회되었으며, Iceberg snapshot metadata를 통해 테이블 변경 이력이 snapshot 단위로 관리됨을 확인하였다.
