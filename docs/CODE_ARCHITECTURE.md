# Code architecture

この文書はPython実装の責務境界と依存方向を定義する。データ形式、GCS object、
運用上の不変条件は`DESIGN.md`を正とする。

## 構成

```text
src/health_data_pipeline/
  health/                 健康データの意味、source catalog、日次schema
  state/                  content addressing、Snapshot、HEAD CAS、lease、GC
  sync/                   取得計画、ingestion、projection、commit前後のworkflow
  views/                  Snapshot read model、Notion本文、Steps SVG
  setup/                  owner登録、OAuth session、識別子HMAC
  ops/                    Class A予算、同期alert
  integrations/
    google_health/        API、OAuth、subscriber、webhook署名検証
    gcp/                  Secret Manager、Cloud Tasks、Google OIDC
    notion.py             Notion data source adapter
  runtime/
    composition.py        Service / Jobの依存組み立て
    service.py            Cloud Run Service entrypoint
    job.py                Cloud Run Job entrypoint
    cli.py                operator CLI entrypoint
    http/                 FastAPI routesとHTTP境界
```

テストは`tests/health`、`tests/state`、`tests/sync`のように同じfeature名で配置する。
Terraformは`infra/bootstrap`と`infra/main`に分けたままにし、Python package内へ混在させない。

## 依存方向

```text
runtime ──> integrations
   │              │
   ├──> setup <───┤
   ├──> ops   <───┤
   ├──> sync  <───┤
   └──> views <───┘
           │
           ├──> state ──> health
           └────────────> health
```

- `health`はGCS、HTTP、FastAPI、外部adapterをimportしない。
- `state`は`health`のschemaを利用できるが、workflow、view、runtime、feature adapterをimportしない。
  generation/CASと不可分なGCS adapterだけは`state/gcs.py`に同居させる。
- `sync`、`views`、`setup`、`ops`はruntimeや外部adapterをimportしない。
- `integrations`は内側のcontractと値型を実装する。
- `runtime`だけが具体adapterを選び、Cloud Run Service / Jobへ組み立てる。
- feature間の共有は、所有するfeatureの`contracts.py`または値型を直接参照する。
  再exportだけを目的とするfacadeは置かない。

この方向は`tests/test_architecture.py`で静的に検査する。

## GCS stateの責務

`state`はこのプロダクトの中心的な永続化モデルをまとめる。

- `objects.py`: `ObjectStore`、in-memory実装、content-addressed publish/read
- `gcs.py`: generation preconditionを使う本番GCS adapter
- `head.py`: 唯一のmutable pointerであるHEADの読取とcompare-and-swap
- `snapshots.py`: 日次revision、年shard、Root Snapshotの構築と検証
- `commit.py`: Snapshot公開後にHEADを一度だけCASするcommit coordinator
- `lease.py`: advisory lease。commit可否の正本にはしない
- `versioned.py`: OAuthやview state向けの小さなgeneration付きstate helper
- `gc.py`: HEADからの到達可能性に基づくmaintenance policy

永続化のcommit pointはHEAD CASだけである。content-addressed object、ingestion manifest、
Snapshotの作成成功をcommitとして扱わない。通常readはlistingで最大generationを探さず、
HEADから参照された正確なURIを辿る。

## Syncの責務

`IncrementalSyncPipeline`は次の順序を調停する。

1. advisory leaseを取得する。
2. `planning.py`でWebhook、現在照合、backfillから取得計画を作る。
3. `ingestion.py`がAPI原本、flattened source、Manifestを不変保存する。
4. `projection.py`が既存revisionと取得結果から完全な日次置換を作る。
5. `CommitCoordinator`がRoot Snapshotを構築してHEADを一度だけCAS更新する。
6. `runner.py`がcommit後のview taskとpending sync recoveryを要求する。

`FetchWindow`と`DataPointPage`はsource adapterへ要求する同期contractであり、
Google Health固有moduleではなく`sync/contracts.py`が所有する。

旧revisionの変換はrowのfield有無から推測せず、
`DailyRevisionReference.schema_version`を`sync/migrations.py`へ渡して明示的に行う。

## Viewsと外部連携

`views`はHEAD / Snapshotからread modelを作り、Notion本文とSteps SVGを決定的に生成する。
Notion API呼び出しは`integrations/notion.py`、Google Healthの取得・OAuth・Webhook署名は
`integrations/google_health`に置く。viewのmonotonic generationとSteps pointerの
content-addressed contractは外部API adapterへ移さない。

## Entrypoint contract

TerraformとDockerが利用するコマンド名は安定contractとする。

- `health-data-pipeline-service` → `runtime.service:run_service`
- `health-data-pipeline-job` → `runtime.job:main`
- `health-data-pipeline` → `runtime.cli:main`

ServiceとJobは同じcontainer imageを使う。module import時にcredential取得、network access、
server起動を行わず、実行時だけ`runtime/composition.py`で依存を生成する。

## 分割しないもの

- 汎用DI container、event bus、generic repository hierarchyは導入しない。
- 健康データの純粋計算は行数だけを理由に細切れにしない。
- 一つの不変条件を複数serviceへ分散しない。
- Terraformのbootstrap/main境界や、Service / Jobの同一image運用をPython構成変更へ巻き込まない。
