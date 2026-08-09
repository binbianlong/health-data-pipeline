# Code architecture

この文書はPython実装の責務境界と依存方向を定義する。データ形式、GCS object、
運用上の不変条件は`DESIGN.md`を正とする。

## 依存方向

```text
entrypoints / bootstrap
          ↓
      application  → ports/contracts
          ↓                 ↑
        domain           adapters
          ↓
      persistence
```

- `domain`は健康指標、睡眠、日次集計、schemaの意味を持ち、GCS、HTTP、FastAPIをimportしない。
- `application`は同期とView更新のユースケースを調停し、entrypointをimportしない。
- `persistence`はgeneration付きstate、Snapshot、content-addressed保存のrepositoryを持つ。
- `adapters`はGoogle Health、Notion、GCPなど外部API固有の処理を持つ。
- `presentation`はSnapshotからread modelを作り、Notion本文とSVGへ描画する。
- `entrypoints`と`bootstrap`だけが具体adapterを組み立てる。

`data_types.py`はGoogle Healthの取得条件、Webhook/backfill属性、canonical metricとの対応を
一か所で管理する共有カタログである。指標定義や同期計画はこの中立moduleを参照し、
`adapters.google_health.catalog`は旧import向けのfacadeだけを提供する。

## Syncの責務

`IncrementalSyncPipeline`は次の順序だけを調停する。

1. leaseを取得する。
2. `planning`でWebhook、現在照合、backfillから取得計画を作る。
3. `SourceIngestor`がAPI原本、flattened-source、Manifestを不変保存する。
4. `DailyProjector`が既存revisionと取得結果から完全な日次置換を作る。
5. `CommitCoordinator`がRoot Snapshotを構築してHEADを一度だけCAS更新する。

HEAD CASだけをcommit pointとする規約は、モジュール分割後も変更しない。取得途中のobjectや
Manifestをcommit pointとして扱わない。

## Schema migration

旧revisionの変換はrowのfield有無から推測せず、`DailyRevisionReference.schema_version`を
`application.sync.migrations`へ渡して明示的に行う。移行関数は入力versionと出力versionを
固定し、golden testを持つ。

## Compatibility facade

Cloud Run、Docker、Terraform、テストや既存importを一度に壊さないため、移行中は従来の
`pipeline.py`、`views.py`、`notion_adapter.py`、`job.py`から新moduleをre-exportする。
新規コードは新しいpackageを直接importし、移行完了後にfacade削除を別変更として判断する。

## 分割しないもの

- 汎用DI container、event bus、generic repository hierarchyは導入しない。
- 数行のparserや同名helperを機械的に共通化しない。
- 一つの不変条件を複数serviceへ分散しない。
- ファイル行数だけを理由に、凝集した純粋ロジックを細切れにしない。
