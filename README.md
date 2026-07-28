# health-data-pipeline

Fitbit Sense 2のGoogle Health APIデータをGCSへ保存し、日次値・月曜始まりの暦週平均・暦月の月初来平均をNotionへ反映する個人用パイプラインです。GitHubプロフィール向けには直近7日分の歩数SVGだけを公開します。

BigQueryは使用しません。HEAD、Root Snapshot、年単位Shard、content-addressedな日次revisionにより、将来BigQueryを追加しても取得・日次計算を作り直さない構造です。詳細設計は[DESIGN.md](DESIGN.md)を参照してください。

## 構成

```text
Google Health webhook / Browser / GitHub
                  ↓
Firebase Hosting（<PROJECT_ID>.web.app、完全一致の7 route）
                  ↓
Cloud Run Service（Invoker IAM check無効、内部routeはアプリでOIDC保護）
                  ↓ GCS event
             Cloud Tasks ───────────────→ Cloud Run Jobs API
                  ↓ OIDC                           ↑
             View更新                       Cloud Scheduler
                                                   ↓
                                      Cloud Run Job → Google Health API
                                                   ↓
                                          GCS HEAD CAS
                                                   ↓
                                      Notion / steps.svg
```

- Python 3.13、FastAPI、Pydantic v2、uv
- 公開入口のFirebase Hosting default site 1個（完全一致の7 routeだけをCloud Runへrewrite）
- Cloud Run Service 1個、Cloud Run Job 1個
- Cloud Tasks Queue 1個、Cloud Scheduler 1個
- GCSを正式な永続基盤として使用
- Terraform bootstrapとmainを分離
- Ruff、mypy strict、pytest、Hypothesis、TFLint、Trivy、Gitleaks、Hadolint

## ローカル開発

必要なもの：

- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- Docker
- Terraform 1.10以上
- Google Cloud CLI

```bash
uv sync --frozen --all-groups
make check
make docker-test
make docker-runtime
```

`uv.lock`は依存関係変更時だけ`make lock`で更新し、レビューしてコミットします。Dockerの`test`と`runtime` stageはCIとCloud Buildで個別にbuildされます。ローカルサーバーは必要な場合だけ明示的に起動してください。

## Google Cloudの初期構築

### 0. CLIとTerraformの認証

Terraformより先にCLIとApplication Default Credentialsを設定します。

```bash
gcloud auth login
gcloud config set project <PROJECT_ID>
gcloud auth application-default login
gcloud auth application-default set-quota-project <PROJECT_ID>
```

### 1. Terraform state bucket

```bash
cp infra/bootstrap/terraform.tfvars.example infra/bootstrap/terraform.tfvars
terraform -chdir=infra/bootstrap init
terraform -chdir=infra/bootstrap plan
terraform -chdir=infra/bootstrap apply
```

`state_bucket_name`は世界で一意なprivate bucket名に変更します。bootstrapはstate bucketだけを管理し、通常構成とは別のstateを使います。

### 2. 基盤だけを作成

Artifact Registryへ最初のimageをpushする前はCloud Runを作れないため、初回は`deploy_workloads=false`で適用します。

```bash
cp infra/main/terraform.tfvars.example infra/main/terraform.tfvars
export TF_STATE_BUCKET="$(
  terraform -chdir=infra/bootstrap output -raw state_bucket_name
)"
terraform -chdir=infra/main init \
  -backend-config="bucket=$TF_STATE_BUCKET"
terraform -chdir=infra/main plan
terraform -chdir=infra/main apply
```

この段階でAPI、private health bucket、7日で自動削除される専用Cloud Build source bucket、Secret Managerの空container、Artifact Registry、IAM、Queue、CI用Workload Identity Federationを作成します。Terraformはsecret値をstateへ保存しません。

### 3. Google OAuthとNotionを用意

ここだけは各サービスの管理画面で所有者本人が操作します。

#### Google OAuth

1. Terraformを適用したものと同じprojectを選び、[Google Health APIのOAuth設定](https://developers.google.com/health/setup)からOAuth clientを`Web application`として作成する。
2. 初回imageのdeploy前でFirebase Hosting siteがまだない場合は、Authorized redirect URIへ一時的に`https://www.google.com`を設定する。client作成後、credentials JSONをダウンロードする。
3. Google Auth Platformの`Data Access`で次のscopeだけを追加する。

   ```text
   openid
   https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly
   https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly
   https://www.googleapis.com/auth/googlehealth.sleep.readonly
   https://www.googleapis.com/auth/googlehealth.settings.readonly
   ```

4. `Audience`を`External`にする。所有者のGoogleアカウントをtest userへ追加してから`Publish app`を実行し、`Publishing status`が`In production`になったことを確認する。`Testing`のまま発行されたrefresh tokenは7日で失効する。
5. Restricted scopeについてGoogle Cloud ConsoleがOAuth verificationまたはsecurity assessmentを要求した場合は、表示された要件を完了する。要求を回避するためにscopeを減らしたり、`Testing`へ戻したりしない。

実際のOAuth同意はFirebase Hostingのredirect URIを登録してから行います。redirect URIの変更でclient IDとclient secretは変わらないため、credentials JSONを再登録する必要はありません。

#### Notion

1. Notionでinternal integrationを1つ作り、integration tokenをコピーする。
2. 健康データ専用の非公開databaseを1つ作る。最初はtitleプロパティが1つだけあればよい。
3. databaseの`Connections`から作成したintegrationを接続する。親pageを共有しただけでなく、対象data sourceをintegrationから読書きできることを確認する。
4. `Manage data sources`の`Copy data source ID`でIDを取得する。page IDやdatabase IDではなく、data source IDを使用する。
5. IDを`infra/main/terraform.tfvars`の`notion_data_source_id`へ設定する。残りのpropertyはdeploy後に`make notion-configure`が作成する。

### 4. Secret値を5件登録

Terraformは6個のSecret containerを作ります。この時点でversionを登録するのは次の5件だけです。`google-oauth-refresh-token`はOAuth完了時にアプリが最初のversionを追加するため、空versionを作成しません。

最初にprojectを固定します。project IDは秘密値ではありません。

```bash
printf 'Google Cloud project ID: ' >&2
IFS= read -r PROJECT_ID
export PROJECT_ID
gcloud config set project "$PROJECT_ID"
```

ダウンロード済みOAuth credentials JSONを登録します。ファイルの内容をコマンド引数やシェル変数へ入れません。

```bash
printf 'Downloaded OAuth credentials JSON path: ' >&2
IFS= read -r OAUTH_CLIENT_JSON
test -f "$OAUTH_CLIENT_JSON"
gcloud secrets versions add health-data-pipeline-google-oauth-client \
  --project="$PROJECT_ID" \
  --data-file="$OAUTH_CLIENT_JSON"
unset OAUTH_CLIENT_JSON
```

有効なSecret versionを後段で確認したら、ダウンロードしたcredentials JSONはFinderでゴミ箱へ移します。誤ったファイルを消さないよう、自動削除コマンドには含めません。

Notion tokenは非表示の対話入力から直接標準入力へ渡します。

```bash
python3 - <<'PY' |
import getpass
import sys
sys.stdout.write(getpass.getpass("Notion integration token: "))
PY
  gcloud secrets versions add health-data-pipeline-notion-token \
    --project="$PROJECT_ID" \
    --data-file=-
```

残り3件は端末上で生成して直接登録します。2つの32 byte keyは必ず別々に生成します。Webhook secretはGoogle Healthがそのまま`Authorization` headerへ設定できるよう、`Bearer `を含む完全なheader値です。

```bash
openssl rand -base64 32 |
  gcloud secrets versions add health-data-pipeline-application-hmac-key \
    --project="$PROJECT_ID" \
    --data-file=-

{ printf 'Bearer '; openssl rand -hex 32; } |
  gcloud secrets versions add health-data-pipeline-webhook-authorization \
    --project="$PROJECT_ID" \
    --data-file=-

openssl rand -base64 32 |
  gcloud secrets versions add health-data-pipeline-oauth-session-aead-key \
    --project="$PROJECT_ID" \
    --data-file=-
```

値を読み出さず、5 containerに有効なversionがあることだけ確認します。

```bash
for SECRET_ID in \
  health-data-pipeline-google-oauth-client \
  health-data-pipeline-notion-token \
  health-data-pipeline-application-hmac-key \
  health-data-pipeline-webhook-authorization \
  health-data-pipeline-oauth-session-aead-key
do
  gcloud secrets versions list "$SECRET_ID" \
    --project="$PROJECT_ID" \
    --filter='state=ENABLED' \
    --limit=1 \
    --format='value(name,state)'
done
```

これらの手順は秘密値をシェル履歴へ残さず、新しい平文secretファイルも作りません。端末の画面共有やsession recordingは無効にして実行してください。

### 5. CI/CD

Terraform outputをGitHub ActionsのRepository Variablesへ設定します。

| GitHub variable | Terraform output / value |
|---|---|
| `GCP_PROJECT_ID` | project ID |
| `GCP_REGION` | `region` |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `github_workload_identity_provider` |
| `GCP_DEPLOY_SERVICE_ACCOUNT` | `github_actions_service_account` |
| `CLOUD_BUILD_SERVICE_ACCOUNT` | `cloud_build_service_account` |
| `TF_STATE_BUCKET` | state bucket名 |
| `HEALTH_BUCKET_NAME` | health bucket名 |
| `NOTION_DATA_SOURCE_ID` | 前節で取得したNotion data sourceのID |
| `NOTIFICATION_EMAIL` | 通知先 |
| `BILLING_ACCOUNT_ID` | 任意 |
| `BILLING_CURRENCY_CODE` | 請求先アカウントの通貨（例: `JPY`） |
| `MONTHLY_BUDGET_AMOUNT` | 通貨単位の月次警告予算（例: `1000`） |
| `REPOSITORY_ID` | GitHub repositoryの変更されない数値ID |
| `SCHEDULER_PAUSED` | OAuth完了前は`true`、完了後は`false` |

GitHubの`production` Environmentに`main`だけのdeployment branch ruleを設定します。プランが対応している場合はrequired reviewerも追加します。WIF側も`main`以外のtoken交換を拒否します。`Deploy` workflowを`main`から手動実行すると、Terraformで先に作成した専用source bucketへarchiveを置き、Cloud Buildがtest/runtime build、scan、push、Terraform plan/applyを行います。GitHub Actionsの権限はこのbucketの参照と新規object作成だけに限定されます。初回deploy以降は`deploy_workloads=true`になり、Cloud RunとSchedulerが作成されます。

BudgetをCDで管理する場合は、Cloud Build service accountへ請求先アカウント上の`roles/billing.costsManager`を一度付与します。SchedulerはOAuth refresh tokenが作成されるまで停止し、初回連携完了後に`SCHEDULER_PAUSED=false`として再deployします。

## 初回連携

インフラとCloud Runの初回deploy後に、ローカル端末でApplication Default Credentialsを一度設定します。

```bash
if [ -z "${PROJECT_ID:-}" ]; then
  printf 'Google Cloud project ID: ' >&2
  IFS= read -r PROJECT_ID
  export PROJECT_ID
fi
gcloud config set project "$PROJECT_ID"
gcloud auth application-default login
gcloud auth application-default set-quota-project "$PROJECT_ID"

export REGION="$(terraform -chdir=infra/main output -raw region)"
export HEALTH_BUCKET="$(terraform -chdir=infra/main output -raw health_bucket_name)"
export PUBLIC_BASE_URL="$(terraform -chdir=infra/main output -raw public_base_url)"
export CLOUD_RUN_SERVICE_URL="$(terraform -chdir=infra/main output -raw cloud_run_service_url)"
printf 'Notion data source ID: ' >&2
IFS= read -r NOTION_DATA_SOURCE_ID
export NOTION_DATA_SOURCE_ID

curl --fail --silent --show-error "$PUBLIC_BASE_URL/healthz"
```

`PUBLIC_BASE_URL`は`https://<PROJECT_ID>.web.app`です。Webhook、OAuth、GitHub SVGには
このURLだけを使用し、Cloud Runの`run.app` URLは公開URLとして登録しません。

project Ownerとして初期構築している場合、以下の追加IAM設定は不要です。権限を分離する場合、コマンド実行者には必要なものだけを付与します。

| コマンド | 必要な権限の目安 |
|---|---|
| `make setup-code` | health bucketへの`roles/storage.objectAdmin` |
| `make notion-configure` | projectの`roles/secretmanager.secretAccessor` |
| `make subscriber-upsert` | `roles/health.editor`、`roles/secretmanager.secretAccessor`、project情報取得用`roles/browser` |

ADCのquota project設定には`serviceusage.services.use`も必要です。権限不足の場合はproject管理者に`roles/serviceusage.serviceUsageConsumer`を依頼します。

続けて、必ず次の順番で実行します。

1. Web application OAuth clientのAuthorized redirect URIへ、次のコマンドが表示するURIを完全一致で登録する。登録後、一時的な`https://www.google.com`は削除する。

   ```bash
   printf '%s/setup/oauth/callback\n' "$PUBLIC_BASE_URL"
   ```

   Google Auth Platformが`External / In production`で、前節の5 scopeがすべて登録済みであり、要求されたverificationが完了していることも再確認する。

2. Notionで対象data sourceがintegrationへ接続済みであることを確認し、`make notion-configure`を実行する。これは何度実行しても安全で、専用data sourceを次のschemaへ揃える。

   ```bash
   make notion-configure
   ```

   | Property | Notion type |
   |---|---|
   | `Date` | Title |
   | `Source HEAD generation` | Number |
   | `Content SHA-256` | Rich text |
   | `Data status` | Select |
   | `Fitbit last sync time` | Date |

   `Data status`の選択肢は`PROVISIONAL`、`CANONICAL_SYNCED`、`CANONICAL_STALE`、`MISSING`、`MIXED`、`INSUFFICIENT_DATA`です。

3. OAuth同意より先に`make subscriber-upsert`を実行する。これはdeploy済みHTTPS endpointをGoogle Healthの2段階verificationへ通し、`health-data-pipeline` Subscriberを作成または更新する。全対応data typeは`AUTOMATIC`なので、個別subscriptionの作成は不要。

   ```bash
   make subscriber-upsert
   ```

   成功時に`Subscriber ready: projects/<PROJECT_NUMBER>/subscribers/health-data-pipeline`と表示される。endpoint未deploy、Webhook secret未登録、または`roles/health.editor`相当の権限がない場合はここで失敗するため、OAuthへ進む前に解消する。

4. `make setup-code`で10分有効・1回限りのcodeを発行し、表示されたcodeをブラウザで`$PUBLIC_BASE_URL/setup`へ入力する。Fitbit Sense 2を利用しているGoogleアカウントでログインし、表示されたGoogle Health権限をすべて許可する。別アカウントを使うと所有者照合で拒否される。

   ```bash
   make setup-code
   printf '%s/setup\n' "$PUBLIC_BASE_URL"
   ```

5. OAuth完了後、アプリがrefresh tokenの最初のversionを作成したことを確認する。値自体は表示しない。

   ```bash
   gcloud secrets versions list \
     health-data-pipeline-google-oauth-refresh-token \
     --project="$PROJECT_ID" \
     --filter='state=ENABLED' \
     --limit=1 \
     --format='value(name,state)'
   ```

6. GitHubプロフィールREADMEへ`$PUBLIC_BASE_URL/public/steps.svg`を追加する。
7. ChatGPTからNotion connectorを接続し、定期分析タスクを作成する。

`notion-configure`と`subscriber-upsert`はSecret Managerの値を読み、後者はGoogle Health APIとCloud Resource Manager APIを呼びます。対応data type、Webhook URL、認証secretを変更した場合は`make subscriber-upsert`を再実行します。

setup code、OAuth callback query、token、健康値、Webhook本文はログへ出しません。
callbackのCloud Run request logと、将来Firebase Hostingのログ連携を有効にした場合の
web request logは、Terraformの`_Default` exclusionで保存対象から外します。

## 主要な実行契約

- Service: `health-data-pipeline-service`
- Job: `health-data-pipeline-job`
- Public base URL: `https://<PROJECT_ID>.web.app`
- Webhook: `POST /webhooks/google-health`
- View更新: `POST /internal/tasks/apply-views`
- 歩数SVG: `GET /public/steps.svg`
- Health check: `GET /healthz`

Firebase HostingからCloud Runへrewriteするのは次の7 routeだけです。

```text
GET  /healthz
GET  /public/steps.svg
GET  /setup
POST /setup/session
GET  /setup/oauth/start
GET  /setup/oauth/callback
POST /webhooks/google-health
```

Firebase Hostingの動的rewriteは60秒でtimeoutするため、これらのhandlerは60秒未満で
応答します。HostingがCloud Runへ転送するcookieは`__session`だけです。アプリは
`GET /setup`ではこのcookieをCSRF tokenとして発行し、`POST /setup/session`成功時に
不透明なOAuth session IDへ上書きします。cookie名を分けたり変更したりしません。

Firebase HostingからのrewriteにはCloud Runの未認証呼出し許可が必要なため、Serviceは
公式推奨の`invoker_iam_disabled = true`を設定します。Cloud Run IAMはpath単位で制御できないため、
`POST /internal/tasks/apply-views`はHostingへ公開せず、Cloud TasksのGoogle署名付きOIDC
token、audience、service accountをアプリ側で必ず検証します。その他の未定義routeは
404とし、Webhook、setup session、OAuth stateにも各route固有の検証を適用します。

Cloud TasksとSchedulerはCloud Run Admin APIの次のendpointをOAuth tokenで直接呼びます。

```text
POST https://run.googleapis.com/v2/projects/<PROJECT>/locations/us-central1/jobs/health-data-pipeline-sync:run
```

WebhookのJob起動Taskは、GCS保存完了後の時刻から求めた次の10分境界まで
遅延実行します。同一bucketの通知は同じ明示Task名へ合流するため、後続通知を
取りこぼさず、通常は10分ごとに1回だけJobを起動します。Job終了時にもHEADと
GCSを再照合し、未処理イベントがあれば次の境界Taskへ回収します。

Jobs APIの成功はExecution作成成功であり、HEAD commit成功ではありません。
実行失敗やCAS競合後もGCSに未処理イベントを残し、次回Taskまたは毎時Jobが
回収します。

## データと安全性

- `state/data-head.json`のGCS generation付きCAS更新だけがcommit pointです。
- prefix列挙や最大generation探索では現行データを決めません。
- API原本とnormalized cacheは期限付きの非権威cacheです。`SAMPLE`と`INTERVAL`は90日、
  `SESSION`と`DAILY`は365日で削除します。
- HEADから到達する日次revisionは正式な長期状態です。
- CAS失敗時は同じJob内で再試行せず、objectも即時削除しません。
- NotionとSVGは適用済みHEAD generationを比較し、古い値へ逆行しません。
- BigQuery API、dataset、table、client dependencyは含みません。
- バックフィルは現在同期を常に優先し、空き実行で1日1 data type・14 civil日だけ進めます。
- 高頻度詳細は直近90日、その他は`2009-01-01`までを上限とし、成功したHEADでだけcursorを進めます。
- Class A月4,000回は強い警告として扱い、現在同期やバックフィルの機械的な停止条件にはしません。
- 高容量のraw cacheは30〜365日のGCS lifecycleで削除し、小容量のcanonical／Snapshot／Shardはv1では自動削除しません。健康データbucketが1 GiBを超えたら見直し通知を送ります。

日次long rowの期間は`period_kind`で区別します。

- `DAY`: `period_start = period_end = metric_date`
- `CALENDAR_WEEK`: 対象週の月曜日から対象日までを含む週初来値。日曜日rowがその週の確定値
- `CALENDAR_MONTH`: 対象月の月初から対象日までを含む月初来値。月末rowがその月の確定値

一意keyは`metric_name + statistic + period_kind + dimension_key`です。旧`window_days`や固定日数の月間rollingは使用しません。

## License

Private use only. No license is granted.
