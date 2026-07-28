<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# InfluxDBコネクタ

InfluxDB用のネイティブIrodoriテーブルコネクタ拡張機能。

このクレートは、コネクタのメタデータ、ネイティブABIエクスポート、およびIrodori拡張マーケットプレイスで使用されるドライバ実装をパッケージ化しています。

## コネクタ

- 拡張ID: `irodori.influxdb`
- エンジンID: `influxdb`
- ワイヤープロトコル: `influxdb`
- デフォルトポート: `8086`
- ネイティブABI: `irodori.connector.native.v1`
- ドライバ連携: `はい`
- マーケットプレイスの表示: `公開`
- パッケージバージョン: `0.1.3`

このパッケージには、`db/influx.rs`からのデスクトップアダプタのソーススナップショットが含まれています。

コネクタのメタデータは`connector.config.json`と`irodori.extension.json`に格納されています。
Rustクレートは`src/lib.rs`からネイティブABIをエクスポートし、`irodori-connector-abi`を共有JSON/バッファヘルパーとして使用し、コネクタの動作は`src/driver.rs`に保持しています。

## 接続メタデータ

- エンドポイントモード: `hostPort`, `connectionString`
- トランスポートモード: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS対応: `はい`
- TLS必須（デフォルト）: `いいえ`
- カスタムドライバオプション: `はい`

### エンドポイントフィールド

| フィールド | ラベル | 型 | 必須 |
| --- | --- | --- | --- |
| `host` | ホスト | `string` | はい |
| `port` | ポート | `number` | いいえ |
| `database` | 組織またはバケット | `string` | いいえ |

## 認証

コネクタはこれらの認証モードを公開しており、クライアントは適切な資格情報フィールドをレンダリングできます。必要に応じて、ドライバ固有またはプロバイダ固有の値は`options`を通じて渡すことも可能です。

| 認証方法 | ラベル | 種類 | シークレットの用途 |
| --- | --- | --- | --- |
| `none` | 認証なし | `none` | なし |
| `connectionString` | 接続文字列 / DSN | `connectionString` | なし |
| `userPassword` | ユーザー/パスワード | `userPassword` | `password` |
| `bearerToken` | ベアラートークン | `token` | `token` |
| `apiKey` | APIキー | `apiKey` | `token` |
| `oauthAccessToken` | OAuth 2.0アクセストークン | `token` | `token` |
| `clientCertificate` | クライアント証明書 / mTLS | `certificate` | `privateKey`, `privateKeyPassphrase` |
| `customDriverOptions` | カスタムドライバオプション | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## エクスペリエンスメタデータ

- ドメイン: `timeSeries`
- 結果ビュー: `timeChart`, `table`, `heatmap`
- オブジェクトタイプ: `buckets`, `measurements`, `fields`, `tags`, `retentionPolicies`, `tasks`
- インスパイア元: InfluxDB Data Explorer、Flux aggregateWindow、InfluxDBタスク

| ワークフロー | 結果ビュー | テンプレート |
| --- | --- | --- |
| 時間範囲クエリ | `timeChart` | `time-influx-aggregate-window` |
| ダウンサンプリングウィンドウ | `timeChart` | `time-influx-aggregate-window` |
| 最新値 | `table` | `time-influx-latest` |

| テンプレート | ラベル | 言語 | 結果ビュー |
| --- | --- | --- | --- |
| `time-influx-aggregate-window` | 集約ウィンドウ | `flux` | `timeChart` |
| `time-influx-latest` | 最新値 | `flux` | `table` |

## ネイティブABI呼び出し

| メソッド | 応答 |
| --- | --- |
| `health` | コネクタのヘルス状態、エンジンID、ABIバージョン、ドライバの状態を返します。 |
| `describe` | 埋め込みマニフェストとコネクタ設定を返します。 |
| `manifest` | 生の`irodori.extension.json`を返します。 |
| `config` | 生の`connector.config.json`を返します。 |
| `connect` | ネイティブコネクタ接続を開き、検証します。 |
| `query` | コネクタクエリを実行し、構造化された行またはJSON結果を返します。 |
| `metadata` | スキーマ、テーブル、列、インデックス、コレクション、または同等のメタデータを読み取ります。 |
| `close` | キャッシュされたネイティブ接続を閉じて削除します。 |

## 開発

このチェックアウト内のすべての拡張クレートは`../target`を共有しており、依存関係は兄弟リポジトリ間で一度だけコンパイルされます。

```sh
make check
make build
```

リリースパッケージは、プラットフォーム固有のネイティブアーティファクトを`dist/native`に配置します。

## ライセンス

0BSD。このプロジェクトはほぼすべての目的で使用、コピー、修正、配布できます。