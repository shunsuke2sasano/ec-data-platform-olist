# EC Data Platform (Olist) - Portfolio

## 概要
Olist（公開ECデータ）のCSVをGCSに配置し、BigQuery上に raw / clean / mart / ops のレイヤを構築するポートフォリオです。
本リポジトリはまず **ingestion（GCS → external table → raw table）** を実装しています。

## アーキテクチャ
GCS（raw CSV）→ BigQuery external table → BigQuery raw table → clean → mart → ops（監視）

## データソース
- Olist Brazilian E-Commerce Public Dataset（CSV）

## 実装済み（このリポジトリの現状）
- GCSにCSVを配置（dt=YYYY-MM-DD 配下）
- BigQuery external table（orders_external）を作成
- raw_olist.orders を作成（PARTITION BY ingest_date）
- external → raw へロード（ingest_date, loaded_at, source_file のメタ列付与）

## ディレクトリ構成
- `sql/10_external/` : external table 作成SQL
- `sql/20_raw/`      : raw table 作成・ロードSQL

## 実行手順
1. GCSに配置  
   `gs://ec-data-platform-olist/raw/orders/dt=2026-03-05/*.csv`
2. external table 作成  
   `sql/10_external/10_external_orders.sql`
3. raw table 作成  
   `sql/20_raw/20_raw_orders_ddl.sql`
4. rawへロード  
   `sql/20_raw/21_raw_orders_insert.sql`

## 今後の予定
- clean層：型変換、欠損・重複処理、purchase_dateでパーティション
- mart層：日次売上/注文数などKPI集計
- ops層：品質メトリクス記録と異常検知、日次自動実行