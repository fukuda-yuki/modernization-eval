# Fineract 資料条件 — Issue #36

主表示は **[3課題×7条件の集合表](condition-sets.md)**。初回文書ID、本文量、
選択索引量、全体索引量、関連性の内訳、要求/source/探索可能集合/評価仕様hashを
全条件で表示する。[入力・生成CLI契約](contract.md)は #37–39 のconsumer向け。
本変更は資料条件部分であり、実行・採点・研究全体の完成を意味しない。

## 固定対象と封印

- 基点は modernization-eval `40d0064`。新規ファイルと独立producerのみを追加。
- Apache Fineract 1.15.0、SHA `d5636847ac556c30b437254c353f05526d172b97`。
  原本は [SHA指定の公開archive](https://codeload.github.com/apache/fineract/tar.gz/d5636847ac556c30b437254c353f05526d172b97)。
  圧縮archiveと全ファイルのpath/bytes/hash/実行属性を固定して照合する。
- 資料・tags・選択規則・実際の小中大集合は **`5f89a14`** の
  [selection-seal.json](../../tools/fineract_corpus/selection-seal.json)で封印。
  その後の検証強化は本文、順位、promptを変更していない。生成・検証の両方で封印を照合する。
- 規模十分性の判断は [#36](https://github.com/fukuda-yuki/modernization-eval/issues/36)
  に記録された判断を実装上の前提とし、ここで別の研究設計判断へ置換しない。

7,772ファイルすべて（コード・公開テスト・文書・ビルド定義・license/notice）を
原配置のまま保持する。全34 feature文書を含む。736は原文の節または文書単位数であり、
機能数・独立工程成果物数・独立標本数ではない。15の台帳分類は業務能力および
業務を支える共通能力で、ファイル分割による水増しではない。

[機能対応・不足台帳](feature-ledger.md)と [27件の本文確認根拠](../../tools/fineract_corpus/evidence.json)
で、原資料、代表的コード、公開テストを追跡する。台帳の「本文確認」「ファイル名のみ」
「未確認」を混同しない。元のtesting indexはTBDで、全機能に完全なPT/ST原本があるとは
主張しない。公開テストは存在と意味対応を静的に確認した範囲のみで、未実行。

## 条件の意味

| 初回条件 | 原配置 | 原配置＋全体索引 |
| --- | --- | --- |
| 自律探索 | explore-original | explore-indexed |
| 選択索引のみ | — | selection-indexed |
| 同じ選択索引＋少量 | — | small-indexed |
| 同じ選択索引＋中量 | medium-original | medium-indexed |
| 同じ選択索引＋多量 | — | large-indexed |

量比較は索引ありの5条件、案内比較は自律探索/中量×原配置/全体索引の2×2。
中量は結果によらず代表用量。2種類の索引を区別する。

- **全体索引**: 158,136 UTF-8 bytes。機能・情報役割をキーに原見出しと参照を並べ、
  indexedのworkspaceだけに配置。promptには所在を示し、全索引を自動preloadしない。
  codeやテストのパスは原配置を保つ。概要・正解説明は索引へ追加しない。
- **選択索引**: 当該課題のlargeまでの同一見出し・参照集合。
  selection/small/medium/largeに同一内容を初回提示する。
  medium-originalにも同じ選択索引を渡し、全体の案内操作との交絡を避ける。

初回選択対象はUTF-8の `.adoc` と `.md`。他のコード・画像・JSON仕様・ビルド等は
全体の探索可能集合に残り、選択本文に変換しない。AsciiDocはブロック外の第2階層
見出しで節を区切り、表・コード・preambleを含む全bytesを保持する。Markdownは
文書全体を1単位とする。includeは原文参照のままで展開・外部取得しない。
本文は8/32/128 KiB以内のprefixで、次の節が入らなければ停止し、理由をmanifestに残す。
文書節を飛ばしたり切り詰めたりしない。小中大が厳密に増えなければ生成自体を失敗させる。

選択順位は事前固定の対象機能tags→公開要求の共通契約tags→path/byte順。
タグは公開見出しと業務対応に基づき、gold変更位置を使わない。複数機能にまたがる
後追い資料もあるため、貯蓄と手数料のsmallは同じ資料となる。これは固定規則からの
結果であり、成果物の成功に合わせて個別に調整しない。情報役割は章・資料用途の分類で、
すべての段落がその役割を満たすという品質判定ではない。

関連性・網羅性・重複も量とともに変わる。純粋な長さ効果や散在全般の効果とは呼ばず、
この選択規則による初回量と、全体索引という案内施策の比較として扱う。
bytes/4の切上げはproxy tokenで、model token数やcontext収容保証ではない。
本文予算はprompt全体の予算ではない。consumerは真の入力制約を確認し、切捨てずに
admissionを拒否する必要がある。

## 後追い資料と同一情報集合

[5つの研究者作成資料](../../tools/fineract_corpus/reconstructed)は全条件の探索集合へ
同じbytesで配置する。全体業務概要、延滞の詳細仕様/公開試験説明、預金の詳細仕様/
公開試験説明、手数料の既存許容値対応表、横断PT/ST相当チェックリストを含む。
各文書に固定baseline、本文根拠、未確認事項、作成日、作成資源、非実行を明記した。
当時の工程原本や実行済み試験報告とは呼ばない。

[共通の来歴・正本性表](../../tools/fineract_corpus/authority.md)は全workspaceおよび
全初回promptに同一bytesで入る。business-dateの古い不在記述、pause文書の不整合、
最終公開要求による新しい仕様の優先をここで区別する。索引条件だけに修正情報を渡さない。
semantic corpus hashは全コード/公開文書/公開テスト/後追い資料/共通注記の
path・bytes・hash・実行属性に基づく。案内metadataの量/hashと別に保存する。

## 再生成・検証

```sh
python -m tools.fineract_corpus fetch --output /tmp/fineract-source.tar.gz
python -m tools.fineract_corpus generate \
  --source-archive /tmp/fineract-source.tar.gz \
  --tasks docs/fineract-corpus/public/tasks.json \
  --output /tmp/fineract-materials-new
python -m tools.fineract_corpus verify --bundle /tmp/fineract-materials-new
FINERACT_SOURCE_ARCHIVE=/tmp/fineract-source.tar.gz \
  python -m unittest discover -s tests/fineract_corpus -v
```

検証環境はLinux/POSIXのCPython 3.12。原本の実行属性も固定するため、
WindowsではLinux container/WSLのfilesystemを使用する。native Windows生成は未検証。
生成はoffline、Python標準ライブラリのみ。既存outputを上書きしない。
テストは別ディレクトリへ2回生成し、manifest/promptを含む全ファイルのbyte同一性を確認する。
予算による飛ばし読み、UTF-8/表/コード破損、archiveの改変・path traversal・symlink、
追加ファイル、prompt/manifestの差替え、要求や評価hashの不一致も検査する。
[validation.json](validation.json)は非モデル検証記録。
原コード・公開テストの実行、Java/Gradle/DB依存物の事前取得・固定、実モデル受入は
#37–39の担当であり、このproducerはそれらを完了したと扱わない。

`public/`は [#37](https://github.com/fukuda-yuki/modernization-eval/issues/37) の
公開要求と公開評価契約のsnapshot。`evaluation_spec_kind=public-contract`は公開契約の
hashを固定しているだけで、採点実装が完成した証明ではない。consumerは確定した公開要求と
実際の評価仕様のhashを明示的に入力する。要求が変われば、封印済み資料選択との適合を
研究管理側で確認する。変更要求、評価hash、管理manifestは原文選択の順位決定に使わない。

原文は変更せずLICENSE_SOURCE/NOTICE_SOURCE等も全bytes保持する。初回の抽出箇所は
元path/範囲/hashから復元できる。研究者の追加文はApache-2.0と表示し、原著者の文書と
区別した。生prompt実行ログ、秘密、私有評価器、gold、旧E1/E2/E3資産は本PRに含めない。
