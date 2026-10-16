# 公開入力だけによる資料の再封印 — revision 2

本版は Max の3点の資料修正指摘に対する独立作業であり、Max の確認待ち。
実モデル送信・gold作成・非公開評価の変更・main mergeは行っていない。

入力は委任文、AGENTS.md、公開 [Issue #36](https://github.com/fukuda-yuki/modernization-eval/issues/36)、
公開 [Issue #37](https://github.com/fukuda-yuki/modernization-eval/issues/37)、公開producerとその資料、
公式codeloadから独立scratchへ取得した固定Apache Fineract sourceだけ。
archive SHA256は `ee6fed2a9c5574d2dbbc19403704db25c341e89db190f6cb99c258d71518727a`。
親から旧封印後の私有評価草案の存在は知らされたが、草案・隠し期待値・gold・モデル結果、
親の artifacts/・runs/・tmp/・work/ は閲覧していない。実装位置や正解パッチから資料を選んでいない。

## 変更と根拠

1. `roles()`はfeature全275節へ同じ3分類を付けていた。原文の節内容と見出しから、
   API契約、検証、データ構造、会計、計算規則、業務例、設定等の16役割へ変更した。
   [節別台帳](section-roles.md)と `section-roles.json` に本文anchor・原文hashを記録する。
   タイトル・属性・来歴を扱う29節はdocument-metadata、feature indexはnavigation。
   savingsの計算説明＋例など複数役割も区別する。これは主用途の注釈であり、
   全文の意味解釈や全段落への網羅的注釈を主張するものではない。
2. `charges-detail.md`のsavings-deposits所属は貯蓄手数料についての所属であり、
   利息計算への直接関連ではなかった。広い所属 `feature_ids` と主題
   `selection_feature_ids` を分離した。手数料資料はSavings課題ではrest、
   savings-detailはtarget、横断の業務日・試験手順はcommonとする。
   小中大は同じ8/32/128 KiB・完全文書節prefix規則を維持し、飛ばし読みしない。
3. 3課題の最終要求からクラス名、試験名、資料の探索先、研究者への指示を除き、
   新仕様・維持仕様・対象外の業務契約を残した。要求hashを更新し、全条件へ同一本文を配る。
   公開試験説明は既存の境界計算9メソッド、貯蓄設定20組、定期預金設定14正常＋6不正組、
   通常daily APIの観測内容と限界を区別した。これらはソース上の数で、試験合格数ではない。

選択規則・資料内容の確定時刻は2026-10-16 18:37:52 UTC。
本文量を望ましい結果へ合わせるための再順序付けや予算変更は行っていない。
索引は原見出しと参照のままで、本文解説を追加していない。
全体索引量は158,136から97,308 bytesへ変わったため、旧版と同じ介入とは扱わない。

## 封印と保存

| 対象 | revision 1 | revision 2 |
| --- | --- | --- |
| selection-seal.json SHA256 | `be0274f3ac642f9f4f1463e26870e2769af37919ac5f97470cce6bd180e8553f` | `130fbd2a49f14a6c89bf2636f5a1f593d7d3e749d286c02c67a2a457db951634` |
| semantic corpus SHA256 | `7b0c0f848999b2d7a53b86af8163ae37cb7d38ba997f538d752e954bdea3658f` | `00eb6ec1fa1384ab535c0da5910559f02261ef154c3ea086c20816963e97acaf` |

source inventory SHA256は両版で
`e3d23465c917d9e9f10b3b9a08f3be701269e7247af71b84d74b9a0fad9079d6`。
7,772の原本ファイルとlicense/noticeを変更していない。
新版sealはsemantic bytes、選択順位、用量集合に加え、節metadataと索引hashも固定する。
`propose-seal`は新しい候補ファイルだけを作り、既存sealや既存出力を上書きしない。

旧seal・policy・要求・評価公開契約・集合表・validationは [history/v1](history/v1/README.md)
へ複製保存した。旧producerの復元commitは `69f22d90a624f7be0f0521fa2a3646155404c50f`。
資料は同じ管理パスで新版へ更新したが、旧版の復元経路を保持している。

## 検証

| 課題 | small 節数 / bytes | medium 節数 / bytes | large 節数 / bytes |
| --- | --- | --- | --- |
| Delinquency | 5 / 7,820 | 16 / 31,359 | 45 / 118,559 |
| Savings | 2 / 6,607 | 20 / 30,926 | 76 / 127,071 |
| Charges | 5 / 7,510 | 32 / 32,656 | 104 / 127,654 |

Savings smallの実内容はsavings-detail 6,579 bytesと原文タイトル28 bytes。
次のOverview 1,648 bytesは残予算へ収まらないため、規則どおり停止する。
「target」は関連性の分類であり、機能網羅や要求の全条件充足を意味しない。

独立Linux volumeに公開Git archiveと100644の作業差分を展開し、ネットワークを無効にして検証した。
Windows bind mountの実行bitをsemantic inventoryへ混入させていない。
18件のproducerテストが全件成功、skipは0。2回のoffline生成でmanifest/promptを含む全ファイルが一致。
3課題×7条件の要求/source/semantic/evaluation hash一致と、両整理水準の本文hash一致を確認した。
節役割の根拠、Savingsの手数料誤分類再発、要求への探索先混入、metadata/索引/本文の改竄も検査した。
詳細は [validation.json](validation.json)、[テストログ](validation-tests.txt)、[集合表](condition-sets.md)。

Fineract Java試験の実行結果やモデル受入の成功はこの検証から主張しない。
公開最終要求の同期、Maxの資料確認、実行評価とモデルadmissionは親側に残る。
