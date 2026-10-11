# 固定 OpenCode の追加入力経路と検証範囲

単一条件 `staged-explore` に、初回から通算の時間枠内で追加文を1回だけ渡す経路を追加する。
同じ Run UUID・workspace・native session を使い、元sourceとDBへのアクセスを保持する。
実Docker上の人工provider統合は確認済み。実モデルでのlive受入はこの実装検証では実施していない。
有効な実行指示と実測pin・凍結計画・受入結果は別々に扱う。
要求分割・対象file範囲・件数・予算・再試行回数を値入りの研究計画として作成していない。
[限定評価器修正](limited-evaluator-review.md)は別の変更で、既存保存物の原判定は変更しない。
新しい追加ゼロ対照、共通の再開後観測、混在割付は
[observed presentation](observed-presentation.md)で別契約として明示する。
以下のv1の履歴と証跡を新契約へ遡及変更しない。

## 固定版で確認できた経路

OpenCode `1.17.11` の実バイナリとループバックの人工providerで確認した。
バイナリのSHA256は
`0254a429cd0e6cf0ba53fc01672cf98e4a8dc728f7fa94be88f1e4b3645e6ded`。
試験は一時workspace・独立したconfig/stateを作り、終了後に削除する。
実モデル、provider credential、保存済み研究Runは使用しない。既存runtime lockや取得版は更新しない。

```text
python outer/verify/probe-staged-session.py --opencode /absolute/path/to/opencode-1.17.11 --observe-boundary
python outer/verify/probe-staged-server.py --opencode /absolute/path/to/opencode-1.17.11 --gateway-barrier
```

通常の `run` は [run.ts](https://github.com/anomalyco/opencode/blob/v1.17.11/packages/opencode/src/cli/cmd/run.ts)
でstdinを一度読み、非attach時にはprocess内のserverを使う。tool terminalのJSON出力には
次のrequestを停止するACK手順がない。初回CLIの終了後に `--session` で再開できることは確認したが、
それを最初の実装変更tool終了の境界には使わない。通常のCLIを2つ同時起動する方式も使わない。

候補経路は同じ固定版の `serve --pure --hostname 127.0.0.1` と `run --attach --session`。
[session prompt](https://github.com/anomalyco/opencode/blob/v1.17.11/packages/opencode/src/session/prompt.ts)
の `noReply: true` はuser messageの追加後に戻るため、別のmodel loopを起動せず、保留中の
同じsessionへ追加できる。gatewayはprovider SSEの `[DONE]` 行だけを一時保留し、先に届いた
固定版 `write` toolが終了することを実バイナリで確認した。その保留中にnative messageを追加し、
ACK後に元の応答末尾を開放すると、同じloopの次のrequestが追加文を含む。

実gatewayを使った人工試験では、native session 1つ、追加user message 1つ、request合計3件。
追加文は初回requestに存在せず、境界直後のrequestに初回履歴とともに存在した。
同じ実装fileが追加後にも変更され、usageは3件の合計36 synthetic tokens、段階証明の不一致は0。
これは有限のtool/API/証跡の能力試験であり、モデル性能や実研究Runの結果ではない。

## 実装契約

`profiles.create` は、hashで固定した元の最終要求を、明示したsection一覧へ過不足なく分割する。
sectionの重複・欠落・順序変更と元要求の変更を拒否する。共通wrapperは初回に残し、source preloadは行わない。
初回promptだけをworkerのinputsへ置き、追加本文は `_controller/staged-input` へ保存する。
controller領域はworkerにマウントせず、初回前にinputs/workspace/stateの本文漏洩を検査する。
追加文はowned containerへの `docker exec` のstdinからnative APIへ渡し、argv・環境変数・初回worker fileには置かない。
gatewayのみcontroller領域をread-onlyで参照し、本文を独立に照合する。

| 証跡 | 記録と制約 |
| --- | --- |
| 予定 | Run UUID、task/condition、初回・追加prompt hash、partition、明示transport/snapshot policy、元の予算、delivery ID |
| 境界 | gateway request ID/hash、provider順の全tool call ID、各native terminal message/session/status、file hash差分、全tool終了、保留barrier ID。新policyは個別toolへの変更帰属を要求しない |
| 投入 | hash連鎖のcontroller ledgerでintent、send-return、native ACKを分離。単一writer、ACK不明・停止・競合で自動再送しない |
| 最初のrequest | gatewayが追加文のuser-role中の一意性とrequest原本hashを照合。最初の観測requestと最初の送信requestは別項目。native ACKだけでprovider到達としない |
| 終端 | 初回からのwall/monotonic deadline、全request usage、未到達/早期終了/追加前失敗/unknown、停止確認、最終snapshotを保持 |

実装fileのscopeは実行計画のsuffix/excluded-directory一覧で固定する。symlinkを拒否し、build合格や
採点合格を条件にしない。toolが失敗していても実装fileが変われば候補となる。read-only toolでは
snapshotと保留解除を記録する。旧policyでは複数toolを含む応答で変更が起きた場合、最初の変更を個別toolへ
帰属できないためunknownとして停止し、都合のよい後続境界へ進めない。未完了toolのeventは元のdeadline内で待つ。tool terminal後にも子processが
残る場合はunknownで停止し、子processの終了を待って後続時点へ境界をずらさない。追加文なしのrequestが投入境界を越える場合も拒否する。

新契約は `boundary_contract.boundary_policy: response_tool_group_pause_v1` を明示する。
初めて実装差分を観測した応答の全tool終了後、次requestを保留したまま、group単位で追加を1回投入する。
複数tool・成功/error混在・残存子processを存在だけで停止しない。個別toolの変更順序は推測しない。
子processはPID、親PID、状態、開始tick、clock tick単位を記録する。
安定したtree観測のため所有workerだけをpauseし、観測後はfinallyでunpauseする。
独立したpause watchdogも5秒または残時間の短い方で解除を試みる。長い複写中に解除された場合も
source→copy→source hash検証を維持し、不整合archiveはnullにする。tree観測がpause内に完了しなければ
同じbarrier内で再観測し、新しいgroupへ進めない。解除未確認・STOP・元のdeadline超過なら追加しない。
旧3項目のboundary contractを暗黙にこのpolicyへ読み替えない。

group terminal観測、native提供時刻またはnull理由、tree観測区間、pauseの理由/開始/解除結果、
送信intent/return、ACK、最初の包含requestを別々に保存する。native時刻は元の単位のまま残し、
観測時刻やgroup終端を実提示時刻へ代入しない。pause・複写・待機も元Runの通算時間に含む。

`runtime` は起動直前にも承認済みcampaign/epoch・実際のdispatch journal・partition/transportを照合する。
直接のRun開始もこの照合を迂回できず、campaign STOP中は拒否する。
既存containerの所有権・mount・Run UUIDを確認してnative APIを操作する。
既存のmodel ID、provider、tool権限、question deny、LSP/compaction設定、入力分離を維持する。
段階間に回収・採点を行わず、最終停止後に既存の回収・採点部品へ戻る。未到達でも初回入力と
usage/停止証拠が完備する終端は、追加到達をfalseのまま保持する。観測障害を到達成功へ変換しない。
私有archiveにはcontroller原本も含める。public公開allowlistは広げない。

## 復元可能な私有checkpoint

初期workspace/input manifest、追加送信前、追加文を含むrequest以降の最初の実装変更後、最終停止後の
4時点を区別する。段階checkpointは最終submissionの固定・回収・採点ではない。
既存のfile選択/DB sidecar規則を使い、正規化せずbyte列・tree manifest・直前checkpointとの差分を
`_controller/staged-input/checkpoint-archive` へ保存する。複写前後のworkspace hashを再照合し、
取得開始/終了時刻・所要時間・byte数・receipt/archive hashをledgerへ結ぶ。
初回のinput manifestと既存Run archiveのinputsを保持し、source/DBを隠さない。
DBのcheckpoint/修復は行わず、非空sidecarや複写中変更を検出した場合はnullと理由を残す。
旧policyでは停止する。新group policyでは境界・同一性・台帳・配送制御が成立していれば継続し、
途中archiveの成功を配送必須条件にしない。証跡の書込み自体が不能なら成功を作らず停止する。

追加後もgatewayはtoolを含む応答の終端を保留し、最初の追加包含request IDに結び付いた変更を
保存してから保留を解除する。その後の上書き・削除にかかわらず前後の内容をprivate archiveから復元できる。
追加後に変更がない、配送に到達しない、並行tool/子processで境界が不明、snapshotが失敗した場合は
追加後archiveをnullと理由で残す。finalを追加後checkpointの代用にしない。
新しいarchiveも私有Run保存の対象であり、Git/public exportへ原本を追加しない。

## 明示計画と停止・再開

[新campaign入口](campaign-initialization.md)で `staged_inputs`（taskごとのpath/hash参照）と
`staged_retry` の両方を必須にする。partitionのkindは `staged_request_partition_v1`、transportは
`opencode-server-response-barrier-v1`。`known_pre_dispatch_retries` は明示した0または1、
旧 `after_dispatch_retries` は0のみ。旧契約は既知の準備失敗receiptとdispatch不存在・usage不存在が揃う場合だけ、
承認された準備retryを許す。送信後・不明・証跡不十分は新UUIDへの取り直しも保留する。
旧2条件の再開形式・retry規則は変更しない。停止・未知sendからの自動session継続は行わない。
新共有枠と技術障害allowlistは[campaign入口](campaign-initialization.md)の `staged_retry` を参照する。
同一messageの再送と、確認済み基盤障害後のfresh UUIDによる別attemptを区別する。

この実行形態は通常の単発CLIと異なる。provider応答の保留時間、controllerのsnapshot時間と
待機時間も初回からの通算予算に含まれる。既存exploreとの比較は運用比較であり、時機だけの
純粋因果効果とは扱えない。この経路を採る明示実行計画と、その計画hashに対する実際の実行指示が必要。
既に与えられた有効な指示は継承し、未検証のpinや受入結果を補わない。

## Docker人工provider受入と残る検証

```text
python outer/verify/probe-staged-docker.py --opencode /absolute/path/to/opencode-1.17.11 --output /private/new-fixture-directory
python outer/verify/probe-staged-docker.py --opencode /absolute/path/to/opencode-1.17.11 --output /private/new-group-fixture-directory --group-boundary --concurrency 4
```

このopt-in probeは固定digestのPython baseと既に取得した固定版binaryを使う有限のfixture imageを作る。
providerはgateway container内のloopbackで定型SSEを返し、実provider credentialを読まない。
人工Runのために置換するのはcampaign承認validatorとgateway stdin secretだけ。
実 `runtime.start`、DockerTransport、read-only/non-root/internal-networkのisolation probe、
container所有権、停止、network回収、gateway、live_usage、private checkpoint/復元を通す。
各caseで元の承認validatorの未承認拒否を検査する。通常テストは直接起動のDocker割当前拒否も検査する。
Docker client設定はfixture専用の空ディレクトリへ分離し、hostのproxy/認証設定を変更・継承しない。

通常の同一file継続、並行write、detached子processを残すbash、および追加後の同じ曖昧ケースを扱う。
曖昧ケースは保留中にunknownで停止し、再送せず、観測usageとfinalを保持する。
新group policyではこれらを全て継続し、前後archiveの復元と次request包含を検証する。
さらに一時的なDB sidecarによる中間archive欠測と、pause中のSTOPを人工的に発生させ、
継続時の1回送信、停止時の無送信、解除・owned cleanup・全usageを確認する。並列probeは実Run区間の最大重複数も記録する。
途中停止した応答はstream未完・観測不完備のままであり、成功usageへ書き換えない。
実Docker試験で検出したevent到着順の競合、非rootで読むgatewayコードのmode、直接起動の承認照合を修正した。

通常テストはcheckpoint復元・上書き/削除・CRLF byte保持・DB sidecar保留・receipt改変拒否、
SSE chunk分割、ACK消失、重複/外来ID/停止、失敗tool、予算非reset、未到達、producer漏洩、
単一条件と旧2条件の回帰を検証する。fixture workerに.NET SDKは含めず、production .NET image全体、
任意のtool動作、Windows Docker、実provider/liveモデルは受入済みとしない。
実モデル送信、課金API、保存物の再採点は行わない。Ready変更は親のレビュー判断に委ねる。
