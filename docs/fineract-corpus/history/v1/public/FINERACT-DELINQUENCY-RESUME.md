# 融資の再開日を休止日数から除く

ID案: `FINERACT-DELINQUENCY-RESUME`。

新しい業務要求として、RESUME日Rを「計算を再開する初日」とする。PAUSE開始S、予定終了Eに対し有効休止日はS以上・min(E,R−1)以下。RESUMEなしの予定終了Eは従来どおり含む。S=Rなら有効な休止日は0日で、集約されたpause-period表示から空区間を除くが、PAUSE/RESUMEの監査履歴は保持する。

R当日のAPI pause状態、COBでの融資/分割払いの延滞計算、休止日数の解釈が一致すること。現在のbusiness dateを使い、system dateへすり替えない。API入口（loan id/external id）、受理される日付/検証規則、取引/返済/監査の保存を意図した変更以外で崩さない。

これは文書の一例に合わせた「既存バグ修正」とは呼ばない。元文書は本文・DB説明で終端inclusive、例ではresume前日までとなり、baselineのhelperはresume日をendDateにし、公開統合テストも当日activeを期待する。今回の要求が優先するという明示裁定である。2026年文書を2023年当時の仕様として扱わない。

公開要求→元資料→判定の対応: pause-delinquency、business-dateの現行性注記、COB/イベント/永続化の関連資料、既存16メソッドのDelinquencyActionIntegrationTests。16件は存在数で、実行合格の証拠ではない。
