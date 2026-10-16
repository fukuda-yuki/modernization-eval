# 融資の再開日を休止日数から除く

要求ID: `FINERACT-DELINQUENCY-RESUME`。

新しい業務要求として、RESUME日Rを「計算を再開する初日」とする。PAUSE開始S、予定終了Eに対し有効休止日はS以上・min(E,R−1)以下。RESUMEなしの予定終了Eは従来どおり含む。S=Rなら有効な休止日は0日で、集約されたpause-period表示から空区間を除くが、PAUSE/RESUMEの監査履歴は保持する。

R当日のAPI pause状態、COBでの融資/分割払いの延滞計算、休止日数の解釈が一致すること。現在のbusiness dateを使い、system dateへすり替えない。API入口（loan id/external id）、受理される日付/検証規則、取引/返済/監査の保存を意図した変更以外で崩さない。

新しい業務仕様として適用し、既存資料や従来挙動と異なる箇所では本要求を優先する。
