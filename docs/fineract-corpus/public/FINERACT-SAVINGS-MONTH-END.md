# 貯蓄・定期預金の月末起点周年利息

ID案: `FINERACT-SAVINGS-MONTH-END`。

新しい業務要求として、周年型8/9/10/11で開始日がその月の最終日なら、後続posting dateも各対象月の最終日とする。間隔は既存の1/3/6/12か月。最初の開始日が月末でなければ元のday-of-monthを維持し、短月のみ末日に丸める。短月を通ったために以後が月末型へ変わってはいけない。

貯蓄・定期預金の共通境界計算に適用する。通常型1/4/5/6/7、financialYearBeginningMonthの扱い、手動post-interest-as-onによる期間分割、既存の計算対象日での打切り、重複記帳防止を保持する。新しい型番号/勝手なAPIを追加しない。開始日が2月28/29日や4月30日の場合にbaselineと差があり、1月31日だけでは新要求の検証にならない。

固定SHAのSavingsHelperは開始日の数値を保持してmin(day,month-length)を使う。これを未来の正解パッチとして資料選択へ使わず、現行挙動の証拠とする。公開資料はsavings-interest-posting、商品/API、business date、会計/利息処理、後追いの試験説明。SavingsHelperAnniversaryPostingTest、SavingsAccountAnniversaryValidationTest、FixedDepositProductAnniversaryValidationTestと統合APIの対応を明示する。
