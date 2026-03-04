import sqlite3

# SQLiteに接続
conn = sqlite3.connect("receipts.db")
cursor = conn.cursor()

print("📋 セルフメディケーション対象商品の合計金額（2025年）")
cursor.execute('''
    SELECT SUM(self_med_amount)
    FROM purchases
    WHERE date BETWEEN '20250101' AND '20251231'
      AND self_med_amount > 0
''')
total = cursor.fetchone()[0] or 0
print(f"▶ 合計金額：¥{total:,}")
print()

print("📊 商品別セルフメディケーション対象金額ランキング（2025年）")
cursor.execute('''
    SELECT item, SUM(self_med_amount) AS total
    FROM purchases
    WHERE date BETWEEN '20250101' AND '20251231'
      AND self_med_amount > 0
    GROUP BY item
    ORDER BY total DESC
''')

rows = cursor.fetchall()
if not rows:
    print("（対象商品はありません）")
else:
    print("商品名\t\t金額")
    print("-" * 30)
    for item, amount in rows:
        print(f"{item[:14]:<14} ¥{amount:,}")

conn.close()

