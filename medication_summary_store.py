import sqlite3

conn = sqlite3.connect("receipts.db")
cursor = conn.cursor()

print("📋 セルフメディ対象合計（2025年）")
cursor.execute("""
SELECT SUM(self_med_amount)
FROM purchases
WHERE date BETWEEN '20250101' AND '20251231'
  AND self_med_amount > 0
""")
total = cursor.fetchone()[0] or 0
print(f"▶ 合計：¥{total:,}")
print()

print("🏪 店舗別セルフメディ対象金額（2025年）")
cursor.execute("""
SELECT store, SUM(self_med_amount) AS total
FROM purchases
WHERE date BETWEEN '20250101' AND '20251231'
  AND self_med_amount > 0
GROUP BY store
ORDER BY total DESC
""")

rows = cursor.fetchall()
if not rows:
    print("対象データなし")
else:
    for store, amount in rows:
        print(f"{store}  ¥{amount:,}")

conn.close()