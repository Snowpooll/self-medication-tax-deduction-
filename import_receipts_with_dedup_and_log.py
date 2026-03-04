import sqlite3
import csv
import os
from datetime import datetime
import re

# 設定
csv_folder = './csv_files'
db_file = 'receipts.db'
skipped_log = 'skipped.csv'

# SQLite接続
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# purchasesテーブル（UNIQUE制約付き）
cursor.execute('''
    CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        store TEXT,
        date TEXT,
        item TEXT,
        unit_price INTEGER,
        total_price INTEGER,
        quantity INTEGER,
        self_med_amount INTEGER,
        UNIQUE(date, store, item)
    )
''')

# import_logテーブル
cursor.execute('''
    CREATE TABLE IF NOT EXISTS import_log (
        filename TEXT PRIMARY KEY
    )
''')

# skipped.csv が存在していたら上書き、なければ新規作成
with open(skipped_log, 'w', newline='', encoding='utf-8') as skipfile:
    skip_writer = csv.writer(skipfile)
    skip_writer.writerow(['購入店舗名', '購入年月日', '商品名', '単品税抜価格', '価格', '個数', 'セルフメディケーション対象金額'])

    for filename in os.listdir(csv_folder):
        if not filename.endswith('.csv'):
            continue

        filepath = os.path.join(csv_folder, filename)

        # ファイルの重複チェック
        cursor.execute('SELECT 1 FROM import_log WHERE filename = ?', (filename,))
        if cursor.fetchone():
            print(f"[スキップ] {filename}（すでにインポート済み）")
            continue

        print(f"[処理中] {filename}")
        try:
            with open(filepath, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # ヘッダー行がデータ行に混入していたらスキップ
                    if row['購入年月日'] == '購入年月日':
                        continue

                    
                    store = row['購入店舗名'].strip()
                    date = datetime.strptime(row['購入年月日'], '%Y/%m/%d').strftime('%Y%m%d')
                    item = row['商品名'].strip()
                    unit_price = int(row['単品税抜価格'])
                    total_price = int(row['価格'])
                    quantity = int(row['個数'])
                    self_med_amount = int(row['セルフメディケーション対象金額'])

                    try:
                        cursor.execute('''
                            INSERT INTO purchases
                            (store, date, item, unit_price, total_price, quantity, self_med_amount)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (store, date, item, unit_price, total_price, quantity, self_med_amount))
                    except sqlite3.IntegrityError:
                        # 重複データをskipped.csvに記録
                        skip_writer.writerow([
                            store, row['購入年月日'], item,
                            unit_price, total_price, quantity, self_med_amount
                        ])

            # ファイル名をインポート済みとして記録
            cursor.execute('INSERT INTO import_log (filename) VALUES (?)', (filename,))
            print(f"[完了] {filename} をインポートしました。")

        except Exception as e:
            print(f"[エラー] {filename}: {e}")

conn.commit()
conn.close()
print("✅ すべてのCSVの処理が完了しました。スキップされた商品は skipped.csv に保存されています。")

