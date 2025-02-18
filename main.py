from fastapi import FastAPI, File, UploadFile
import pandas as pd
import pdfplumber
import io
import sqlite3
import datetime

app = FastAPI()

# Подключение к базе SQLite
conn = sqlite3.connect("finance.db", check_same_thread=False)
cursor = conn.cursor()

# Создаём таблицы
cursor.execute("""
CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    category TEXT,
    amount REAL,
    details TEXT
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    amount REAL,
    details TEXT
)
""")
conn.commit()

# Категории расходов
CATEGORY_MAP = {
    "MAGNUM": "Еда",
    "WOLT.COM": "Еда",
    "МакДак": "Еда",
    "Beeline": "Связь",
    "Яндекс.Такси": "Транспорт",
    "Ali mart": "Разное"  # Если нужно изменить, просто поменяй здесь
}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Загружает PDF, CSV или Excel, обрабатывает и сохраняет данные."""
    file_extension = file.filename.split('.')[-1].lower()
    contents = await file.read()

    if file_extension == "csv":
        df = pd.read_csv(io.BytesIO(contents))
    elif file_extension == "xlsx":
        df = pd.read_excel(io.BytesIO(contents))
    elif file_extension == "pdf":
        df = extract_pdf_data(contents)
    else:
        return {"error": "Формат не поддерживается."}

    if df is None or df.empty:
        return {"error": "Не удалось обработать файл."}

    # Фильтруем данные
    for _, row in df.iterrows():
        if "пополнение" in row["details"].lower() or "перевод" in row["details"].lower():
            # Это внутренний перевод Kaspi -> Kaspi
            cursor.execute("INSERT INTO transfers (date, amount, details) VALUES (?, ?, ?)",
                           (row["date"], row["amount"], row["details"]))
        else:
            # Это реальный расход
            category = categorize_expense(row["details"])
            cursor.execute("INSERT INTO expenses (date, category, amount, details) VALUES (?, ?, ?, ?)",
                           (row["date"], category, row["amount"], row["details"]))

    conn.commit()
    return {"message": "Файл успешно загружен и обработан.", "rows_added": len(df)}

def extract_pdf_data(contents):
    """Извлекает данные из PDF."""
    with pdfplumber.open(io.BytesIO(contents)) as pdf:
        data = []
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                for row in table[1:]:  # Пропускаем заголовок
                    date, amount, operation, details = row[:4]
                    if amount:
                        amount = float(amount.replace("₸", "").replace(" ", "").replace(",", ".").strip())
                        data.append({"date": date, "amount": amount, "details": details})
        return pd.DataFrame(data)

def categorize_expense(details):
    """Определяет категорию расхода по деталям."""
    for keyword, category in CATEGORY_MAP.items():
        if keyword.lower() in details.lower():
            return category
    return "Другое"  # Если категория не определена

@app.get("/summary")
def get_summary():
    """Возвращает сумму трат по категориям и общую сумму."""
    cursor.execute("SELECT category, SUM(amount) FROM expenses GROUP BY category")
    summary = cursor.fetchall()
    total = sum(amount for _, amount in summary)
    
    return {"total_spent": total, "category_breakdown": {cat: amt for cat, amt in summary}}

@app.get("/daily_summary")
def get_daily_summary():
    """Возвращает расходы по дням."""
    cursor.execute("SELECT date, SUM(amount) FROM expenses GROUP BY date ORDER BY date")
    daily_summary = cursor.fetchall()
    
    return {"daily_expenses": {date: amt for date, amt in daily_summary}}

@app.get("/monthly_summary")
def get_monthly_summary():
    """Возвращает расходы по месяцам."""
    cursor.execute("SELECT SUBSTR(date, 4, 7) AS month, SUM(amount) FROM expenses GROUP BY month ORDER BY month")
    monthly_summary = cursor.fetchall()
    
    return {"monthly_expenses": {month: amt for month, amt in monthly_summary}}

@app.get("/check_overbudget")
def check_overbudget(limit: float):
    """Проверяет, превышен ли бюджет."""
    cursor.execute("SELECT SUM(amount) FROM expenses")
    total_spent = cursor.fetchone()[0] or 0
    
    if total_spent > limit:
        return {"status": "overbudget", "total_spent": total_spent, "limit": limit}
    else:
        return {"status": "ok", "total_spent": total_spent, "limit": limit}

@app.get("/advice")
def get_advice():
    """Анализирует расходы и даёт советы по сокращению трат."""
    cursor.execute("SELECT category, SUM(amount) FROM expenses GROUP BY category ORDER BY SUM(amount) DESC")
    category_data = cursor.fetchall()

    if not category_data:
        return {"advice": "Данных недостаточно для анализа."}

    top_category, max_spent = category_data[0]
    total_spent = sum(amount for _, amount in category_data)
    
    advice = [
        f"Самая большая статья расходов — '{top_category}', всего {max_spent}₸. Подумай, можно ли сократить эти траты.",
        f"Общий расход за этот период составил {total_spent}₸. Проверь, не выходишь ли за рамки бюджета.",
        f"Если хочешь сэкономить, попробуй ограничить траты в категории '{top_category}' и распределить деньги более равномерно."
    ]

    return {"advice": advice}
