import sqlite3
from fastapi import FastAPI

app = FastAPI()

conn = sqlite3.connect("finance.db", check_same_thread=False)
cursor = conn.cursor()

@app.get("/transaction_counts")
def get_transaction_counts():
    cursor.execute("SELECT details, COUNT(*) FROM expenses GROUP BY details")
    transactions = cursor.fetchall()
    
    return {"transaction_counts": {place: count for place, count in transactions}}
