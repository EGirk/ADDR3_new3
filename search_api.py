"""API для пошуку адрес по вільному тексту"""

from fastapi import FastAPI, Query, HTTPException
from typing import Optional, List
from src.search.address_search import get_search_engine

app = FastAPI(title="Addrinity Search API", version="1.0.0")

@app.get("/api/search")
async def search_address(
    query: str = Query(..., description="Текст для пошуку адреси"),
    limit: int = Query(50, description="Максимальна кількість результатів")
):
    """
    Пошук адрес по вільному тексту
    
    Приклади запитів:
    - /api/search?query=вулиця Хрещатик 15
    - /api/search?query=Дніпро, Старий Шлях 192
    - /api/search?query=Кірова вул., буд. 100
    """
    try:
        searcher = get_search_engine()
        results = searcher.search_by_free_text(query, limit)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/autocomplete")
async def autocomplete(
    partial: str = Query(..., description="Частковий текст для автодоповнення"),
    object_type: str = Query("street", description="Тип об'єкта: street, district")
):
    """
    Автодоповнення для пошуку
    
    Приклади запитів:
    - /api/autocomplete?partial=Хрещ&object_type=street
    - /api/autocomplete?partial=Таром&object_type=district
    """
    try:
        searcher = get_search_engine()
        suggestions = searcher.fuzzy_search(partial, object_type)
        return {"suggestions": suggestions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/address/{building_id}")
async def get_address_details(building_id: int):
    """
    Отримання деталей адреси за ID будівлі
    
    Приклад: /api/address/12345
    """
    try:
        searcher = get_search_engine()
        # Реалізація отримання деталей адреси
        return {"building_id": building_id, "details": "Адресна інформація"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    