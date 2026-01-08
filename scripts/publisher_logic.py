from datetime import datetime, timedelta
from typing import List, Dict, Optional
from supabase import create_client

def get_vacancies_for_publication(
    supabase_client, 
    city_slug: str, 
    limit: int = 5,
    min_salary_net: int = 70000
) -> List[Dict]:
    """
    Получает вакансии для публикации по новым критериям.
    
    Критерии:
    1. is_posted = FALSE
    2. published_at не старше 30 дней
    3. created_at (парсинг) не старше 7 дней  
    4. currency = 'RUR'
    5. salary_to_net >= min_salary_net
    """
    
    # Рассчитываем даты-ограничители
    now = datetime.now()
    max_vacancy_date = now - timedelta(days=30)  # Не старше 30 дней
    max_parsed_date = now - timedelta(days=7)    # Не старше 7 дней с парсинга
    
    # Строим запрос
    query = (
        supabase_client
        .table("vacancies")
        .select("*")
        .eq("city_slug", city_slug)
        .eq("is_posted", False)
        .eq("currency", "RUR")
        .gte("salary_to_net", min_salary_net)
        .gte("published_at", max_vacancy_date.isoformat())
        .gte("created_at", max_parsed_date.isoformat())
    )
    
    # Сортировка
    query = query.order("salary_to_net", desc=True)
    query = query.order("published_at", desc=True)
    
    # Лимит
    query = query.limit(limit)
    
    # Выполняем
    response = query.execute()
    
    return response.data if response.data else []

def format_salary_display(vacancy: Dict) -> str:
    """Форматирует отображение зарплаты."""
    parts = []
    
    # Зарплата "от" (на руки)
    if vacancy.get("salary_from_net"):
        parts.append(f"от {vacancy['salary_from_net']:,} ₽".replace(",", " "))
    
    # Зарплата "до" (на руки) - приоритетная
    if vacancy.get("salary_to_net"):
        if vacancy.get("salary_from_net"):
            parts.append(f"до {vacancy['salary_to_net']:,} ₽".replace(",", " "))
        else:
            parts.append(f"{vacancy['salary_to_net']:,} ₽".replace(",", " "))
    
    return " ".join(parts) if parts else "ЗП не указана"

def format_payment_info(vacancy: Dict) -> str:
    """Форматирует информацию о выплатах."""
    parts = []
    
    # Форма выплат (за месяц/день и т.д.)
    if vacancy.get("salary_period_name"):
        parts.append(vacancy["salary_period_name"])
    
    # Частота выплат
    if vacancy.get("salary_frequency_name"):
        parts.append(f"({vacancy['salary_frequency_name']})")
    
    return " ".join(parts) if parts else ""

def format_publication_date(published_at: str) -> str:
    """Форматирует дату публикации."""
    try:
        pub_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        now = datetime.now()
        diff = now - pub_date
        
        if diff.days == 0:
            return "сегодня"
        elif diff.days == 1:
            return "вчера"
        elif diff.days < 7:
            return f"{diff.days} дня назад"
        elif diff.days < 30:
            return f"{diff.days // 7} недели назад"
        else:
            return pub_date.strftime("%d.%m.%Y")
    except:
        return ""

def should_publish_now(post_times_msk: List[str]) -> bool:
    """Проверяет, нужно ли публиковать сейчас."""
    from datetime import timezone
    
    now_msk = datetime.now(timezone(timedelta(hours=3)))
    current_time_str = now_msk.strftime("%H:%M")
    
    # Проверяем плюс-минус 10 минут от запланированного времени
    for scheduled_time in post_times_msk:
        scheduled_hour, scheduled_minute = map(int, scheduled_time.split(":"))
        scheduled_dt = now_msk.replace(hour=scheduled_hour, minute=scheduled_minute, second=0)
        
        time_diff = abs((now_msk - scheduled_dt).total_seconds() / 60)  # в минутах
        
        if time_diff <= 10:  # +-10 минут
            return True
    
    return False
