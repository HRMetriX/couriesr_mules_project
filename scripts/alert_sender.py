#!/usr/bin/env python3
"""
Система алертов для проекта Courier Mules.
Отправляет уведомления в Telegram бота.

Использование:
    from alert_sender import send_alert
    
    # Простое сообщение
    send_alert("Парсинг запущен", alert_type="info")
    
    # С ошибкой
    send_alert("Ошибка парсинга", 
               details="Не удалось подключиться к HH.ru",
               alert_type="error")
    
    # Успех со статистикой
    send_alert("Публикация завершена",
               details="Опубликовано 24 вакансии",
               stats={"города": 5, "вакансии": 24},
               alert_type="success")
"""

import os
import sys
import requests
from datetime import datetime
from typing import Dict, Optional, Any
import json
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
PROJECT_NAME = "Courier Mules"
BOT_TOKEN = os.environ.get("TG_ALERT_BOT_TOKEN")
CHAT_ID = os.environ.get("TG_ALERT_CHAT_ID")  # Берем из переменных окружения

# Emoji для разных типов алертов
EMOJI_MAP = {
    "info": "ℹ️",
    "success": "✅", 
    "warning": "⚠️",
    "error": "❌",
    "critical": "🚨",
    "report": "📊",
    "parser": "🔍",
    "publisher": "📢",
    "system": "⚙️",
    "start": "🚀",
    "complete": "🏁",
    "debug": "🐛",
}

def check_config() -> tuple[bool, str]:
    """Проверяет конфигурацию."""
    if not BOT_TOKEN:
        return False, "TG_ALERT_BOT_TOKEN не установлен"
    if not CHAT_ID:
        return False, "TG_ALERT_CHAT_ID не установлен"
    return True, "OK"

def format_timestamp() -> str:
    """Возвращает отформатированное время для сообщения."""
    now = datetime.now()
    return now.strftime("%d.%m.%Y %H:%M:%S")

def escape_html(text: str) -> str:
    """Экранирует HTML символы для Telegram."""
    if not text:
        return ""
    return (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;'))

def format_stats(stats: Dict[str, Any]) -> str:
    """Форматирует статистику в читаемый вид."""
    if not stats:
        return ""
    
    lines = []
    for key, value in stats.items():
        if isinstance(value, (int, float)):
            # Форматируем числа с разделителями
            if isinstance(value, int):
                value_str = f"{value:,}".replace(",", " ")
            else:
                value_str = f"{value:,.1f}".replace(",", " ").replace(".", ",")
        elif isinstance(value, list):
            value_str = ", ".join(str(v) for v in value[:5]) + ("..." if len(value) > 5 else "")
        elif isinstance(value, dict):
            value_str = json.dumps(value, ensure_ascii=False)[:50] + "..."
        else:
            value_str = str(value)
        
        # Красивые названия для стандартных метрик
        key_names = {
            "vacancies_found": "Найдено вакансий",
            "vacancies_added": "Добавлено новых",
            "duplicates": "Дубликатов",
            "duration": "Длительность",
            "cities": "Городов",
            "posts": "Постов",
            "errors": "Ошибок",
            "success_rate": "Успешность",
            "total": "Всего",
            "new": "Новых",
            "awaiting": "Ожидают публикации",
            "age_days": "Средний возраст (дни)",
            "parsing_sessions": "Сессий парсинга",
            "publications": "Публикаций",
            "coverage": "Охват городов",
            "avg_per_post": "Среднее на пост",
        }
        
        display_key = key_names.get(key, key.replace("_", " ").title())
        lines.append(f"  • {display_key}: {value_str}")
    
    return "\n".join(lines)

def send_alert(
    message: str,
    details: Optional[str] = None,
    stats: Optional[Dict[str, Any]] = None,
    alert_type: str = "info",
    context: Optional[str] = None,
    error_traceback: Optional[str] = None,
    include_timestamp: bool = True,
    max_length: int = 4000,
) -> bool:
    """
    Отправляет алерт в Telegram.
    
    Args:
        message: Основное сообщение
        details: Детали (что именно произошло)
        stats: Статистика в виде словаря
        alert_type: Тип алерта (info, success, error и т.д.)
        context: Контекст (parser, publisher, system)
        error_traceback: Трейсбэк ошибки для детального отчета
        include_timestamp: Добавлять временную метку
        max_length: Максимальная длина сообщения
        
    Returns:
        bool: Успешно ли отправлено
    """
    # Проверяем конфигурацию
    config_ok, config_error = check_config()
    if not config_ok:
        logger.warning(f"⚠️ {config_error}. Сообщение: {message}")
        # В режиме разработки выводим в консоль
        if os.environ.get("DEBUG_ALERTS"):
            print(f"[DEBUG ALERT] {message}")
            if details:
                print(f"  Детали: {details}")
            if stats:
                print(f"  Статистика: {stats}")
        return False
    
    # Получаем emoji для типа алерта
    emoji = EMOJI_MAP.get(alert_type, "🔔")
    
    # Получаем emoji для контекста если указан
    context_emoji = ""
    if context and context in EMOJI_MAP:
        context_emoji = EMOJI_MAP[context]
    
    # Собираем заголовок
    header_parts = []
    if context_emoji:
        header_parts.append(context_emoji)
    header_parts.append(PROJECT_NAME)
    header = " ".join(header_parts)
    
    # Формируем сообщение
    parts = []
    
    # Заголовок
    parts.append(f"<b>{header}</b>")
    
    # Временная метка
    if include_timestamp:
        parts.append(f"<i>🕐 {format_timestamp()}</i>")
    
    # Основное сообщение
    parts.append(f"\n<b>{escape_html(message)}</b>")
    
    # Детали
    if details:
        parts.append(f"\n📝 <b>Детали:</b>\n{escape_html(details)}")
    
    # Статистика
    if stats:
        formatted_stats = format_stats(stats)
        if formatted_stats:
            parts.append(f"\n📊 <b>Статистика:</b>\n{escape_html(formatted_stats)}")
    
    # Трейсбэк ошибки (сокращенный)
    if error_traceback:
        # Берем только последние 3 строки трейсбэка
        trace_lines = error_traceback.strip().split('\n')
        if len(trace_lines) > 3:
            trace_lines = trace_lines[:1] + ["..."] + trace_lines[-2:]
        short_trace = "\n".join(trace_lines)
        parts.append(f"\n🔍 <b>Ошибка:</b>\n<code>{escape_html(short_trace)}</code>")
    
    # Собираем финальное сообщение
    full_message = "\n".join(parts)
    
    # Проверяем длину (ограничение Telegram: 4096 символов)
    if len(full_message) > max_length:
        logger.warning(f"Сообщение слишком длинное ({len(full_message)} символов), обрезаем...")
        # Берем заголовок, время, сообщение и детали
        basic_parts = parts[:4] if len(parts) >= 4 else parts
        full_message = "\n".join(basic_parts)
        full_message += f"\n\n⚠️ <i>Статистика обрезана (сообщение {len(full_message)}/{max_length} символов)</i>"
    
    # Отправляем в Telegram
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": full_message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.info(f"✅ Алерт отправлен: {message[:50]}...")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка отправки алерта: {e}")
        logger.error(f"Сообщение: {message}")
        return False
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка: {e}")
        return False

def send_simple_alert(text: str, alert_type: str = "info") -> bool:
    """Упрощенная отправка алерта."""
    return send_alert(text, alert_type=alert_type)

def test_alert_system():
    """Тестирует систему алертов отправкой тестовых сообщений."""
    print("🧪 Тестирование системы алертов...")
    logger.info("Запуск теста системы алертов")
    
    # Проверка конфигурации
    config_ok, config_error = check_config()
    if not config_ok:
        print(f"❌ {config_error}")
        print("Проверь переменные окружения:")
        print("  - TG_ALERT_BOT_TOKEN")
        print("  - TG_ALERT_CHAT_ID")
        return False
    
    print(f"✅ Конфигурация OK")
    print(f"   Бот: {'*' * 10}{BOT_TOKEN[-10:] if BOT_TOKEN else 'N/A'}")
    print(f"   Chat ID: {CHAT_ID[:5]}...{CHAT_ID[-5:] if CHAT_ID else 'N/A'}")
    
    # Тест 1: Информационное сообщение
    print("📨 Отправка информационного сообщения...")
    success1 = send_alert(
        "Тестовый запуск системы алертов",
        details="Проверка работы всех компонентов системы уведомлений",
        alert_type="info",
        context="system"
    )
    
    # Тест 2: Успешная операция
    print("📨 Отправка сообщения об успехе...")
    success2 = send_alert(
        "Парсинг вакансий завершен успешно",
        details="Обработан источник HH.ru для 5 городов",
        stats={
            "vacancies_found": 156,
            "vacancies_added": 47,
            "duplicates": 109,
            "duration": "3m 22s",
            "success_rate": "100%",
            "cities": ["Москва", "СПб", "Новосибирск", "Екатеринбург", "Казань"]
        },
        alert_type="success",
        context="parser"
    )
    
    # Тест 3: Предупреждение
    print("📨 Отправка предупреждения...")
    success3 = send_alert(
        "Мало новых вакансий",
        details="За последние 24 часа добавлено менее 20 новых вакансий",
        stats={
            "vacancies_added": 15,
            "threshold": 20,
            "period": "24 часа"
        },
        alert_type="warning",
        context="parser"
    )
    
    # Тест 4: Ошибка
    print("📨 Отправка сообщения об ошибке...")
    try:
        # Имитируем ошибку
        raise ConnectionError("Не удалось подключиться к Supabase: timeout 30s")
    except Exception as e:
        import traceback
        success4 = send_alert(
            "Критическая ошибка подключения",
            details=f"Ошибка при работе с базой данных: {str(e)}",
            alert_type="critical",
            context="system",
            error_traceback=traceback.format_exc()
        )
    
    # Итоги
    print("\n" + "="*50)
    print("ИТОГИ ТЕСТИРОВАНИЯ:")
    print(f"  Информационное: {'✅' if success1 else '❌'}")
    print(f"  Успех: {'✅' if success2 else '❌'}")
    print(f"  Предупреждение: {'✅' if success3 else '❌'}")
    print(f"  Ошибка: {'✅' if success4 else '❌'}")
    
    total = sum([success1, success2, success3, success4])
    if total == 4:
        print("✅ Все тесты пройдены успешно!")
    else:
        print(f"⚠️  Пройдено {total}/4 тестов")
    
    return total == 4

if __name__ == "__main__":
    # Запуск теста при прямом выполнении
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        success = test_alert_system()
        sys.exit(0 if success else 1)
    else:
        print("Использование: python alert_sender.py test")
        print("\nПеременные окружения:")
        print("  TG_ALERT_BOT_TOKEN - токен Telegram бота")
        print("  TG_ALERT_CHAT_ID   - chat_id получателя")
        print("\nДля локального теста:")
        print("  export TG_ALERT_BOT_TOKEN='your_token'")
        print("  export TG_ALERT_CHAT_ID='your_chat_id'")
        print("  python alert_sender.py test")
