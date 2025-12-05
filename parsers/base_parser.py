import asyncio
import random
import re
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime

import nodriver
from bs4 import BeautifulSoup


class BaseParser(ABC):
    """Базовый класс для всех парсеров"""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[nodriver.Browser] = None
        self.results: List[Dict[str, Any]] = []

    async def init_browser(self) -> bool:
        """Инициализация браузера с улучшенной маскировкой"""
        try:
            print(f"🚀 Запускаем браузер (headless={self.headless})...")

            # Дополнительные аргументы для обхода детекции
            args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-web-security",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-sync",
                "--disable-translate",
                "--metrics-recording-only",
                "--no-first-run",
                "--mute-audio",
                "--hide-scrollbars",
                "--disable-notifications",
                "--disable-popup-blocking",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-breakpad",
                "--disable-component-extensions-with-background-pages",
                "--disable-features=TranslateUI",
                "--disable-ipc-flooding-protection",
                "--disable-renderer-backgrounding",
                "--enable-automation",
                "--password-store=basic",
                "--use-mock-keychain",
                f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]

            self.browser = await nodriver.start(
                headless=self.headless,
                window_size=(1200, 800),
                disable_features=[],
                args=args
            )

            # Дополнительно: скрываем WebDriver флаги через JavaScript
            page = await self.browser.get('about:blank')
            await page.evaluate("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                window.chrome = {
                    runtime: {}
                };
            """)

            print("✅ Браузер запущен")
            return True
        except Exception as e:
            print(f"❌ Ошибка запуска браузера: {e}")
            return False

    async def close(self):
        """Закрытие браузера - упрощенная версия для nodriver"""
        print("\n🔄 Завершаем работу парсера...")
        if self.browser:
            try:
                # В nodriver просто очищаем ссылку, так как stop() может не работать
                self.browser = None
                print("✅ Ресурсы освобождены")
            except Exception as e:
                print(f"⚠️ Ошибка при закрытии: {e}")
                self.browser = None

    async def random_delay(self, min_seconds: float = 1, max_seconds: float = 3):
        """Случайная задержка между запросами"""
        delay = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(delay)

    def normalize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Нормализация данных в единый формат"""
        normalized = {
            'Название объекта': data.get('Название объекта', data.get('name', '')),
            'Координаты': data.get('Координаты', data.get('coordinates', '')),
            'Адрес': data.get('Адрес', data.get('address', '')),
            'Телефон': data.get('Телефон', data.get('phone', '')),
            'Сайт': data.get('Сайт', data.get('website', '')),
            'Тип объекта': data.get('Тип объекта', data.get('category', '')),
            'Ссылка на объект': data.get('Ссылка', data.get('url', '')),
            'Тип парковки': data.get('Тип парковки', data.get('parking_type', 'неизвестно')),
            'Доступ': data.get('Доступ', ''),
            'Тарифы': data.get('Тарифы', ''),
            'Цены': data.get('Цены', ''),
            'Время работы': data.get('Время работы', data.get('opening_hours', '')),
            'Вместимость': data.get('Вместимость', ''),
            'Оценка': data.get('Оценка', data.get('rating', '')),
            'Количество оценок': data.get('Количество оценок', data.get('review_count', '')),
            'Описание': data.get('Описание', ''),
            'source': self.source_name,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Очистка данных
        for key, value in normalized.items():
            if isinstance(value, str):
                value = ' '.join(value.split())
                normalized[key] = value

        return normalized

    def extract_coordinates(self, url: str) -> Optional[str]:
        """Извлечение координат из URL"""
        patterns = [
            r'@([\d\.]+),([\d\.]+)',
            r'll=([\d\.]+)%2C([\d\.]+)',
            r'/([\d\.]+)%2C([\d\.]+)/',
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                lon, lat = match.groups()
                return f"{lon},{lat}"

        return None

    def detect_parking_type(self, html: str, name: str = "") -> str:
        """Определение типа парковки"""
        text = (html + " " + name).lower()
        type_info = []

        if any(word in text for word in ['платн', 'оплат', 'тариф', 'цена', '₽', 'руб']):
            type_info.append('платная')
        elif any(word in text for word in ['бесплатн', 'free', 'gratis']):
            type_info.append('бесплатная')

        if any(word in text for word in ['крыт', 'закрыт', 'охраня', 'подземн']):
            type_info.append('крытая')
            type_info.append('охраняемая')
        elif any(word in text for word in ['уличн', 'открыт', 'гост']):
            type_info.append('уличная')

        return ", ".join(type_info) if type_info else "неизвестно"

    def _safe_get_text(self, element, default="") -> str:
        """Безопасное получение текста из BeautifulSoup элемента"""
        if element:
            text = element.get_text(' ', strip=True)
            return ' '.join(text.split())
        return default

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Имя источника"""
        pass

    @abstractmethod
    async def parse(self) -> List[Dict[str, Any]]:
        """Основной метод парсинга"""
        pass