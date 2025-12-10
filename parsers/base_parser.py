import asyncio
import random
import re
import time
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Set
from datetime import datetime

import nodriver
from bs4 import BeautifulSoup


class BaseParser(ABC):
    """Базовый класс для всех парсеров"""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[nodriver.Browser] = None
        self.results: List[Dict[str, Any]] = []
        self.start_time = None
        self.all_urls: Set[str] = set()
        self.max_consecutive_no_new = 3  # Максимум 3 попытки без новых URL

    # === ОБЩИЕ МЕТОДЫ ИНИЦИАЛИЗАЦИИ ===

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
                window_size=(1200, 900),
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
        """Закрытие браузера"""
        print("\n🔄 Завершаем работу парсера...")
        if self.browser:
            try:
                self.browser = None
                print("✅ Ресурсы освобождены")
            except Exception as e:
                print(f"⚠️ Ошибка при закрытии: {e}")
                self.browser = None

    # === ОБЩИЕ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

    async def random_delay(self, min_seconds: float = 1, max_seconds: float = 3):
        """Случайная задержка между запросами"""
        delay = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(delay)

    def _shorten_url(self, url: str, max_length: int = 60) -> str:
        """Сокращение URL для вывода"""
        if len(url) <= max_length:
            return url
        return url[:max_length - 3] + "..."

    def _clean_text(self, text: str) -> str:
        """Очистка текста"""
        if not text:
            return ""
        text = ' '.join(text.split())
        return text.strip()

    def _safe_get_text(self, element, default: str = "") -> str:
        """Безопасное получение текста из BeautifulSoup элемента"""
        if element:
            text = element.get_text(' ', strip=True)
            return ' '.join(text.split())
        return default

    def _is_loading_element_visible(self, soup: BeautifulSoup) -> bool:
        """Проверка видимости элементов загрузки"""
        loading_selectors = [
            '.spin2', '.spinner', '.loading', '.loader',
            '[class*="loading"]', '[class*="spinner"]'
        ]

        for selector in loading_selectors:
            if soup.select_one(selector):
                return True
        return False

    # === МЕТОДЫ ПАРСИНГА СТРАНИЦ ОБЪЕКТОВ ===

    async def _parse_all_parking_pages(self, urls: List[str]) -> None:
        """Общий метод парсинга всех страниц объектов"""
        print(f"\n🏢 Начинаем парсинг {len(urls)} объектов из {self.source_name}...")

        success_count = 0
        fail_count = 0

        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Объект {i}")
            print(f"   🔗 {self._shorten_url(url, 60)}")

            # Парсим страницу
            data = await self._parse_single_page(url)

            if data:
                normalized_data = self.normalize_data(data)
                self.results.append(normalized_data)
                success_count += 1

                # Выводим краткую информацию
                name = normalized_data.get('Название объекта', 'Без названия')[:40]
                address = normalized_data.get('Адрес', '')[:50]
                parking_type = normalized_data.get('Тип парковки', 'неизвестно')

                print(f"   ✅ {name}")
                print(f"      📍 {address}")
                print(f"      🚗 Тип: {parking_type}")
            else:
                fail_count += 1
                print(f"   ❌ Не удалось распарсить")

            # Статистика прогресса
            if i % 10 == 0 or i == len(urls):
                progress = (i / len(urls)) * 100
                elapsed = time.time() - self.start_time
                estimated_total = (elapsed / max(1, i)) * len(urls)
                remaining = max(0, estimated_total - elapsed)

                print(f"\n📊 Прогресс: {i}/{len(urls)} ({progress:.1f}%)")
                print(f"⏱ Прошло: {elapsed:.0f}с | Осталось: {remaining:.0f}с")
                print(f"✅ Успешно: {success_count} | ❌ Ошибок: {fail_count}")

            # Задержка между запросами
            if i < len(urls):
                delay = random.uniform(4, 7)
                print(f"   ⏳ Задержка {delay:.1f}с...")
                await asyncio.sleep(delay)

        print(f"\n🎉 Парсинг завершен!")
        print(f"📊 Итог: Успешно {success_count}, Ошибок {fail_count}")

    async def _parse_single_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Общий метод парсинга одной страницы объекта"""
        max_retries = 2

        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    print(f"   🔄 Повторная попытка {attempt}/{max_retries}")
                    await asyncio.sleep(random.uniform(3, 5))

                # Открываем страницу в новом табе
                tab = await self.browser.get(url)

                # Ждем загрузки
                await asyncio.sleep(random.uniform(3, 4))

                # Получаем HTML
                html = await tab.get_content()

                # Парсим данные
                soup = BeautifulSoup(str(html), 'lxml')

                # Используем метод конкретного парсера
                data = self._extract_page_data(url, soup, str(html))

                # Проверяем минимальные данные
                if data.get('Название объекта') or data.get('Адрес'):
                    return data
                else:
                    print(f"   ⚠ Мало данных на странице")

            except Exception as e:
                error_msg = str(e)
                print(f"   ✗ Ошибка: {error_msg[:50]}...")

            # Задержка перед повторной попыткой
            if attempt < max_retries:
                retry_delay = random.uniform(5, 8)
                await asyncio.sleep(retry_delay)

        return None

    # === МЕТОДЫ НОРМАЛИЗАЦИИ И ОБРАБОТКИ ===

    def normalize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Нормализация данных в единый формат"""
        normalized = {
            'Название объекта': data.get('Название объекта', ''),
            'Координаты': data.get('Координаты', ''),
            'Адрес': data.get('Адрес', ''),
            'Телефон': data.get('Телефон', ''),
            'Сайт': data.get('Сайт', ''),
            'Тип объекта': data.get('Тип объекта', ''),
            'Ссылка': data.get('Ссылка', ''),
            'Название парковки': data.get('Название парковки', ''),
            'Ссылка на парковку': data.get('Ссылка на парковку', ''),
            'Адрес парковки': data.get('Адрес парковки', ''),
            'Тип парковки': data.get('Тип парковки', ''),
            'Доступ': data.get('Доступ', ''),
            'Время работы': data.get('Время работы', ''),
            'Тарифы': data.get('Тарифы', ''),
            'Цены': data.get('Цены', ''),
            'Вместимость': data.get('Вместимость', ''),
            'Оценка': data.get('Оценка', ''),
            'Количество оценок': data.get('Количество оценок', ''),
            'Описание': data.get('Описание', ''),
            'source': self.source_name,
            'timestamp': data.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        }

        # Очистка данных
        for key, value in normalized.items():
            if isinstance(value, str):
                value = ' '.join(value.split())
                normalized[key] = value

        return normalized

    def _remove_duplicates(self):
        """Удаление дубликатов из результатов"""
        if not self.results:
            return

        unique_results = []
        seen_keys = set()

        for item in self.results:
            # Генерируем уникальный ключ
            name = item.get('Название объекта', '').strip()
            address = item.get('Адрес', '').strip()
            url = item.get('Ссылка на объект') or item.get('Ссылка', '')

            if url:
                key = f"{self.source_name}_{url}"
            elif name and address:
                key = f"{self.source_name}_{name[:30]}_{address[:30]}"
            else:
                continue  # Пропускаем объекты без ключевых данных

            if key not in seen_keys:
                seen_keys.add(key)
                unique_results.append(item)

        removed = len(self.results) - len(unique_results)
        if removed > 0:
            print(f"🗑 Удалено {removed} дубликатов из {self.source_name}")

        self.results = unique_results

    def _print_final_stats(self, urls_collected: int = None):
        """Общий метод вывода финальной статистики"""
        print("\n" + "=" * 60)
        print(f"📊 ФИНАЛЬНАЯ СТАТИСТИКА {self.source_name.upper()}")
        print("=" * 60)

        if self.start_time:
            elapsed = time.time() - self.start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            print(f"⏱ Общее время: {minutes} мин {seconds} сек")

        if urls_collected:
            print(f"🔗 Собрано уникальных URL: {urls_collected}")

        print(f"✅ Успешно распарсено: {len(self.results)}")

        if urls_collected and len(self.results) > 0:
            efficiency = len(self.results) / max(1, urls_collected) * 100
            print(f"📈 Эффективность парсинга: {efficiency:.1f}%")

        if self.results:
            # Статистика по типам парковок
            closed_count = len([p for p in self.results if 'закрыт' in p.get('Тип парковки', '').lower()])
            paid_count = len([p for p in self.results if 'платн' in p.get('Тип парковки', '').lower()])
            guarded_count = len([p for p in self.results if 'охраня' in p.get('Тип парковки', '').lower()])

            print(f"\n🚗 ТИПЫ ПАРКОВОК:")
            print(f"   Закрытых/охраняемых: {closed_count}")
            print(f"   Платных: {paid_count}")

            # Качество данных
            with_coords = len([p for p in self.results if p.get('Координаты')])
            with_address = len([p for p in self.results if p.get('Адрес')])
            with_phone = len([p for p in self.results if p.get('Телефон')])

            print(f"\n📊 КАЧЕСТВО ДАННЫХ:")
            print(
                f"   С координатами: {with_coords}/{len(self.results)} ({with_coords / len(self.results) * 100:.1f}%)")
            print(f"   С адресами: {with_address}/{len(self.results)} ({with_address / len(self.results) * 100:.1f}%)")
            print(f"   С телефонами: {with_phone}/{len(self.results)} ({with_phone / len(self.results) * 100:.1f}%)")

        print("=" * 60)

    # === АБСТРАКТНЫЕ МЕТОДЫ ===

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Имя источника"""
        pass

    @abstractmethod
    async def parse(self) -> List[Dict[str, Any]]:
        """Основной метод парсинга"""
        pass

    @abstractmethod
    def _extract_page_data(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """Извлечение данных со страницы объекта"""
        pass
