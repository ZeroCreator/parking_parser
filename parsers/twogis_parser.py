"2Gis parser"

import asyncio
import random
import time
import re
import hashlib
from typing import List, Dict, Any, Optional, Set
from datetime import datetime

from bs4 import BeautifulSoup
import nodriver

from .base_parser import BaseParser


class TwoGisParser(BaseParser):
    """Парсер 2ГИС."""

    def __init__(self, headless: bool = True):
        super().__init__(headless)
        self.processed_ids: Set[str] = set()
        self.start_time = None
        self.all_parking_urls: Set[str] = set()
        self.session_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        }

    @property
    def source_name(self) -> str:
        return "2gis"

    #
    # Основной метод парсинга 2ГИС
    #

    async def parse(self, max_pages: int = 30) -> List[Dict[str, Any]]:
        """Основной метод парсинга 2ГИС."""
        print("=" * 60)
        print("🚀 ЗАПУСК ПАРСЕРА 2ГИС")
        print("=" * 60)

        self.start_time = time.time()
        self.results = []

        if not await self.init_browser():
            return []

        try:
            # ЭТАП 1: Собираем ВСЕ ссылки
            print(f"\n📄 ЭТАП 1: СБОР ВСЕХ ССЫЛОК НА ПАРКОВКИ")
            print("-" * 50)

            await self._collect_all_parking_urls_by_scroll_simple()

            if not self.all_parking_urls:
                print("❌ Не удалось собрать ссылки на парковки")
                return []

            print(f"\n✅ Собрано уникальных ссылок на парковки: {len(self.all_parking_urls)}")

            # ЭТАП 2: Парсим ВСЕ собранные ссылки на парковки
            print("\n🏢 ЭТАП 2: ПАРСИНГ ВСЕХ СОБРАННЫХ ПАРКОВОК")
            print("-" * 50)

            urls_list = list(self.all_parking_urls)
            await self._parse_all_parking_pages(urls_list)

            # Финальная статистика
            self._print_final_stats(urls_list)

            # Удаляем дубликаты
            self._remove_duplicates()

            return self.results

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
            return self.results
        finally:
            await self.close()

    #
    # Сбор ВСЕХ URL парковок
    #

    async def _collect_all_parking_urls_by_scroll_simple(self) -> bool:
        """Сбор ВСЕХ URL парковок."""
        print("🔍 Начинаем сбор ссылок...")

        # Начинаем с первой страницы
        start_url = "https://2gis.ru/spb/search/parking"

        print(f"📍 Начальная страница: {start_url}")

        tab = await self.browser.get(start_url)
        await asyncio.sleep(random.uniform(5, 7))

        # Собираем ссылки с первой страницы
        print("   📥 Собираем ссылки с первой страницы...")
        initial_urls = await self._get_urls_from_current_page(tab)
        if initial_urls:
            self.all_parking_urls.update(initial_urls)
            print(f"   📊 Первая страница: {len(initial_urls)} URL")
        else:
            print("   ⚠ Не удалось получить ссылки с первой страницы")
            return False

        # Прокручиваем страницу для подгрузки новых элементов
        print("   📜 Начинаем прокрутку страницы...")

        # Выполняем скроллинг до конца
        await self._scroll_to_bottom(tab)

        # Собираем ВСЕ URL после прокрутки
        current_urls = await self._get_urls_from_current_page(tab)
        if current_urls:
            previous_count = len(self.all_parking_urls)
            self.all_parking_urls.update(current_urls)
            new_urls = len(self.all_parking_urls) - previous_count
            print(f"      📎 Всего URL после прокрутки: {len(self.all_parking_urls)} (+{new_urls} новых)")

        # ПОСЛЕ ПРОКРУТКИ ДО КОНЦА - ПРОБУЕМ НАЙТИ КНОПКУ ПАГИНАЦИИ
        print("      🔍 Пробуем найти кнопку пагинации после прокрутки...")
        await self._try_find_pagination_after_scroll(tab)

        print(f"\n✅ Прокрутка завершена")
        print(f"📊 Итог: {len(self.all_parking_urls)} уникальных URL")

        return len(self.all_parking_urls) > 0

    async def _scroll_to_bottom(self, tab):
        """Прокручивает ВСЕ скроллируемые контейнеры на странице."""
        print("   📜 СКРОЛЛИМ ВСЕ КОНТЕЙНЕРЫ...")

        try:
            current_url = await tab.evaluate("window.location.href")
            print(f"      📍 Страница: {current_url}")

            # 1. Считаем сколько контейнеров
            container_count = await tab.evaluate("""
                document.querySelectorAll('[data-scroll], [tabindex], [overflow="auto"], [overflow="scroll"]').length
            """)

            # 2. Прокручиваем КАЖДЫЙ контейнер
            for i in range(container_count):
                await tab.evaluate(f"""
                    (function() {{
                        const containers = document.querySelectorAll('[data-scroll], [tabindex], [overflow="auto"], [overflow="scroll"]');
                        if (containers[{i}]) {{
                            const container = containers[{i}];
                            // Проверяем, можно ли скроллить
                            if (container.scrollHeight > container.clientHeight) {{
                                console.log('Скроллим контейнер', container.tagName, container.className);
                                container.scrollTop = container.scrollHeight;
                            }}
                        }}
                    }})()
                """)

                # Небольшая пауза между скроллами
                await asyncio.sleep(0.5)

            # 3. Ждем
            await asyncio.sleep(random.uniform(2, 3))

        except Exception as e:
            print(f"      ❌ Ошибка: {str(e)[:100]}")

    async def _try_find_pagination_after_scroll(self, tab, current_page: int = 1):
        """Попытка найти кнопки пагинации после прокрутки"""
        try:
            # Получаем HTML страницы
            html = await tab.get_content()
            soup = BeautifulSoup(html, 'lxml')

            next_page_num = current_page + 1
            found_next_page = False

            # Ищем ссылку на следующую страницу
            for link in soup.find_all('a', href=True):
                href = link['href']
                match = re.search(r'/page/(\d+)', href)
                if match:
                    page_num = int(match.group(1))
                    if page_num == next_page_num:
                        print(f"      🖱 Переходим на страницу {next_page_num}")

                        try:
                            selector = f'a[href*="/page/{next_page_num}"]'
                            element = await tab.query_selector(selector)

                            if element:
                                await element.click()
                                await asyncio.sleep(random.uniform(4, 6))

                                await self._scroll_to_bottom(tab)

                                urls_page = await self._get_urls_from_current_page(tab)
                                if urls_page:
                                    before = len(self.all_parking_urls)
                                    self.all_parking_urls.update(urls_page)
                                    new_count = len(self.all_parking_urls) - before
                                    print(f"      📊 +{new_count} новых URL")

                                await self._try_find_pagination_after_scroll(tab, next_page_num)
                                found_next_page = True
                                break

                        except Exception as e:
                            print(f"      ❌ Ошибка: {str(e)[:60]}")
                        break

            if not found_next_page:
                print(f"      ⚠ Нет больше страниц")

        except Exception as e:
            print(f"      ❌ Ошибка: {str(e)[:60]}")

    async def _collect_from_remaining_pages(self, tab, start_page: int = 3, max_pages: int = 20):
        """Сбор ссылок с оставшихся страниц."""
        print(f"      🔄 Начинаем сбор с оставшихся страниц (с {start_page})...")

        for page_num in range(start_page, max_pages + 1):
            print(f"      📄 Ищем страницу {page_num}...")

            # Собираем ссылки с текущей страницы
            current_urls = await self._get_urls_from_current_page(tab)
            if current_urls:
                before = len(self.all_parking_urls)
                self.all_parking_urls.update(current_urls)
                new_count = len(self.all_parking_urls) - before
                print(f"      📊 Собрано: {len(current_urls)} URL (+{new_count} новых)")

            # Ищем ссылку на следующую страницу
            selector = f'a[href*="/page/{page_num}"]'
            element = await tab.query_selector(selector)

            if element:
                print(f"      ✅ Нашли элемент для страницы {page_num}")

                try:
                    href = await element.get_attribute('href')
                    print(f"      🔗 HREF: {href}")
                except:
                    pass

                # Кликаем
                print(f"      🖱 Кликаем на страницу {page_num}...")
                await element.click()
                print(f"      ✅ Клик выполнен")

                # Ждем загрузки
                await asyncio.sleep(random.uniform(3, 5))
            else:
                print(f"      ❌ Страница {page_num} не найдена, заканчиваем")
                break

        # Собираем с последней страницы
        print(f"      📥 Собираем ссылки с последней страницы...")
        last_urls = await self._get_urls_from_current_page(tab)
        if last_urls:
            before = len(self.all_parking_urls)
            self.all_parking_urls.update(last_urls)
            new_count = len(self.all_parking_urls) - before
            print(f"      📊 С последней страницы: +{new_count} новых URL")

        print(f"      ✅ Сбор со страниц завершен")

    async def _get_urls_from_current_page(self, tab) -> Set[str]:
        """Получение URL парковок с текущей страницы."""
        try:
            await asyncio.sleep(1)
            html = await tab.get_content()

            urls = self._extract_urls_from_html(html)

            filtered_urls = set()
            for url in urls:
                if self._is_valid_parking_url(url):
                    clean_url = self._clean_parking_url(url)
                    if clean_url:
                        filtered_urls.add(clean_url)

            return filtered_urls

        except Exception as e:
            print(f"   ❌ Ошибка при извлечении URL: {str(e)[:50]}")
            return set()

    def _extract_urls_from_html(self, html: str) -> List[str]:
        """Извлечение URL парковок из HTML страницы поиска."""
        soup = BeautifulSoup(html, 'lxml')
        urls = []

        # Ищем все ссылки на фирмы
        firm_links = soup.select('a[href*="/firm/"]')

        for link in firm_links:
            href = link.get('href', '')
            if href and '/firm/' in href:
                # Формируем полный URL
                if href.startswith('//'):
                    full_url = f"https:{href}"
                elif href.startswith('/'):
                    full_url = f"https://2gis.ru{href}"
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue

                # Очищаем URL
                clean_url = self._clean_parking_url(full_url)
                if clean_url:
                    urls.append(clean_url)

        # Также ищем в data-атрибутах
        for elem in soup.select('[data-id]'):
            data_id = elem.get('data-id', '')
            if data_id and data_id.startswith('firm_'):
                firm_id = data_id.replace('firm_', '')
                url = f"https://2gis.ru/spb/firm/{firm_id}"
                if url not in urls:
                    urls.append(url)

        # Удаляем дубликаты
        return list(set(urls))

    def _is_valid_parking_url(self, url: str) -> bool:
        """Проверка валидности URL парковки"""
        # Должен содержать /firm/ и ID
        if '/firm/' not in url:
            return False

        # Исключаем нежелательные URL
        exclude_patterns = [
            '/reviews',
            '/gallery',
            '/photos',
            '/menu',
            '/contacts',
            '/search/',
            'tab=',
            '#'
        ]

        for pattern in exclude_patterns:
            if pattern in url:
                return False

        return True

    def _clean_parking_url(self, url: str) -> str:
        """Очистка URL парковки"""
        # Удаляем параметры и якоря
        url = url.split('?')[0].split('#')[0]

        # Удаляем лишние слэши
        url = url.rstrip('/')

        # Убеждаемся, что это полный URL
        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/'):
            url = f"https://2gis.ru{url}"
        elif not url.startswith('http'):
            url = f"https://{url}"

        return url

    def _short_url(self, url: str, max_length: int = 60) -> str:
        """Сокращение URL для вывода"""
        if len(url) <= max_length:
            return url
        return url[:max_length - 3] + "..."

    # Остальные методы остаются без изменений...
    async def _parse_all_parking_pages(self, urls: List[str]):
        """Парсинг ВСЕХ собранных страниц парковок"""
        print(f"\n🏢 Начинаем парсинг {len(urls)} парковок 2ГИС...")

        success_count = 0
        fail_count = 0

        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Парковка {i}")
            print(f"   🔗 {self._short_url(url, 60)}")

            # Парсим страницу
            data = await self._parse_single_parking_page(url)

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

    #
    # Парсинг одной страницы парковки
    #

    async def _parse_single_parking_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Парсинг одной страницы парковки."""
        max_retries = 2

        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    print(f"   🔄 Повторная попытка {attempt}/{max_retries}")
                    await asyncio.sleep(random.uniform(3, 5))

                # Открываем страницу парковки в новом табе
                tab = await self.browser.get(url)

                # Ждем загрузки
                await asyncio.sleep(random.uniform(3, 4))

                # Получаем HTML
                html = await tab.get_content()

                # Парсим данные
                soup = BeautifulSoup(str(html), 'lxml')
                data = self.extract_data(url, soup, str(html))

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

    def extract_data(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """Извлечение данных из страницы 2ГИС."""
        data = {}

        # Базовые поля
        data['Ссылка'] = url
        data['Координаты'] = self.extract_coordinates(url) or ""

        # Название
        title_selectors = [
            'h1',
            '[itemprop="name"]',
            '.firm-card__title',
            '.business-card-title',
            'h1[data-qa="firm-card-header-name"]'
        ]

        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(' ', strip=True)
                if text and len(text) > 2:
                    data['Название объекта'] = text
                    break

        # Адрес
        address_selectors = [
            'address',
            '[itemprop="address"]',
            '.address',
            '.firm-card__address',
            '[data-qa="firm-card-address"]'
        ]

        for selector in address_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(' ', strip=True)
                if text and len(text) > 5:
                    data['Адрес'] = text
                    break

        # Телефоны
        phones = []
        phone_selectors = [
            'a[href^="tel:"]',
            '[data-qa="phone"]',
            '.contact__phone',
            '.phone',
            '.firm-card__phone'
        ]

        for selector in phone_selectors:
            for link in soup.select(selector):
                href = link.get('href', '')
                text = link.get_text(strip=True)

                if href.startswith('tel:'):
                    phone = href.replace('tel:', '').strip()
                elif text:
                    phone = text
                else:
                    continue

                if phone:
                    clean_phone = re.sub(r'[^\d\+\(\)\s\-]', '', phone)
                    clean_phone = ' '.join(clean_phone.split())
                    if clean_phone and clean_phone not in phones:
                        phones.append(clean_phone)

        data['Телефон'] = ', '.join(phones) if phones else ""

        # Сайт
        site_selectors = [
            'a[href^="http://"]:not([href*="2gis.ru"])',
            'a[href^="https://"]:not([href*="2gis.ru"])',
            '.contact__website',
            '.website',
            '[data-qa="website"]'
        ]

        for selector in site_selectors:
            for link in soup.select(selector):
                href = link.get('href', '')
                if href and '2gis.ru' not in href:
                    data['Сайт'] = href
                    break

        # Тип объекта / категория
        type_selectors = [
            '[itemprop="category"]',
            '.category',
            '.firm-card__category',
            '.business-card-category'
        ]

        for selector in type_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    data['Тип объекта'] = text
                    break

        # Время работы
        hours_selectors = [
            '[itemprop="openingHours"]',
            '.working-hours',
            '.schedule',
            '.hours',
            '[data-qa="opening-hours"]'
        ]

        for selector in hours_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(' ', strip=True)
                if ':' in text or 'час' in text.lower() or 'открыт' in text.lower():
                    data['Время работы'] = text
                    break

        # Рейтинг
        rating_selectors = [
            '[itemprop="ratingValue"]',
            '.rating',
            '.business-rating-badge',
            '[data-qa="rating"]'
        ]

        for selector in rating_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    try:
                        rating_match = re.search(r'[\d\.]+', text)
                        if rating_match:
                            data['Оценка'] = rating_match.group(0)
                    except:
                        data['Оценка'] = text
                    break

        # Количество отзывов
        reviews_selectors = [
            '[itemprop="reviewCount"]',
            '.reviews-count',
            '.review-count',
            '[data-qa="reviews-count"]'
        ]

        for selector in reviews_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    reviews_match = re.search(r'\d+', text)
                    if reviews_match:
                        data['Количество оценок'] = reviews_match.group(0)
                    break

        # Тип парковки
        data['Тип парковки'] = self.detect_parking_type(html, data.get('Название объекта', ''))

        # Цены и тарифы
        price_text = soup.get_text(' ', strip=True)
        price_patterns = [
            r'(\d+[\s\u00A0]*руб[лей\.]*)',
            r'(\d+[\s\u00A0]*₽)',
            r'(\d+[\s\u00A0]*р\.)',
            r'(\d+[\s\u00A0]*в час)',
            r'(\d+[\s\u00A0]*в сутки)',
            r'(\d+[\s\u00A0]*в месяц)',
        ]

        prices = []
        for pattern in price_patterns:
            matches = re.findall(pattern, price_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    price = match[0]
                else:
                    price = match

                if price and price not in prices:
                    prices.append(price.strip())

        if prices:
            data['Тарифы'] = '; '.join(prices[:3])
            data['Цены'] = prices[0] if prices else ""

        # Вместимость
        capacity_patterns = [
            r'(\d+)[\s\u00A0]*мест[а-я]*',
            r'(\d+)[\s\u00A0]*машиномест',
            r'вместимость[\s:]*(\d+)',
            r'(\d+)[\s\u00A0]*автомобил[ейя]'
        ]

        for pattern in capacity_patterns:
            match = re.search(pattern, price_text, re.IGNORECASE)
            if match:
                data['Вместимость'] = match.group(1)
                break

        # Описание
        desc_selectors = [
            '.firm-card__description',
            '[itemprop="description"]',
            '.description',
            '.firm-description'
        ]

        for selector in desc_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(' ', strip=True)
                if text and len(text) > 20:
                    data['Описание'] = text[:200]
                    break

        # Название парковки (дублирует название объекта)
        data['Название парковки'] = data.get('Название объекта', 'Парковка')

        # Дополнительная проверка на парковку
        if data['Тип парковки'] == 'неизвестно':
            page_text = soup.get_text(' ', strip=True).lower()
            name_text = data.get('Название объекта', '').lower()

            parking_keywords = ['парковк', 'стоянк', 'parking', 'автостоянк', 'паркинг']
            for keyword in parking_keywords:
                if keyword in page_text or keyword in name_text:
                    data['Тип парковки'] = 'парковка'
                    break

        return data

    def _generate_parking_id(self, url: str) -> str:
        """Генерация уникального ID для парковки."""
        match = re.search(r'/firm/(\d+)', url)
        if match:
            return f"2gis_{match.group(1)}"

        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        return f"2gis_{url_hash}"

    def _remove_duplicates(self):
        """Удаление дубликатов из результатов."""
        if not self.results:
            return

        unique_results = []
        seen_ids = set()

        for item in self.results:
            # Генерируем уникальный ключ
            name = item.get('Название объекта', '').strip()
            address = item.get('Адрес', '').strip()
            url = item.get('Ссылка на объект') or item.get('Ссылка', '')

            if url:
                parking_id = self._generate_parking_id(url)
                key = parking_id
            elif name and address:
                key = f"{name[:30]}_{address[:30]}"
            else:
                data_str = str(item)
                key = hashlib.md5(data_str.encode()).hexdigest()[:12]

            if key not in seen_ids:
                seen_ids.add(key)
                unique_results.append(item)

        removed = len(self.results) - len(unique_results)
        if removed > 0:
            print(f"🗑 Удалено {removed} дубликатов 2ГИС")

        self.results = unique_results

    #
    # Вывод финальной статистики
    #

    def _print_final_stats(self, all_urls: List[str]):
        """Вывод финальной статистики."""
        print("\n" + "=" * 60)
        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА 2ГИС")
        print("=" * 60)

        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        print(f"⏱ Общее время: {minutes} мин {seconds} сек")
        print(f"🔗 Собрано уникальных URL: {len(all_urls)}")
        print(f"✅ Успешно распарсено: {len(self.results)}")

        if all_urls:
            efficiency = len(self.results) / max(1, len(all_urls)) * 100
            print(f"📈 Эффективность парсинга: {efficiency:.1f}%")

        if self.results:
            closed_count = 0
            paid_count = 0

            for item in self.results:
                parking_type = item.get('Тип парковки', '').lower()
                if 'крыт' in parking_type or 'охраня' in parking_type:
                    closed_count += 1
                if 'платн' in parking_type:
                    paid_count += 1

            print(f"\n🚗 Типы парковок 2ГИС:")
            print(f"   Закрытых/охраняемых: {closed_count}")
            print(f"   Платных: {paid_count}")

            if len(self.results) >= 3:
                print(f"\n🏆 Примеры найденных парковок:")
                for i, item in enumerate(self.results[:3], 1):
                    name = item.get('Название объекта', 'Без названия')[:40]
                    address = item.get('Адрес', '')[:50]
                    parking_type = item.get('Тип парковки', 'неизвестно')
                    print(f"   {i}. {name}")
                    print(f"      Адрес: {address}")
                    print(f"      Тип: {parking_type}")

        print("=" * 60)