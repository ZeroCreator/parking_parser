import asyncio
import random
import re
import hashlib
import time
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode

from bs4 import BeautifulSoup
import nodriver

from .base_parser import BaseParser


class TwoGisParser(BaseParser):
    """Парсер 2ГИС с разбиением на зоны."""

    def __init__(self, headless: bool = True):
        super().__init__(headless)
        self.processed_ids: Set[str] = set()
        self.session_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        }

    @property
    def source_name(self) -> str:
        return "2gis"

    async def parse(self, max_pages: int = 30) -> List[Dict[str, Any]]:
        """Основной метод парсинга 2ГИС с разбиением на зоны"""
        print("=" * 60)
        print("🚀 ЗАПУСК ПАРСЕРА 2ГИС (ЗОНИРОВАННЫЙ)")
        print("=" * 60)

        self.start_time = time.time()
        self.results = []

        if not await self.init_browser():
            return []

        try:
            # 1. Генерируем зоны для Санкт-Петербурга
            print(f"\n🎯 ГЕНЕРАЦИЯ ЗОН ДЛЯ САНКТ-ПЕТЕРБУРГА")
            print("-" * 50)

            search_areas = self.generate_grid_z14()
            print(f"✅ Сгенерировано зон: {len(search_areas)}")

            # 2. Парсим все зоны города
            print(f"\n📄 ЭТАП 1: СБОР ВСЕХ ССЫЛОК НА ПАРКОВКИ ПО ЗОНАМ")
            print("-" * 50)

            for i, area in enumerate(search_areas, 1):
                urls_before = len(self.all_urls)

                print(f"\n📍 Зона {i}/{len(search_areas)}: {area['name']}")
                print(f"   Координаты: {area['coords'][1]:.4f}°N, {area['coords'][0]:.4f}°E")
                print(f"   Масштаб: z={area['zoom']}")
                print(f"   URL: {area['url']}")

                # Открываем страницу зоны
                await self._collect_urls_from_zone(
                    area['url'],
                    area['name'],
                    area['coords'],
                    area['zoom']
                )

                new_urls = len(self.all_urls) - urls_before
                print(f"✅ В зоне найдено парковок: {new_urls}")
                print(f"📊 Всего собрано ссылок: {len(self.all_urls)}")

                # Пауза между зонами
                if i < len(search_areas):
                    await asyncio.sleep(random.uniform(5, 8))

            if not self.all_urls:
                print("❌ Не удалось собрать ссылки на парковки")
                return []

            print(f"\n✅ Всего собрано уникальных ссылок: {len(self.all_urls)}")

            # 3. Парсим все собранные парковки
            print("\n🏢 ЭТАП 2: ПАРСИНГ ВСЕХ СОБРАННЫХ ПАРКОВОК")
            print("-" * 50)

            urls_list = list(self.all_urls)
            await self._parse_all_parking_pages(urls_list)

            # 4. Удаляем дубликаты и выводим статистику
            self._remove_duplicates()
            self._print_final_stats(len(self.all_urls))

            return self.results

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
            return self.results
        finally:
            await self.close()

    def generate_grid_z14(self) -> List[Dict[str, Any]]:
        """
        Автоматически генерирует сетку зон для парсинга (z=14).
        Возвращает список URL для поиска, покрывающих весь Санкт-Петербург.
        """
        # Границы Санкт-Петербурга для 2GIS
        LAT_MIN, LAT_MAX = 59.85, 60.05  # Немного расширяем для полного охвата
        LON_MIN, LON_MAX = 30.15, 30.70  # Запад-Восток

        # Шаг сетки для z=14
        LAT_STEP = 0.04  # ~4.4 км
        LON_STEP = 0.06  # ~3.8 км на широте СПб
        ZOOM = 14  # Фиксированный масштаб

        zones = []
        zone_counter = 1

        # Генерируем координаты сетки
        lat = LAT_MIN
        while lat < LAT_MAX:
            lon = LON_MIN
            while lon < LON_MAX:
                # Формируем URL для поиска парковок в этой зоне
                # 2GIS использует параметры m для позиционирования: lon,lat,zoom
                url = f"https://2gis.ru/spb/search/parking/?m={lon:.6f}%2C{lat:.6f}%2F{ZOOM}"

                zones.append({
                    "name": f"Зона {zone_counter}",
                    "url": url,
                    "coords": (lon, lat),
                    "zoom": ZOOM
                })

                zone_counter += 1
                lon += LON_STEP
            lat += LAT_STEP

        print(f"✅ Сгенерировано {len(zones)} зон для парсинга (z={ZOOM})")
        print(f"📐 Шаг сетки: {LON_STEP:.3f}° (долгота) × {LAT_STEP:.3f}° (широта)")
        print(f"📍 Охватываемая область: {LAT_MIN}-{LAT_MAX}°N, {LON_MIN}-{LON_MAX}°E")

        return zones

    async def _collect_urls_from_zone(self, zone_url: str, zone_name: str, coords: tuple, zoom: int) -> bool:
        """Сбор URL парковок из конкретной зоны"""
        try:
            print(f"   🔍 Начинаем сбор ссылок в зоне: {zone_name}")

            # Открываем страницу зоны
            tab = await self.browser.get(zone_url)
            await asyncio.sleep(random.uniform(4, 6))

            # Кликаем по поисковой выдаче, если есть
            await self._click_search_results_if_needed(tab)

            # Собираем ссылки с первой страницы
            print("   📥 Собираем ссылки с первой страницы...")
            initial_urls = await self._get_urls_from_current_page(tab)
            if initial_urls:
                self.all_urls.update(initial_urls)
                print(f"   📊 Первая страница: {len(initial_urls)} URL")
            else:
                print("   ⚠ Не удалось получить ссылки с первой страницы")

            # Прокручиваем страницу
            print("   📜 Начинаем прокрутку страницы...")
            await self._scroll_2gis_to_bottom(tab)

            # Собираем ВСЕ URL после прокрутки
            current_urls = await self._get_urls_from_current_page(tab)
            if current_urls:
                previous_count = len(self.all_urls)
                self.all_urls.update(current_urls)
                new_urls = len(self.all_urls) - previous_count
                print(f"   📎 Всего URL после прокрутки: {len(self.all_urls)} (+{new_urls} новых)")

            # Пробуем найти кнопку пагинации (передаем параметры зоны)
            print("   🔍 Пробуем найти кнопку пагинации после прокрутки...")
            await self._try_find_2gis_pagination_after_scroll(
                tab,
                coords,
                zoom,
                current_page=1
            )

            print(f"   ✅ Сбор ссылок в зоне {zone_name} завершен")
            return True

        except Exception as e:
            print(f"   ❌ Ошибка сбора в зоне {zone_name}: {str(e)[:100]}")
            return False

    async def _click_search_results_if_needed(self, tab):
        """Кликает по результатам поиска, если они есть"""
        try:
            # Ждем загрузки результатов
            await asyncio.sleep(2)

            # Пробуем найти кнопку или элемент с результатами
            selectors = [
                '.searchResults',
                '.listContainer',
                '.searchResults__list',
                '.searchResults__container',
                '[data-qa="search-results"]',
                '.searchTab__content'
            ]

            for selector in selectors:
                element = await tab.query_selector(selector)
                if element:
                    print("   🖱 Найден контейнер результатов, кликаем...")
                    await element.click()
                    await asyncio.sleep(2)
                    break

            # Также пробуем кликнуть по первой карточке
            first_card = await tab.query_selector('.minicard')
            if first_card:
                await first_card.click()
                await asyncio.sleep(1)

        except Exception as e:
            print(f"   ⚠ Не удалось кликнуть по результатам: {str(e)[:50]}")

    async def _scroll_2gis_to_bottom(self, tab):
        """Прокручивает ВСЕ скроллируемые контейнеры на странице 2ГИС"""
        print("   📜 СКРОЛЛИМ ВСЕ КОНТЕЙНЕРЫ...")

        try:
            # 1. Сначала пробуем прокрутить основной контейнер с результатами
            await tab.evaluate("""
                (function() {
                    const mainContainers = [
                        '.searchResults__list',
                        '.listContainer',
                        '.searchResults__container',
                        '.scroll__container',
                        '[data-scroll]'
                    ];

                    for (const selector of mainContainers) {
                        const container = document.querySelector(selector);
                        if (container && container.scrollHeight > container.clientHeight) {
                            container.scrollTop = container.scrollHeight;
                            return { scrolled: true, selector: selector };
                        }
                    }
                    return { scrolled: false };
                })()
            """)
            await asyncio.sleep(random.uniform(1, 2))

            # 2. Прокручиваем окно браузера
            await tab.evaluate("""
                window.scrollBy({
                    top: 800,
                    behavior: 'smooth'
                });
            """)
            await asyncio.sleep(random.uniform(1, 2))

            # 3. Прокручиваем все скроллируемые контейнеры
            container_count = await tab.evaluate("""
                document.querySelectorAll('[data-scroll], [tabindex], [overflow="auto"], [overflow="scroll"]').length
            """)

            for i in range(container_count):
                await tab.evaluate(f"""
                    (function() {{
                        const containers = document.querySelectorAll('[data-scroll], [tabindex], [overflow="auto"], [overflow="scroll"]');
                        if (containers[{i}]) {{
                            const container = containers[{i}];
                            if (container.scrollHeight > container.clientHeight) {{
                                container.scrollTop = container.scrollHeight;
                            }}
                        }}
                    }})()
                """)
                await asyncio.sleep(0.3)

            await asyncio.sleep(random.uniform(2, 3))

        except Exception as e:
            print(f"   ❌ Ошибка скроллинга: {str(e)[:100]}")

    async def _try_find_2gis_pagination_after_scroll(self, tab, coords: tuple, zoom: int, current_page: int = 1):
        """Попытка найти кнопки пагинации после прокрутки с сохранением параметров зоны"""
        try:
            html = await tab.get_content()
            soup = BeautifulSoup(html, 'lxml')

            next_page_num = current_page + 1
            found_next_page = False

            # Ищем ссылку на следующую страницу
            for link in soup.find_all('a', href=True):
                href = link['href']

                # Паттерны пагинации 2GIS
                patterns = [
                    r'/page/(\d+)',
                    r'page=(\d+)',
                    r'pagination=(\d+)'
                ]

                for pattern in patterns:
                    match = re.search(pattern, href)
                    if match:
                        page_num = int(match.group(1))
                        if page_num == next_page_num:
                            print(f"   🖱 Переходим на страницу {next_page_num}")

                            # ВАЖНО: Формируем URL для следующей страницы с ПАРАМЕТРАМИ ЗОНЫ
                            lon, lat = coords
                            base_url = f"https://2gis.ru/spb/search/parking/"

                            # Добавляем параметры зоны (координаты и масштаб)
                            params = {
                                'm': f"{lon:.6f},{lat:.6f}/{zoom}"
                            }

                            # Определяем формат пагинации и добавляем номер страницы
                            if '/page/' in href:
                                # Формат: /page/2/
                                page_url = f"{base_url}page/{next_page_num}/?{urlencode(params)}"
                            else:
                                # Формат: ?page=2
                                params['page'] = next_page_num
                                page_url = f"{base_url}?{urlencode(params)}"

                            print(f"   📍 URL с параметрами зоны: {page_url}")

                            # Переходим на следующую страницу
                            await tab.get(page_url)
                            await asyncio.sleep(random.uniform(4, 6))

                            # Прокручиваем новую страницу
                            await self._scroll_2gis_to_bottom(tab)

                            # Собираем URL с новой страницы
                            urls_page = await self._get_urls_from_current_page(tab)
                            if urls_page:
                                before = len(self.all_urls)
                                self.all_urls.update(urls_page)
                                new_count = len(self.all_urls) - before
                                print(f"   📊 +{new_count} новых URL")

                            # Рекурсивно ищем следующую страницу
                            await self._try_find_2gis_pagination_after_scroll(
                                tab,
                                coords,
                                zoom,
                                next_page_num
                            )
                            found_next_page = True
                            break

                if found_next_page:
                    break

            if not found_next_page:
                # Также проверяем кнопки "Дальше" или "Следующая"
                next_buttons = soup.find_all(['button', 'a'], string=re.compile(r'дальше|следующ|next', re.I))

                for button in next_buttons:
                    print(f"   🖱 Найдена кнопка 'Дальше', пробуем перейти на страницу {next_page_num}")

                    # Формируем URL для следующей страницы с параметрами зоны
                    lon, lat = coords
                    base_url = f"https://2gis.ru/spb/search/parking/"

                    # Пробуем разные форматы
                    formats_to_try = [
                        f"{base_url}page/{next_page_num}/?m={lon:.6f}%2C{lat:.6f}%2F{zoom}",
                        f"{base_url}?page={next_page_num}&m={lon:.6f}%2C{lat:.6f}%2F{zoom}"
                    ]

                    for page_url in formats_to_try:
                        try:
                            await tab.get(page_url)
                            await asyncio.sleep(random.uniform(4, 6))

                            # Проверяем, загрузилась ли страница
                            current_url = await tab.evaluate("window.location.href")
                            if "parking" in current_url:
                                print(f"   ✅ Успешно перешли на страницу {next_page_num}")

                                # Прокручиваем новую страницу
                                await self._scroll_2gis_to_bottom(tab)

                                # Собираем URL с новой страницы
                                urls_page = await self._get_urls_from_current_page(tab)
                                if urls_page:
                                    before = len(self.all_urls)
                                    self.all_urls.update(urls_page)
                                    new_count = len(self.all_urls) - before
                                    print(f"   📊 +{new_count} новых URL")

                                # Рекурсивно ищем следующую страницу
                                await self._try_find_2gis_pagination_after_scroll(
                                    tab,
                                    coords,
                                    zoom,
                                    next_page_num
                                )
                                found_next_page = True
                                break

                        except Exception as e:
                            print(f"   ⚠ Ошибка при переходе по URL {page_url}: {str(e)[:50]}")
                            continue

                    if found_next_page:
                        break

            if not found_next_page:
                print(f"   ⚠ Нет больше страниц в этой зоне (достигнута страница {current_page})")

        except Exception as e:
            print(f"   ❌ Ошибка пагинации: {str(e)[:60]}")

    async def _get_urls_from_current_page(self, tab) -> Set[str]:
        """Получение URL парковок с текущей страницы (2ГИС)"""
        try:
            await asyncio.sleep(1)
            html = await tab.get_content()

            urls = self._extract_2gis_urls_from_html(html)

            filtered_urls = set()
            for url in urls:
                if self._is_valid_2gis_url(url):
                    clean_url = self._clean_2gis_url(url)
                    if clean_url:
                        filtered_urls.add(clean_url)

            return filtered_urls

        except Exception as e:
            print(f"   ❌ Ошибка при извлечении URL: {str(e)[:50]}")
            return set()

    def _extract_2gis_urls_from_html(self, html: str) -> List[str]:
        """Извлечение URL парковок из HTML страницы поиска (2ГИС)"""
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

                clean_url = self._clean_2gis_url(full_url)
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

        # Ищем в мини-карточках
        minicards = soup.select('.minicard')
        for card in minicards:
            link = card.select_one('a[href*="/firm/"]')
            if link:
                href = link.get('href', '')
                if href:
                    if href.startswith('//'):
                        full_url = f"https:{href}"
                    elif href.startswith('/'):
                        full_url = f"https://2gis.ru{href}"
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue

                    clean_url = self._clean_2gis_url(full_url)
                    if clean_url and clean_url not in urls:
                        urls.append(clean_url)

        return list(set(urls))

    def _is_valid_2gis_url(self, url: str) -> bool:
        """Проверка валидности URL парковки (2ГИС)"""
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
            '#',
            'reviewTab',
            'photoTab'
        ]

        for pattern in exclude_patterns:
            if pattern in url:
                return False

        return True

    def _clean_2gis_url(self, url: str) -> str:
        """Очистка URL парковки (2ГИС)"""
        url = url.split('?')[0].split('#')[0]
        url = url.rstrip('/')

        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/'):
            url = f"https://2gis.ru{url}"
        elif not url.startswith('http'):
            url = f"https://{url}"

        return url

    def _extract_page_data(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """Извлечение данных из страницы 2ГИС"""
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

        # Тип объекта
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

    def extract_coordinates(self, url: str) -> Optional[str]:
        """Извлечение координат из URL (2ГИС)"""
        patterns = [
            r'@([\d\.]+),([\d\.]+)',
            r'll=([\d\.]+)%2C([\d\.]+)',
            r'/([\d\.]+)%2C([\d\.]+)/',
            r'm=([\d\.]+)%2C([\d\.]+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                lon, lat = match.groups()
                return f"{lon},{lat}"

        return None

    def detect_parking_type(self, html: str, name: str = "") -> str:
        """Определение типа парковки (2ГИС)"""
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

    def _generate_parking_id(self, url: str) -> str:
        """Генерация уникального ID для парковки"""
        match = re.search(r'/firm/(\d+)', url)
        if match:
            return f"2gis_{match.group(1)}"

        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        return f"2gis_{url_hash}"

    def _print_final_stats(self, total_urls: int):
        """Вывод статистики сбора"""
        print("\n" + "=" * 80)
        print("📊 СТАТИСТИКА СБОРА 2ГИС")
        print("=" * 80)

        elapsed_time = time.time() - self.start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print(f"⏱ Время выполнения: {minutes} мин {seconds} сек")
        print(f"🔗 Всего найдено ссылок: {total_urls}")
        print(f"✅ Успешно спарсено: {len(self.results)}")

        # Статистика по данным
        phones_count = sum(1 for r in self.results if r.get('Телефон'))
        sites_count = sum(1 for r in self.results if r.get('Сайт'))
        coords_count = sum(1 for r in self.results if r.get('Координаты'))
        prices_count = sum(1 for r in self.results if r.get('Цены'))

        print(f"📞 Парковок с телефоном: {phones_count}")
        print(f"🌐 Парковок с сайтом: {sites_count}")
        print(f"📍 Парковок с координатами: {coords_count}")
        print(f"💰 Парковок с ценами: {prices_count}")

        # Типы парковок
        types = {}
        for r in self.results:
            parking_type = r.get('Тип парковки', 'неизвестно')
            types[parking_type] = types.get(parking_type, 0) + 1

        print("\n🏢 ТИПЫ ПАРКОВОК:")
        for type_name, count in sorted(types.items(), key=lambda x: x[1], reverse=True):
            print(f"   {type_name}: {count}")

        print("\n" + "=" * 80)