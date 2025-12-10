import asyncio
import random
import re
import time
from typing import List, Dict, Any, Optional, Set
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, parse_qs

from .base_parser import BaseParser


class YandexParser(BaseParser):
    """Парсер Яндекс Карт для поиска парковок в Санкт-Петербурге"""

    @property
    def source_name(self) -> str:
        return "yandex"

    async def parse(self) -> List[Dict[str, Any]]:
        """Основной метод парсинга Яндекс Карт"""
        print("=" * 80)
        print("🚀 ПАРСЕР ЯНДЕКС КАРТ - САНКТ-ПЕТЕРБУРГ")
        print("=" * 80)

        self.start_time = time.time()
        self.results = []
        self.all_urls.clear()

        if not await self.init_browser():
            return []

        try:
            # Используем автоматическую сетку вместо ручного списка
            search_areas = self.generate_grid_z14()

            # 1. Парсим все области города
            print(f"\n🎯 НАЧИНАЕМ ПАРСИНГ {len(search_areas)} АВТОЗОН (z=14)...")

            for i, area in enumerate(search_areas, 1):
                urls_before = len(self.all_urls)

                print(f"\n📍 Зона {i}/{len(search_areas)}: {area['name']}")
                print(f"   Координаты: {area['coords'][1]:.4f}°N, {area['coords'][0]:.4f}°E")
                print(f"   URL: {area['url']}")

                page = await self.browser.get(area['url'])
                await asyncio.sleep(4)

                # Кликаем кнопку "Показать результаты", если есть
                button = await page.query_selector('span.search-command-view__show-results-button')
                if button:
                    print("✅ Кнопка найдена, кликаем...")
                    await button.click()
                    await asyncio.sleep(3)
                    print("✅ Результаты загружены")

                # Скрапим эту область
                await self._scroll_and_collect_urls(page)

                new_urls = len(self.all_urls) - urls_before
                print(f"✅ В области найдено парковок: {new_urls}")
                print(f"\n✅ Всего собрано ссылок на парковки: {len(self.all_urls)}")

                # Пауза между областями
                if i < len(search_areas):
                    await asyncio.sleep(random.uniform(5, 8))

            if not self.all_urls:
                print("❌ Не удалось собрать ссылки")
                return []

            print(f"\n✅ Всего собрано ссылок на парковки: {len(self.all_urls)}")

            # 2. Парсим каждую парковку
            print("\n🏢 ПАРСИМ ДАННЫЕ ПАРКОВОК...")
            urls_list = list(self.all_urls)

            for i, url in enumerate(urls_list, 1):
                print(f"   {i}/{len(urls_list)}: {url}")
                data = await self.parse_parking_page(url)
                if data:
                    self.results.append(data)
                    print(f"      ✅ Получены данные: {data.get('Название парковки', 'Без названия')}")
                else:
                    print(f"      ⚠ Не удалось получить данные")

                # Задержка между запросами
                if i < len(urls_list):
                    await asyncio.sleep(random.uniform(3, 5))

            # 3. Удаляем дубликаты
            self._remove_duplicates()

            # 4. Выводим статистику
            self._print_final_stats(len(self.all_urls))

            return self.results

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
            return self.results
        finally:
            await self.close()

    def generate_grid_z14(self) -> List[Dict[str, str]]:
        """
        Автоматически генерирует сетку зон для парсинга (z=14).
        Возвращает список URL для поиска, покрывающих весь Санкт-Петербург.
        """
        # 1. Определяем географические границы Санкт-Петербурга
        # (широта lat, долгота lon)
        # Эти значения можно немного расширить для полного охвата
        LAT_MIN, LAT_MAX = 59.80, 60.05  # Север-Юг
        LON_MIN, LON_MAX = 29.60, 30.70  # Запад-Восток

        # 2. Рассчитываем шаг сетки для z=14
        # При z=14 sspn ~0.04-0.05 градуса, делаем шаг немного меньше для перекрытия
        # Это обеспечит полный охват без пропусков
        LAT_STEP = 0.04  # ~4.4 км
        LON_STEP = 0.06  # ~3.8 км на широте СПб

        zones = []
        zone_counter = 1

        # 3. Генерируем координаты сетки
        lat = LAT_MIN
        while lat < LAT_MAX:
            lon = LON_MIN
            while lon < LON_MAX:
                # Формируем URL для поиска парковок в этой зоне
                url = (f"https://yandex.ru/maps/2/saint-petersburg/search/парковки/"
                       f"?l=carparks&ll={lon:.6f}%2C{lat:.6f}&z=14")

                zones.append({
                    "name": f"Автозона {zone_counter}",
                    "url": url,
                    "coords": (lon, lat)  # Для отладки
                })

                zone_counter += 1
                lon += LON_STEP
            lat += LAT_STEP

        print(f"✅ Сгенерировано {len(zones)} зон для парсинга (z=14)")
        print(f"📐 Шаг сетки: {LON_STEP:.3f}° (долгота) × {LAT_STEP:.3f}° (широта)")
        print(f"📍 Охватываемая область: {LAT_MIN}-{LAT_MAX}°N, {LON_MIN}-{LON_MAX}°E")

        return zones

    async def _scroll_and_collect_urls(self, page):
        """Скроллинг и сбор ссылок для конкретной области"""
        max_scrolls = 30
        no_new_count = 0
        previous_count = len(self.all_urls)

        for scroll_num in range(1, max_scrolls + 1):
            print(f"   📍 Скролл {scroll_num}/{max_scrolls}")

            # Сохраняем текущее количество
            current_count_before = len(self.all_urls)

            # Выполняем скроллинг для Яндекс
            await self._yandex_specific_scroll(page)
            await asyncio.sleep(random.uniform(1.5, 2.5))

            # Собираем ссылки
            html_content = await page.evaluate("document.documentElement.outerHTML")
            urls_before = len(self.all_urls)
            self._extract_urls_from_html(html_content)
            new_urls = len(self.all_urls) - urls_before

            if new_urls > 0:
                print(f"   📥 Новых парковок: {new_urls}")
                no_new_count = 0
            else:
                no_new_count += 1
                print(f"   📭 Новых парковок нет ({no_new_count}/3)")

                if no_new_count >= 3:
                    print("   🏁 Завершаем скроллинг этой области")
                    break

            # Если количество ссылок не меняется 3 раза подряд - выходим
            if len(self.all_urls) == previous_count:
                no_new_count += 1
            else:
                no_new_count = 0

            previous_count = len(self.all_urls)

            # Короткая пауза
            await asyncio.sleep(random.uniform(0.5, 1))

    async def _yandex_specific_scroll(self, page):
        """Специфичный скроллинг для Яндекс.Карт"""
        try:
            await page.evaluate("""
                (function() {
                    const selectors = [
                        '.scroll__container_width_narrow',
                        '.scroll__container',
                        '.sidebar-view__panel',
                        '.search-list-view__list-container',
                        '.search-list-view__list'
                    ];

                    let scrolled = false;
                    for (const selector of selectors) {
                        const container = document.querySelector(selector);
                        if (container && container.scrollHeight > container.clientHeight) {
                            container.scrollTop = container.scrollHeight;
                            scrolled = true;
                            break;
                        }
                    }

                    window.scrollBy({
                        top: 800,
                        behavior: 'smooth'
                    });

                    return { containerScrolled: scrolled };
                })();
            """)
        except Exception as e:
            print(f"   ⚠ Ошибка скроллинга: {e}")

    def _normalize_url(self, url: str) -> str:
        """Нормализация URL - оставляем только базовую ссылку на парковку"""
        if not url:
            return ""

        # Список вкладок, которые нужно обрезать
        tabs_to_remove = ['/reviews', '/photos', '/gallery', '/menu']

        # Добавляем домен если нужно
        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/'):
            url = f"https://yandex.ru{url}"
        elif not url.startswith('http'):
            return ""

        # Удаляем параметры запроса и якоря
        url = url.split('?')[0].split('#')[0].strip()

        # Обрезаем вкладки (reviews, photos, gallery, menu)
        for tab in tabs_to_remove:
            if tab in url:
                # Находим позицию вкладки и обрезаем до неё
                tab_index = url.find(tab)
                if tab_index != -1:
                    url = url[:tab_index]

        # Удаляем конечные слеши
        url = url.rstrip('/')

        return url

    def _extract_urls_from_html(self, html_content: str):
        """Извлечение ссылок на парковки из HTML"""
        try:
            urls_before = len(self.all_urls)

            # Ищем ссылки на организации
            org_pattern = r'href="(/maps/org/[^"]+)"'
            all_link_matches = re.findall(org_pattern, html_content)

            for link in all_link_matches:
                full_url = f"https://yandex.ru{link}"
                clean_url = self._normalize_url(full_url)
                if clean_url:
                    # Фильтруем системные ссылки
                    if not any(exclude in clean_url.lower() for exclude in ['/reviews/', '/photos/', '/gallery/', '/menu/']):
                        self.all_urls.add(clean_url)

            # Ищем в карточках
            snippet_pattern = r'<li[^>]*class="[^"]*search-snippet-view[^"]*"[^>]*>.*?</li>'
            snippets = re.findall(snippet_pattern, html_content, re.DOTALL)

            for snippet in snippets:
                link_match = re.search(org_pattern, snippet)
                if link_match:
                    link = link_match.group(1)
                    full_url = f"https://yandex.ru{link}"
                    clean_url = self._normalize_url(full_url)
                    if clean_url and not any(exclude in clean_url.lower() for exclude in ['/reviews/', '/photos/', '/gallery/', '/menu/']):
                        self.all_urls.add(clean_url)

            new_urls = len(self.all_urls) - urls_before
            if new_urls > 0:
                print(f"   📥 Извлечено {new_urls} новых URL")

        except Exception as e:
            print(f"❌ Ошибка извлечения URL: {e}")

    async def parse_parking_page(self, url: str) -> Dict[str, Any]:
        """Парсинг страницы парковки"""
        try:
            print(f"      📖 Открываем страницу парковки...")
            page = await self.browser.get(url)
            await asyncio.sleep(random.uniform(3, 4))

            # Получаем HTML
            html_content = await page.evaluate("document.documentElement.outerHTML")
            soup = BeautifulSoup(html_content, 'html.parser')

            # Парсим данные
            data = self._extract_page_data(url, soup, html_content)

            # Проверяем, что парковка в Санкт-Петербурге
            address = data.get('Адрес', '')
            if address:
                address_lower = address.lower()
                spb_patterns = [
                    'санкт-петербург',
                    'спб',
                    'г.санкт-петербург',
                    'г. спб',
                    'ленинград',
                    'г.ленинград'
                ]

                is_spb = any(pattern in address_lower for pattern in spb_patterns)
                if not is_spb:
                    print(f"      🚫 Пропускаем парковку (не из Санкт-Петербурга): {address}")
                    return None

            return data if data else None

        except Exception as e:
            print(f"      ❌ Ошибка парсинга: {e}")
            return None

    def _extract_page_data(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """Извлечение данных со страницы парковки (специфично для Яндекс)"""
        data = {
            'source': 'yandex',
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Ссылка': url,
            'Ссылка на парковку': url
        }

        # 1. Название
        title_selectors = [
            'h1',
            '.orgpage-header-view__header',
            '.business-title',
            '.card-title-view__title',
            '[itemprop="name"]'
        ]

        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_text(elem.get_text())
                if text:
                    data['Название объекта'] = text
                    data['Название парковки'] = text
                    break

        # 2. Адрес
        address_selectors = [
            '[itemprop="address"]',
            '.business-contacts-view__address',
            '.card-address-view__address',
            '.orgpage-address-view__address-text',
            'address'
        ]

        for selector in address_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_text(elem.get_text())
                if text:
                    data['Адрес'] = text
                    data['Адрес парковки'] = text
                    break

        # 3. Координаты
        coords = self._extract_yandex_coordinates(url, soup)
        if coords:
            data['Координаты'] = coords

        # 4. Телефон
        phones = []
        phone_links = soup.find_all('a', href=re.compile(r'^tel:'))
        for link in phone_links:
            phone = link.get('href', '').replace('tel:', '').strip()
            if phone:
                clean_phone = re.sub(r'[^\d+]', '', phone)
                if clean_phone and clean_phone not in phones:
                    phones.append(clean_phone)

        if phones:
            data['Телефон'] = ', '.join(phones)

        # 5. Сайт
        site_selectors = [
            '.business-urls-view__link',
            '.card-website-view__link',
            '.orgpage-url-view__url'
        ]

        for selector in site_selectors:
            elem = soup.select_one(selector)
            if elem:
                href = elem.get('href', '')
                if href and 'yandex.ru' not in href:
                    data['Сайт'] = href
                    break

        # 6. Тип объекта
        category_selectors = [
            '.business-categories-view__category',
            '.card-categories-view__category',
            '.orgpage-categories-view__category',
            '[itemprop="category"]'
        ]

        for selector in category_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_text(elem.get_text())
                if text:
                    data['Тип объекта'] = text
                    break

        # 7. Время работы
        hours_selectors = [
            '.business-feature-view__schedule-days',
            '.card-schedule-view__schedule',
            '.orgpage-working-view__working-days',
            '[itemprop="openingHours"]'
        ]

        for selector in hours_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_text(elem.get_text())
                if text and (':' in text or 'час' in text.lower()):
                    data['Время работы'] = text
                    break

        # 8. Рейтинг и оценки
        rating_selectors = [
            '.business-rating-badge-view__rating-text',
            '.card-rating-view__rating',
            '.orgpage-rating-view__rating',
            '[itemprop="ratingValue"]'
        ]

        for selector in rating_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_text(elem.get_text())
                if text:
                    data['Оценка'] = text
                    break

        reviews_selectors = [
            '.business-rating-badge-view__rating-count',
            '.card-rating-view__reviews-count',
            '.orgpage-reviews-view__reviews-count',
            '[itemprop="reviewCount"]'
        ]

        for selector in reviews_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_text(elem.get_text())
                if text:
                    data['Количество оценок'] = text
                    break

        # 9. Тип парковки
        parking_type = self._detect_yandex_parking_type(soup, data.get('Название объекта', ''), html)
        data['Тип парковки'] = parking_type

        # Определяем доступ
        if 'закрыт' in parking_type.lower() or 'охраня' in parking_type.lower():
            data['Доступ'] = 'Закрытый'
        else:
            data['Доступ'] = 'Открытый'

        # 10. Цены
        page_text = soup.get_text()
        price_matches = re.findall(r'(\d+\s*руб|\d+\s*₽|\d+\s*в час|\d+\s*в сутки)', page_text, re.IGNORECASE)
        if price_matches:
            data['Цены'] = price_matches[0]
            data['Тарифы'] = '; '.join(price_matches[:3])

        # 11. Вместимость
        capacity_match = re.search(r'(\d+)\s*мест|\bвместимость\s*(\d+)', page_text, re.IGNORECASE)
        if capacity_match:
            capacity = capacity_match.group(1) or capacity_match.group(2)
            data['Вместимость'] = capacity

        # 12. Описание
        desc_selectors = [
            '.business-description-view__description',
            '.card-description-view__description',
            '.orgpage-description-view__description',
            '[itemprop="description"]'
        ]

        for selector in desc_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_text(elem.get_text())
                if text:
                    data['Описание'] = text[:500]
                    break

        return data

    def _extract_yandex_coordinates(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение координат для Яндекс"""
        # Из мета-тегов
        meta_coords = soup.find('meta', attrs={'name': 'coordinates'})
        if meta_coords:
            coords = meta_coords.get('content', '')
            if coords:
                return coords

        # Из URL
        parsed = urlparse(url)
        if 'll=' in url:
            params = parse_qs(parsed.query)
            if 'll' in params:
                ll_parts = params['ll'][0].split('%2C')
                if len(ll_parts) == 2:
                    # Яндекс: долгота,широта -> меняем на широта,долгота
                    return f"{ll_parts[1]},{ll_parts[0]}"

        # Из data-атрибутов
        coord_elem = soup.find(attrs={'data-coordinates': True})
        if coord_elem:
            coords = coord_elem.get('data-coordinates')
            if coords:
                return coords

        return None

    def _detect_yandex_parking_type(self, soup: BeautifulSoup, name: str, html: str) -> str:
        """Определение типа парковки для Яндекс"""
        text = (name + ' ' + soup.get_text()).lower()
        type_info = []

        # Проверка платности
        if any(word in text for word in ['платн', 'оплат', 'тариф', 'цена', '₽', 'руб']):
            type_info.append('платная')
        elif any(word in text for word in ['бесплатн', 'free', 'gratis']):
            type_info.append('бесплатная')

        # Проверка типа
        if any(word in text for word in ['крыт', 'закрыт', 'охраня', 'подземн']):
            type_info.append('крытая')
            type_info.append('охраняемая')
        elif any(word in text for word in ['уличн', 'открыт', 'гост']):
            type_info.append('уличная')

        # Дополнительные признаки
        if any(word in text for word in ['торгов', 'тц', 'молл', 'галерея']):
            type_info.append('при тц')
        elif any(word in text for word in ['офиc', 'бизнес', 'центр']):
            type_info.append('бизнес-центр')

        return ", ".join(type_info) if type_info else "неизвестно"

    def _remove_duplicates(self):
        """Удаление дубликатов по уникальному ID"""
        if not self.results:
            return

        unique_results = []
        seen = set()

        for item in self.results:
            # Создаем ключ на основе нормализованной ссылки
            url = item.get('Ссылка', '').lower().strip()

            if url:
                # Извлекаем уникальный ID из URL
                # Паттерн для поиска ID: /org/название/ID/
                match = re.search(r'/(\d+)(?:/|$)', url)
                if match:
                    unique_id = match.group(1)
                    if unique_id not in seen:
                        seen.add(unique_id)
                        unique_results.append(item)
                elif url not in seen:
                    seen.add(url)
                    unique_results.append(item)
            else:
                # Если нет URL, используем название и адрес
                name = item.get('Название парковки', '').lower().strip()
                address = item.get('Адрес', '').lower().strip()

                if name and address:
                    key = f"{name}|{address}"
                    if key not in seen:
                        seen.add(key)
                        unique_results.append(item)
                elif name:
                    if name not in seen:
                        seen.add(name)
                        unique_results.append(item)
                elif address:
                    if address not in seen:
                        seen.add(address)
                        unique_results.append(item)
                else:
                    unique_results.append(item)

        removed = len(self.results) - len(unique_results)
        if removed > 0:
            print(f"🗑 Удалено дубликатов: {removed}")

        self.results = unique_results

    def _print_final_stats(self, total_urls: int):
        """Вывод статистики сбора"""
        print("\n" + "=" * 80)
        print("📊 СТАТИСТИКА СБОРА")
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

    def _clean_text(self, text: str) -> str:
        """Очистка текста от лишних пробелов и символов"""
        if not text:
            return ""
        # Удаляем лишние пробелы, переносы строк
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text
