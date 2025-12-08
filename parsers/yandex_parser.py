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
        print("=" * 60)
        print("🚀 ЗАПУСК ПАРСЕРА ЯНДЕКС КАРТ")
        print("=" * 60)

        self.start_time = time.time()
        self.results = []
        self.all_urls.clear()

        if not await self.init_browser():
            return []

        try:
            # 1. Загружаем страницу поиска
            print("\n📄 ЗАГРУЗКА СТРАНИЦЫ ПОИСКА")
            print("-" * 50)

            search_url = "https://yandex.ru/maps/2/saint-petersburg/search/парковки/"
            print(f"🔗 URL: {search_url}")

            page = await self.browser.get(search_url)
            await asyncio.sleep(3)

            # Кликаем кнопку "Показать результаты", если есть
            button = await page.query_selector('span.search-command-view__show-results-button')
            if button:
                print("✅ Кнопка найдена, кликаем...")
                await button.click()
                await asyncio.sleep(3)
                print("✅ Результаты загружены")

            # 2. Собираем ссылки
            print("\n🔗 СБОР ССЫЛОК СО СКРОЛЛИНГОМ")
            print("-" * 50)

            await self._scroll_and_collect_urls(page)

            if not self.all_urls:
                print("❌ Не удалось собрать ссылки")
                return []

            print(f"✅ Собрано уникальных ссылок: {len(self.all_urls)}")

            # 3. Парсим парковки
            print("\n🏢 ДЕТАЛЬНЫЙ ПАРСИНГ ПАРКОВОК")
            print("-" * 50)

            urls_to_parse = list(self.all_urls)
            print(f"📊 Будем парсить {len(urls_to_parse)} парковок")

            await self._parse_all_parking_pages(urls_to_parse)

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

    async def _scroll_and_collect_urls(self, page):
        """Скроллинг и сбор ссылок (специфично для Яндекс)"""
        print("Начинаем скроллинг для загрузки всех парковок...")

        max_scroll_attempts = 100
        consecutive_no_new = 0
        total_new_urls = 0

        for attempt in range(1, max_scroll_attempts + 1):
            print(f"\n   🔄 Попытка скроллинга {attempt}/{max_scroll_attempts}")

            before_scroll_count = len(self.all_urls)

            # Скроллинг для Яндекс
            await self._yandex_specific_scroll(page)

            # Ждем загрузки
            await asyncio.sleep(random.uniform(2, 3))

            # Получаем обновленный HTML и извлекаем URL
            html_content = await page.evaluate("document.documentElement.outerHTML")
            new_urls_count_before = len(self.all_urls)
            self._extract_urls_from_html(html_content)
            new_urls_added = len(self.all_urls) - new_urls_count_before

            print(f"   📊 URL до: {before_scroll_count}, добавлено: {new_urls_added}, всего: {len(self.all_urls)}")

            if new_urls_added > 0:
                consecutive_no_new = 0
                total_new_urls += new_urls_added
            else:
                consecutive_no_new += 1
                if consecutive_no_new >= self.max_consecutive_no_new:
                    print(f"   🏁 Прекращаем скроллинг - {self.max_consecutive_no_new} раза подряд нет новых URL")
                    break

            await asyncio.sleep(random.uniform(0.5, 1.5))

        print(f"\n✅ Скроллинг завершен")
        print(f"📊 Всего добавлено новых URL: {total_new_urls}")

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

    def _extract_urls_from_html(self, html_content: str):
        """Извлечение ссылок на парковки из HTML (специфично для Яндекс)"""
        try:
            urls_before = len(self.all_urls)

            # Ищем ссылки на организации
            org_pattern = r'href="(/maps/org/[^"]+)"'
            all_link_matches = re.findall(org_pattern, html_content)

            for link in all_link_matches:
                full_url = f"https://yandex.ru{link}"
                clean_url = full_url.split('?')[0].split('#')[0]
                # Фильтруем системные ссылки
                if not any(exclude in clean_url.lower() for exclude in ['/reviews/', '/photos/', '/gallery/']):
                    self.all_urls.add(clean_url)

            # Ищем в карточках
            snippet_pattern = r'<li[^>]*class="[^"]*search-snippet-view[^"]*"[^>]*>.*?</li>'
            snippets = re.findall(snippet_pattern, html_content, re.DOTALL)

            for snippet in snippets:
                link_match = re.search(org_pattern, snippet)
                if link_match:
                    link = link_match.group(1)
                    full_url = f"https://yandex.ru{link}"
                    clean_url = full_url.split('?')[0].split('#')[0]
                    if not any(exclude in clean_url.lower() for exclude in ['/reviews/', '/photos/', '/gallery/']):
                        self.all_urls.add(clean_url)

            new_urls = len(self.all_urls) - urls_before
            if new_urls > 0:
                print(f"   ✅ Извлечено {new_urls} новых URL")

        except Exception as e:
            print(f"❌ Ошибка извлечения URL: {e}")

    # ВАЖНО: переименован метод с _extract_parking_data на _extract_page_data
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
