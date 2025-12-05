import asyncio
import random
import re
import time
import json
from typing import List, Dict, Any, Optional, Set
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, parse_qs

from .base_parser import BaseParser


class YandexParser(BaseParser):
    """Парсер Яндекс Карт для поиска парковок в Санкт-Петербурге"""

    def __init__(self, headless: bool = True):
        super().__init__(headless)
        self.start_time = None
        self.all_parking_urls: Set[str] = set()
        self.max_consecutive_no_new = 3  # Максимум 3 попытки без новых URL

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
        self.all_parking_urls.clear()

        if not await self.init_browser():
            return []

        try:
            # 1. Загружаем страницу поиска и кликаем кнопку
            print("\n📄 ЗАГРУЗКА СТРАНИЦЫ И КЛИК КНОПКИ")
            print("-" * 50)

            search_url = "https://yandex.ru/maps/2/saint-petersburg/search/парковки/"
            print(f"🔗 URL: {search_url}")

            page = await self.browser.get(search_url)
            await asyncio.sleep(3)

            # Кликаем кнопку "Показать результаты"
            button = await page.query_selector('span.search-command-view__show-results-button')
            if button:
                print("✅ Кнопка найдена, кликаем...")
                await button.click()
                await asyncio.sleep(3)
                print("✅ Результаты загружены")
            else:
                print("⚠️ Кнопка не найдена, проверяем есть ли результаты...")

            # 2. Ждем загрузки данных
            print("\n⏱ ОЖИДАНИЕ ЗАГРУЗКИ ДАННЫХ")
            print("-" * 50)
            await asyncio.sleep(5)

            # 3. Собираем ссылки со скроллингом
            print("\n🔗 СБОР ССЫЛОК СО СКРОЛЛИНГОМ")
            print("-" * 50)

            # Сначала собираем ссылки без скроллинга
            html_content = await page.evaluate("document.documentElement.outerHTML")
            self._extract_urls_from_html(html_content)

            # Затем выполняем скроллинг и собираем больше ссылок
            await self._scroll_and_collect_urls(page)

            if not self.all_parking_urls:
                print("❌ Не удалось собрать ссылки")
                return []

            print(f"✅ Собрано уникальных ссылок: {len(self.all_parking_urls)}")

            # 4. Парсим ВСЕ парковки
            print("\n🏢 ДЕТАЛЬНЫЙ ПАРСИНГ ПАРКОВОК")
            print("-" * 50)

            urls_to_parse = list(self.all_parking_urls)
            print(f"📊 Будем парсить {len(urls_to_parse)} парковок")

            await self._parse_all_parking_pages(urls_to_parse)

            # 5. Выводим статистику
            self._print_final_stats()

            return self.results

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
            return self.results
        finally:
            await self.close()

    async def _scroll_and_collect_urls(self, page):
        """Скроллинг панели результатов и сбор ссылок"""
        print("Начинаем скроллинг для загрузки всех парковок...")

        max_scroll_attempts = 100
        consecutive_no_new = 0
        total_new_urls = 0
        last_new_urls_count = 0

        for attempt in range(1, max_scroll_attempts + 1):
            print(f"\n   🔄 Попытка скроллинга {attempt}/{max_scroll_attempts}")

            # Сохраняем количество URL до скроллинга
            before_scroll_count = len(self.all_parking_urls)

            # Агрессивный скроллинг с фокусом на Яндекс.Карты
            await self._yandex_specific_scroll(page)

            # Ждем загрузки новых результатов (уменьшим время ожидания)
            wait_time = random.uniform(2, 3)
            print(f"   ⏱ Ждем {wait_time:.1f} сек...")
            await asyncio.sleep(wait_time)

            # Получаем обновленный HTML
            html_content = await page.evaluate("document.documentElement.outerHTML")

            # Извлекаем новые URL
            new_urls_count_before = len(self.all_parking_urls)
            self._extract_urls_from_html(html_content)
            new_urls_count_after = len(self.all_parking_urls)
            new_urls_added = new_urls_count_after - new_urls_count_before

            print(f"   📊 URL до: {before_scroll_count}, добавлено: {new_urls_added}, всего: {new_urls_count_after}")

            # Проверяем, есть ли новые URL
            if new_urls_added > 0:
                consecutive_no_new = 0
                total_new_urls += new_urls_added
                last_new_urls_count = new_urls_added
                print(f"   ✅ Добавилось {new_urls_added} новых URL")

                # Если добавлено мало URL, возможно конец близок
                if new_urls_added < 5:
                    print(f"   ⚠ Мало новых URL ({new_urls_added}), возможно скоро конец")
            else:
                consecutive_no_new += 1
                print(f"   ⚠ Новых URL нет (попыток без новых: {consecutive_no_new}/{self.max_consecutive_no_new})")

                # Если 3 раза подряд нет новых URL - завершаем
                if consecutive_no_new >= self.max_consecutive_no_new:
                    print(f"   🏁 Прекращаем скроллинг - {self.max_consecutive_no_new} раза подряд нет новых URL")
                    break

            # Быстрая проверка конца (без долгого JavaScript)
            is_end = await self._quick_check_end(page)
            if is_end:
                print("   🏁 Быстрая проверка: достигнут конец списка")
                break

            # Если за последние 2 попытки добавилось мало URL, возможно конец
            if attempt > 2 and last_new_urls_count < 3:
                print(f"   ⚠ В последней попытке мало новых URL ({last_new_urls_count}), проверяем конец...")
                is_loading = await self._check_if_loading(page)
                if not is_loading:
                    print("   🏁 Загрузка не активна и мало новых URL - завершаем")
                    break

            # Небольшая задержка между скроллами
            await asyncio.sleep(random.uniform(0.5, 1.5))

        print(f"\n✅ Скроллинг завершен после {min(attempt, max_scroll_attempts)} попыток")
        print(f"📊 Всего добавлено новых URL: {total_new_urls}")
        print(f"📊 Итоговое количество ссылок: {len(self.all_parking_urls)}")

    async def _yandex_specific_scroll(self, page):
        """Специфичный скроллинг для Яндекс.Карт"""
        try:
            # Пробуем найти и скроллить основной контейнер
            await page.evaluate("""
                (function() {
                    // Ищем основной контейнер
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
                            const oldScroll = container.scrollTop;
                            container.scrollTop = container.scrollHeight;

                            // Пробуем плавный скроллинг
                            setTimeout(() => {
                                container.scrollTo({
                                    top: container.scrollHeight,
                                    behavior: 'smooth'
                                });
                            }, 100);

                            scrolled = container.scrollTop > oldScroll;
                            if (scrolled) {
                                console.log('Скроллен контейнер:', selector);
                                break;
                            }
                        }
                    }

                    // Всегда скроллим окно
                    const oldWindowScroll = window.pageYOffset;
                    window.scrollBy({
                        top: 800,
                        behavior: 'smooth'
                    });

                    return {
                        containerScrolled: scrolled,
                        windowScrolled: window.pageYOffset > oldWindowScroll
                    };
                })();
            """)

        except Exception as e:
            print(f"   ⚠ Ошибка скроллинга: {e}")

    async def _quick_check_end(self, page):
        """Быстрая проверка конца списка"""
        try:
            result = await page.evaluate("""
                (function() {
                    // Быстрая проверка по тексту
                    const bodyText = document.body.innerText.toLowerCase();
                    const endKeywords = [
                        'показаны все',
                        'больше нет',
                        'конец списка',
                        'all results shown',
                        'no more results'
                    ];

                    for (const keyword of endKeywords) {
                        if (bodyText.includes(keyword)) {
                            return true;
                        }
                    }

                    return false;
                })();
            """)

            return bool(result)

        except Exception as e:
            return False

    async def _check_if_loading(self, page):
        """Проверяем, идет ли загрузка данных"""
        try:
            result = await page.evaluate("""
                (function() {
                    // Быстрая проверка спиннеров
                    const spinners = document.querySelectorAll('.spin2, .spinner, .loading, .loader');
                    for (const spinner of spinners) {
                        const style = getComputedStyle(spinner);
                        if (style.display !== 'none' && style.visibility !== 'hidden') {
                            return true;
                        }
                    }
                    return false;
                })();
            """)

            return bool(result)

        except Exception as e:
            print(f"   ⚠ Ошибка проверки загрузки: {e}")
            return False

    def _extract_urls_from_html(self, html_content: str):
        """Извлечение ссылок на парковки из HTML"""
        try:
            # Паттерны для поиска
            org_pattern = r'href="(/maps/org/[^"]+)"'
            snippet_pattern = r'<li[^>]*class="[^"]*search-snippet-view[^"]*"[^>]*>.*?</li>'

            # Ищем карточки
            snippets = re.findall(snippet_pattern, html_content, re.DOTALL)

            urls_before = len(self.all_parking_urls)

            for snippet in snippets:
                # Ищем ссылку
                link_match = re.search(org_pattern, snippet)
                if link_match:
                    link = link_match.group(1)
                    if link:
                        full_url = f"https://yandex.ru{link}"
                        # Очищаем URL от параметров
                        clean_url = full_url.split('?')[0].split('#')[0]
                        # Фильтруем системные ссылки
                        if not any(exclude in clean_url.lower() for exclude in ['/reviews/', '/photos/', '/gallery/']):
                            self.all_parking_urls.add(clean_url)

            # Дополнительный поиск ссылок во всем HTML
            all_link_matches = re.findall(r'href="(/maps/org/[^/"]+/[^/"]*)"', html_content)
            for link in all_link_matches:
                full_url = f"https://yandex.ru{link}"
                clean_url = full_url.split('?')[0].split('#')[0]
                if not any(exclude in clean_url.lower() for exclude in ['/reviews/', '/photos/', '/gallery/']):
                    self.all_parking_urls.add(clean_url)

            urls_after = len(self.all_parking_urls)
            new_urls = urls_after - urls_before

            if new_urls > 0:
                print(f"   ✅ Извлечено {new_urls} новых URL")

        except Exception as e:
            print(f"❌ Ошибка извлечения URL: {e}")
            import traceback
            traceback.print_exc()

    # УДАЛИМ НЕНУЖНЫЕ МЕТОДЫ:
    # - _aggressive_yandex_scroll (слишко сложный)
    # - _fallback_scroll (упростим)
    # - _check_end_of_results (слишком сложный, заменим на быстрый)

    # ОСТАВИМ ТОЛЬКО РАБОЧИЕ МЕТОДЫ:

    async def _parse_all_parking_pages(self, urls: List[str]):
        """Парсинг всех страниц парковок"""
        total = len(urls)
        success = 0
        failed = 0

        print(f"Начинаем парсинг {total} парковок...")

        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{total}] Парсим парковку")
            print(f"   🔗 {self._shorten_url(url)}")

            try:
                parking_data = await self._parse_single_parking_page(url)

                if parking_data:
                    # Нормализуем данные
                    normalized = self.normalize_data(parking_data)
                    self.results.append(normalized)
                    success += 1

                    # Краткая информация
                    name = parking_data.get('Название объекта', 'Без названия')[:50]
                    address = parking_data.get('Адрес', '')[:60]
                    print(f"   ✅ {name}")
                    print(f"      📍 {address}")
                else:
                    print("   ❌ Не удалось распарсить")
                    failed += 1

            except Exception as e:
                print(f"   ❌ Ошибка: {str(e)[:100]}")
                failed += 1

            # Задержка между запросами
            if i < total:
                delay = random.uniform(2, 4)
                await asyncio.sleep(delay)

            # Выводим прогресс
            if i % 10 == 0 or i == total:
                progress = (i / total) * 100
                elapsed = time.time() - self.start_time
                print(f"\n📊 Прогресс: {i}/{total} ({progress:.1f}%)")
                print(f"✅ Успешно: {success} | ❌ Ошибок: {failed}")
                print(f"⏱ Прошло: {elapsed:.0f} сек")

        print(f"\n🎉 Парсинг завершен!")
        print(f"📊 Итог: Успешно {success}, Ошибок {failed}")

    # Остальные методы (parse_single_parking_page, extract_parking_data и т.д.)
    # остаются без изменений, так как они работают

    async def _parse_single_parking_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Парсинг одной страницы парковки"""
        max_retries = 2

        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    print(f"   🔄 Повторная попытка {attempt}/{max_retries}")
                    await asyncio.sleep(random.uniform(3, 5))

                # Открываем страницу парковки
                parking_page = await self.browser.get(url)
                await asyncio.sleep(random.uniform(3, 4))

                # Прокручиваем немного для загрузки контента
                await parking_page.evaluate("window.scrollBy(0, 300)")
                await asyncio.sleep(1)

                # Получаем HTML
                html = await parking_page.evaluate("document.documentElement.outerHTML")

                if not html:
                    continue

                # Парсим данные
                soup = BeautifulSoup(html, 'lxml')
                data = self._extract_parking_data(url, soup, html)

                # Проверяем минимальные данные
                if data.get('Название объекта') or data.get('Адрес'):
                    return data
                else:
                    print(f"   ⚠ Мало данных на странице")

            except Exception as e:
                error_msg = str(e)
                if "timeout" in error_msg.lower():
                    print(f"   ⚠ Таймаут при загрузке")
                else:
                    print(f"   ✗ Ошибка: {error_msg[:50]}...")

            # Задержка перед повторной попыткой
            if attempt < max_retries:
                await asyncio.sleep(random.uniform(5, 8))

        return None

    def _extract_parking_data(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """Извлечение данных со страницы парковки"""
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
        coords = self._extract_coordinates(url, soup)
        if coords:
            data['Координаты'] = coords

        # 4. Телефон
        phone_links = soup.find_all('a', href=re.compile(r'^tel:'))
        phones = []
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
                    data['Время работы парковки'] = text
                    break

        # 8. Рейтинг
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
                    data['Оценка парковки'] = text
                    break

        # 9. Количество оценок
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

        # 10. Тип парковки
        parking_type = self._detect_parking_type(soup, data.get('Название объекта', ''), html)
        data['Тип парковки'] = parking_type

        # Определяем доступ
        if 'закрыт' in parking_type.lower() or 'охраня' in parking_type.lower():
            data['Доступ'] = 'Закрытый'
        else:
            data['Доступ'] = 'Открытый'

        # 11. Цены
        page_text = soup.get_text()
        price_matches = re.findall(r'(\d+\s*руб|\d+\s*₽|\d+\s*в час|\d+\s*в сутки)', page_text, re.IGNORECASE)
        if price_matches:
            data['Цены'] = price_matches[0]
            data['Тарифы'] = '; '.join(price_matches[:3])

        # 12. Вместимость
        capacity_match = re.search(r'(\d+)\s*мест|\bвместимость\s*(\d+)', page_text, re.IGNORECASE)
        if capacity_match:
            capacity = capacity_match.group(1) or capacity_match.group(2)
            data['Вместимость'] = capacity

        # 13. Описание
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

    def _extract_coordinates(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение координат"""
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

    def _detect_parking_type(self, soup: BeautifulSoup, name: str, html: str) -> str:
        """Определение типа парковки"""
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

    def _clean_text(self, text: str) -> str:
        """Очистка текста"""
        if not text:
            return ""
        text = ' '.join(text.split())
        return text.strip()

    def _shorten_url(self, url: str, max_length: int = 60) -> str:
        """Сокращение URL для вывода"""
        if len(url) <= max_length:
            return url
        return url[:max_length - 3] + "..."

    def _print_final_stats(self):
        """Вывод финальной статистики"""
        print("\n" + "=" * 60)
        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА ЯНДЕКС КАРТ")
        print("=" * 60)

        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        print(f"⏱ Общее время: {minutes} мин {seconds} сек")
        print(f"🔗 Всего ссылок собрано: {len(self.all_parking_urls)}")
        print(f"✅ Успешно распарсено: {len(self.results)}")

        if self.all_parking_urls:
            success_rate = (len(self.results) / len(self.all_parking_urls)) * 100
            print(f"📈 Эффективность парсинга: {success_rate:.1f}%")

        if self.results:
            # Статистика по типам
            closed_count = len([p for p in self.results if 'закрыт' in p.get('Тип парковки', '').lower()])
            paid_count = len([p for p in self.results if 'платн' in p.get('Тип парковки', '').lower()])
            guarded_count = len([p for p in self.results if 'охраня' in p.get('Тип парковки', '').lower()])

            print(f"\n🚗 ТИПЫ ПАРКОВОК:")
            print(f"   Закрытых: {closed_count}")
            print(f"   Охраняемых: {guarded_count}")
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

            # Примеры
            print(f"\n🏆 ПРИМЕРЫ НАЙДЕННЫХ ПАРКОВОК:")
            for i, item in enumerate(self.results[:3], 1):
                name = item.get('Название объекта', 'Без названия')[:40]
                address = item.get('Адрес', '')[:50]
                parking_type = item.get('Тип парковки', 'неизвестно')
                print(f"   {i}. {name}")
                print(f"      📍 {address}")
                print(f"      🚗 {parking_type}")

        print("=" * 60)


# Тестовый запуск
async def test_yandex_parser():
    """Тестирование парсера"""
    print("🧪 ТЕСТИРУЕМ ПАРСЕР ЯНДЕКС КАРТ")
    parser = YandexParser(headless=False)

    try:
        results = await parser.parse()

        print(f"\n🎉 Тест завершен! Найдено {len(results)} парковок")

        if results:
            # Сохраняем результаты
            with open("yandex_test_results.json", "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print("💾 Результаты сохранены в yandex_test_results.json")

            # Выводим статистику
            print(f"\n📊 СТАТИСТИКА:")
            print(f"   Всего ссылок собрано: {len(parser.all_parking_urls)}")
            print(f"   Успешно распарсено: {len(results)}")

    except Exception as e:
        print(f"\n❌ Ошибка теста: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await parser.close()


if __name__ == "__main__":
    asyncio.run(test_yandex_parser())
