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
        self.max_scroll_attempts_without_new = 5  # Максимум попыток без новых URL

    @property
    def source_name(self) -> str:
        return "yandex"

    async def parse(self, max_scrolls: int = 50, max_parkings: int = 200) -> List[Dict[str, Any]]:
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
            await asyncio.sleep(5)

            # Кликаем кнопку "Показать результаты"
            button = await page.query_selector('span.search-command-view__show-results-button')
            if button:
                print("✅ Кнопка найдена, кликаем...")
                await button.click()
                await asyncio.sleep(5)
                print("✅ Результаты загружены")
            else:
                print("⚠️ Кнопка не найдена, проверяем есть ли результаты...")

            # 2. Собираем ВСЕ ссылки с использованием скроллинга
            print("\n📜 СБОР ВСЕХ ССЫЛОК СО СКРОЛЛИНГОМ")
            print("-" * 50)

            await self._collect_all_urls_with_scrolling(page, max_scrolls)

            if not self.all_parking_urls:
                print("❌ Не удалось собрать ссылки")
                return []

            print(f"\n✅ Собрано уникальных ссылок: {len(self.all_parking_urls)}")

            # Сохраняем список URL для отладки
            self._save_urls_list()

            # 3. Парсим каждую парковку (ограничиваем количество)
            print("\n🏢 ДЕТАЛЬНЫЙ ПАРСИНГ ПАРКОВОК")
            print("-" * 50)

            urls_to_parse = list(self.all_parking_urls)[:max_parkings]
            print(f"📊 Будем парсить {len(urls_to_parse)} парковок")

            await self._parse_all_parking_pages(urls_to_parse)

            # 4. Выводим статистику
            self._print_final_stats(urls_to_parse)

            return self.results

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
            return self.results
        finally:
            await self.close()

    async def _collect_all_urls_with_scrolling(self, page, max_scrolls: int):
        """Сбор всех ссылок с использованием скроллинга"""
        print("Начинаем сбор ссылок со скроллингом...")

        # Дебаг структуры страницы
        await self._debug_page_structure(page)

        scroll_attempt = 0
        attempts_without_new_urls = 0

        while scroll_attempt < max_scrolls and attempts_without_new_urls < self.max_scroll_attempts_without_new:
            scroll_attempt += 1
            print(f"\n   🔄 Попытка {scroll_attempt}/{max_scrolls}")

            # Собираем ссылки с текущей страницы
            current_urls = await self._extract_all_urls_from_page(page)
            previous_count = len(self.all_parking_urls)
            self.all_parking_urls.update(current_urls)
            current_count = len(self.all_parking_urls)
            new_urls = current_count - previous_count

            print(f"   📎 Всего URL: {current_count} (+{new_urls} новых)")

            # Проверяем, есть ли новые URL
            if new_urls > 0:
                attempts_without_new_urls = 0
                print(f"   ✅ Найдено {new_urls} новых URL")
            else:
                attempts_without_new_urls += 1
                print(
                    f"   ⚠ Новых URL нет (попыток: {attempts_without_new_urls}/{self.max_scroll_attempts_without_new})")

            # Если слишком долго нет новых URL, останавливаемся
            if attempts_without_new_urls >= self.max_scroll_attempts_without_new:
                print("   🏁 Прекращаем - слишком долго нет новых URL")
                break

            # Прокручиваем разными способами
            await self._perform_scroll_actions(page)

            # Ждем загрузки новых результатов
            await asyncio.sleep(random.uniform(3, 5))

            # Периодически сохраняем прогресс
            if scroll_attempt % 5 == 0:
                print(f"   💾 Промежуточный результат: {current_count} ссылок")

        print(f"\n✅ Сбор ссылок завершен после {scroll_attempt} попыток")
        print(f"📊 Всего собрано уникальных URL: {len(self.all_parking_urls)}")

    async def _perform_scroll_actions(self, page):
        """Выполнение различных действий скроллинга"""
        try:
            print("   🎯 Пробуем разные методы скроллинга...")

            # Способ 1: Ищем кнопки с текстом "Показать ещё", "Ещё", "Загрузить ещё"
            show_more_texts = ['показать', 'еще', 'ещё', 'загрузить', 'show more', 'load more']

            all_buttons = await page.query_selector_all('button, [role="button"], [class*="button"], [class*="btn"]')

            clicked = False
            for button in all_buttons:
                try:
                    button_text = await button.text()
                    if button_text:
                        button_text_lower = button_text.lower()
                        if any(text in button_text_lower for text in show_more_texts):
                            print(f"   🖱️ Найдена кнопка: '{button_text}', кликаем...")
                            await button.click()
                            await asyncio.sleep(3)
                            clicked = True
                            break
                except:
                    continue

            if clicked:
                return

            # Способ 2: Используем JavaScript для поиска и клика по кнопкам
            print("   📜 Используем JavaScript для поиска кнопок...")

            scroll_result = await page.evaluate("""
                (function() {
                    let clicked = false;

                    // Ищем все элементы, которые могут быть кнопками
                    const possibleButtons = document.querySelectorAll('button, [role="button"], [class*="button"], [class*="btn"], [onclick]');
                    const showMoreKeywords = ['показать', 'еще', 'ещё', 'загрузить', 'show', 'more', 'load'];

                    for (const element of possibleButtons) {
                        const text = (element.textContent || element.innerText || '').toLowerCase().trim();
                        const title = (element.getAttribute('title') || '').toLowerCase();
                        const ariaLabel = (element.getAttribute('aria-label') || '').toLowerCase();

                        // Проверяем, содержит ли элемент ключевые слова
                        const allText = text + ' ' + title + ' ' + ariaLabel;

                        if (showMoreKeywords.some(keyword => allText.includes(keyword))) {
                            console.log(`Найдена кнопка: ${text}`);
                            element.click();
                            clicked = true;
                            break;
                        }
                    }

                    // Если не нашли кнопку, пробуем прокрутить
                    if (!clicked) {
                        // Прокручиваем разные контейнеры
                        const containers = [
                            document.querySelector('.sidebar-view__panel'),
                            document.querySelector('.scroll__container'),
                            document.querySelector('.search-list-view__list-container'),
                            document.body
                        ].filter(c => c);

                        for (const container of containers) {
                            const oldScroll = container.scrollTop || window.pageYOffset;
                            const scrollAmount = 1000;

                            if (container === document.body) {
                                window.scrollBy(0, scrollAmount);
                            } else {
                                container.scrollTop += scrollAmount;
                            }

                            console.log(`Прокручен ${container === document.body ? 'window' : 'container'} на ${scrollAmount}px`);
                            break;
                        }
                    }

                    return { clicked: clicked };
                })();
            """)

            if scroll_result.get('clicked'):
                print("   ✅ Выполнен клик через JavaScript")
            else:
                print("   📜 Выполнена прокрутка через JavaScript")

            # Способ 3: Имитация действий пользователя
            print("   👤 Имитируем действия пользователя...")
            await self._emulate_user_scrolling(page)

            # Ждем немного после всех действий
            await asyncio.sleep(2)

        except Exception as e:
            print(f"   ⚠ Ошибка при скроллинге: {e}")

    async def _emulate_user_scrolling(self, page):
        """Имитация действий пользователя для загрузки контента"""
        try:
            # 1. Прокрутка вниз
            await page.evaluate("""
                // Прокручиваем окно вниз
                window.scrollTo(0, document.body.scrollHeight);

                // Также пробуем прокрутить возможные контейнеры
                const containers = [
                    '.sidebar-view__panel',
                    '.scroll__container',
                    '.search-list-view__list-container',
                    '.scrollable-container',
                    '.search-list-view__list'
                ];

                containers.forEach(selector => {
                    const container = document.querySelector(selector);
                    if (container && container.scrollHeight > container.clientHeight) {
                        container.scrollTop = container.scrollHeight;
                    }
                });
            """)
            await asyncio.sleep(1)

            # 2. Небольшая прокрутка вверх и вниз
            await page.evaluate("window.scrollBy(0, -200)")
            await asyncio.sleep(0.5)
            await page.evaluate("window.scrollBy(0, 400)")
            await asyncio.sleep(0.5)

            # 3. Имитируем движение мыши
            await page.evaluate("""
                // Движение мыши по экрану
                const moveEvent = new MouseEvent('mousemove', {
                    clientX: window.innerWidth / 2,
                    clientY: window.innerHeight / 2,
                    bubbles: true
                });
                document.dispatchEvent(moveEvent);

                // Клик в центре экрана
                const clickEvent = new MouseEvent('click', {
                    clientX: window.innerWidth / 2,
                    clientY: window.innerHeight / 2,
                    bubbles: true
                });
                document.dispatchEvent(clickEvent);
            """)

            await asyncio.sleep(1)

            print("   📜 Выполнена имитация действий пользователя")

        except Exception as e:
            print(f"   ⚠ Ошибка имитации действий: {e}")

    async def _scroll_with_javascript(self, page):
        """Скроллинг с использованием JavaScript"""
        try:
            # Пробуем найти и прокрутить элементы с результатами
            scroll_result = await page.evaluate("""
                (function() {
                    let scrolled = false;

                    // 1. Ищем карточки результатов
                    const resultCards = document.querySelectorAll('.search-snippet-view, .search-list-view__list-item');
                    if (resultCards.length > 0) {
                        // Прокручиваем к последней карточке
                        const lastCard = resultCards[resultCards.length - 1];
                        lastCard.scrollIntoView({ behavior: 'smooth', block: 'end' });
                        scrolled = true;
                    }

                    // 2. Ищем кнопки загрузки
                    const loadButtons = document.querySelectorAll('button, [role="button"], [class*="button"]');
                    for (const button of loadButtons) {
                        const text = button.textContent || button.innerText || '';
                        if (text.toLowerCase().includes('показать') || 
                            text.toLowerCase().includes('еще') ||
                            text.toLowerCase().includes('загрузить')) {
                            button.click();
                            scrolled = true;
                            break;
                        }
                    }

                    // 3. Ищем элементы пагинации
                    const pagination = document.querySelector('.pagination, .pager, [class*="page"]');
                    if (pagination) {
                        const nextButton = pagination.querySelector('[rel="next"], .next, [class*="next"]');
                        if (nextButton) {
                            nextButton.click();
                            scrolled = true;
                        }
                    }

                    return { success: true, scrolled: scrolled, elementsFound: resultCards.length };
                })();
            """)

            if scroll_result.get('success'):
                print(f"   📜 Найдено {scroll_result['elementsFound']} элементов")
                if scroll_result['scrolled']:
                    print("   ✅ Выполнен скроллинг через JavaScript")
                else:
                    print("   ⚠ Скроллинг не выполнен")

        except Exception as e:
            print(f"   ⚠ Ошибка JavaScript скроллинга: {e}")

    async def _extract_all_urls_from_page(self, page) -> Set[str]:
        """Извлечение ВСЕХ URL парковок с текущей страницы"""
        try:
            # Используем более агрессивный поиск URL
            urls = await page.evaluate("""
                (function() {
                    const urls = new Set();

                    // 1. Ищем все ссылки на организации
                    const allLinks = document.querySelectorAll('a[href*="/org/"]');
                    console.log(`Найдено ссылок /org/: ${allLinks.length}`);

                    for (const link of allLinks) {
                        let href = link.getAttribute('href');
                        if (href) {
                            // Формируем полный URL
                            if (href.startsWith('/')) {
                                href = 'https://yandex.ru' + href;
                            } else if (href.startsWith('./')) {
                                href = 'https://yandex.ru' + href.substring(1);
                            }

                            // Очищаем URL
                            const cleanUrl = href.split('?')[0].split('#')[0];

                            // Фильтруем системные ссылки
                            if (!cleanUrl.includes('/reviews/') && 
                                !cleanUrl.includes('/photos/') && 
                                !cleanUrl.includes('/gallery/')) {
                                urls.add(cleanUrl);
                            }
                        }
                    }

                    // 2. Ищем в data-атрибутах
                    const elementsWithId = document.querySelectorAll('[data-id], [data-bem]');
                    console.log(`Найдено элементов с data-id/data-bem: ${elementsWithId.length}`);

                    for (const element of elementsWithId) {
                        const dataId = element.getAttribute('data-id') || '';
                        const dataBem = element.getAttribute('data-bem') || '';

                        // Ищем ID организации
                        const idMatch = dataId.match(/id(\\d+)/) || dataBem.match(/"id":"(\\d+)"/);
                        if (idMatch) {
                            const orgId = idMatch[1];
                            urls.add(`https://yandex.ru/maps/org/${orgId}/`);
                        }
                    }

                    // 3. Ищем в тексте страницы
                    const pageText = document.body.innerText;
                    const orgPattern = /yandex\\.ru\\/maps\\/org\\/[^\\s)]+/g;
                    const matches = pageText.match(orgPattern);
                    if (matches) {
                        console.log(`Найдено URL в тексте: ${matches.length}`);
                        matches.forEach(match => {
                            const cleanUrl = match.split('?')[0].split('#')[0];
                            urls.add(cleanUrl.startsWith('http') ? cleanUrl : 'https://' + cleanUrl);
                        });
                    }

                    // 4. Ищем элементы с координатами (часто это карточки парковок)
                    const coordElements = document.querySelectorAll('[data-coordinates]');
                    console.log(`Найдено элементов с координатами: ${coordElements.length}`);

                    for (const element of coordElements) {
                        // Ищем ближайшую ссылку
                        const link = element.closest('a[href*="/org/"]') || element.querySelector('a[href*="/org/"]');
                        if (link) {
                            let href = link.getAttribute('href');
                            if (href) {
                                if (href.startsWith('/')) {
                                    href = 'https://yandex.ru' + href;
                                }
                                const cleanUrl = href.split('?')[0].split('#')[0];
                                if (!cleanUrl.includes('/reviews/') && 
                                    !cleanUrl.includes('/photos/') && 
                                    !cleanUrl.includes('/gallery/')) {
                                    urls.add(cleanUrl);
                                }
                            }
                        }
                    }

                    // 5. Ищем карточки результатов
                    const snippetElements = document.querySelectorAll('.search-snippet-view, .search-list-view__list-item');
                    console.log(`Найдено карточек результатов: ${snippetElements.length}`);

                    for (const element of snippetElements) {
                        const link = element.querySelector('a[href*="/org/"]');
                        if (link) {
                            let href = link.getAttribute('href');
                            if (href) {
                                if (href.startsWith('/')) {
                                    href = 'https://yandex.ru' + href;
                                }
                                const cleanUrl = href.split('?')[0].split('#')[0];
                                if (!cleanUrl.includes('/reviews/') && 
                                    !cleanUrl.includes('/photos/') && 
                                    !cleanUrl.includes('/gallery/')) {
                                    urls.add(cleanUrl);
                                }
                            }
                        }
                    }

                    console.log(`Всего найдено уникальных URL: ${urls.size}`);
                    return Array.from(urls);
                })();
            """)

            # Конвертируем список в set (исправляем ошибку)
            url_set = set(urls) if urls else set()

            # Фильтруем только URL парковок
            filtered_urls = {url for url in url_set if self._is_parking_url(url)}

            print(f"   🔍 Найдено {len(filtered_urls)} URL парковок")

            # Для дебага выводим первые 5 URL
            if filtered_urls:
                print(f"   📋 Примеры URL:")
                for i, url in enumerate(list(filtered_urls)[:3]):
                    print(f"      {i + 1}. {self._shorten_url(url, 70)}")

            return filtered_urls

        except Exception as e:
            print(f"   ⚠ Ошибка извлечения URL: {e}")
            import traceback
            traceback.print_exc()
            return set()

    def _is_parking_url(self, url: str) -> bool:
        """Проверка, является ли URL ссылкой на парковку"""
        # Ключевые слова в URL, указывающие на парковку
        parking_keywords = [
            'parkovka',
            'parking',
            'avtoparkovka',
            'avtomobilnaya_parkovka',
            'sto',  # СТО часто имеют парковки
            'parking_lot',
            'car_park'
        ]

        url_lower = url.lower()

        # Проверяем ключевые слова
        for keyword in parking_keywords:
            if keyword in url_lower:
                return True

        # Если URL содержит /org/, считаем что это организация (возможно парковка)
        if '/org/' in url_lower:
            # Исключаем явно не парковочные URL
            non_parking = ['/reviews/', '/photos/', '/gallery/', '/attraction/', '/hotel/']
            if not any(np in url_lower for np in non_parking):
                return True

        return False

    def _save_urls_list(self):
        """Сохранение списка URL для отладки"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"yandex_urls_{timestamp}.txt"

            with open(filename, 'w', encoding='utf-8') as f:
                for url in sorted(self.all_parking_urls):
                    f.write(f"{url}\n")

            print(f"💾 Список URL сохранен в {filename}")

        except Exception as e:
            print(f"⚠ Ошибка сохранения URL: {e}")

    # Остальные методы остаются без изменений (они уже рабочие)
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
            if i % 5 == 0 or i == total:
                progress = (i / total) * 100
                elapsed = time.time() - self.start_time
                print(f"\n📊 Прогресс: {i}/{total} ({progress:.1f}%)")
                print(f"✅ Успешно: {success} | ❌ Ошибок: {failed}")
                print(f"⏱ Прошло: {elapsed:.0f} сек")

        print(f"\n🎉 Парсинг завершен!")
        print(f"📊 Итог: Успешно {success}, Ошибок {failed}")

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

    def _print_final_stats(self, urls_to_parse: List[str]):
        """Вывод финальной статистики"""
        print("\n" + "=" * 60)
        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА ЯНДЕКС КАРТ")
        print("=" * 60)

        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        print(f"⏱ Общее время: {minutes} мин {seconds} сек")
        print(f"🔗 Всего ссылок собрано: {len(self.all_parking_urls)}")
        print(f"🔗 Парсилось: {len(urls_to_parse)}")
        print(f"✅ Успешно распарсено: {len(self.results)}")

        if urls_to_parse:
            success_rate = (len(self.results) / len(urls_to_parse)) * 100
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

    async def _debug_page_structure(self, page):
        """Дебаг структуры страницы"""
        print("\n🔍 ДЕБАГ СТРУКТУРЫ СТРАНИЦЫ:")
        print("-" * 40)

        try:
            structure = await page.evaluate("""
                (function() {
                    const results = {};

                    // 1. Проверяем основные контейнеры
                    const containers = [
                        '.sidebar-view__panel',
                        '.scroll__container',
                        '.search-list-view__list-container',
                        '.search-list-view__list',
                        '.search-list-view__items',
                        '.search-snippet-view',
                        '[data-coordinates]',
                        'a[href*="/org/"]'
                    ];

                    containers.forEach(selector => {
                        const elements = document.querySelectorAll(selector);
                        results[selector] = {
                            count: elements.length,
                            firstExists: elements.length > 0
                        };

                        // Для некоторых селекторов показываем дополнительную информацию
                        if (elements.length > 0 && (selector.includes('panel') || selector.includes('container'))) {
                            const firstEl = elements[0];
                            results[selector].scrollHeight = firstEl.scrollHeight;
                            results[selector].clientHeight = firstEl.clientHeight;
                            results[selector].scrollable = firstEl.scrollHeight > firstEl.clientHeight;
                        }
                    });

                    // 2. Проверяем кнопки
                    const buttons = document.querySelectorAll('button, [role="button"]');
                    results['buttons'] = {
                        count: buttons.length,
                        texts: Array.from(buttons).map(btn => btn.textContent?.trim() || btn.innerText?.trim() || '').filter(t => t)
                    };

                    // 3. Проверяем текст на странице
                    const bodyText = document.body.innerText || '';
                    results['textStats'] = {
                        length: bodyText.length,
                        containsParking: bodyText.toLowerCase().includes('парков'),
                        containsShowMore: bodyText.toLowerCase().includes('показать') || bodyText.toLowerCase().includes('еще')
                    };

                    return results;
                })();
            """)

            # Выводим результаты
            print("📊 СТАТИСТИКА СТРАНИЦЫ:")
            for selector, info in structure.items():
                if selector == 'textStats':
                    print(f"   📝 Текст страницы:")
                    print(f"      Длина: {info['length']} символов")
                    print(f"      Содержит 'парков': {info['containsParking']}")
                    print(f"      Содержит 'показать/еще': {info['containsShowMore']}")
                elif selector == 'buttons':
                    print(f"   🖱️ Кнопки: {info['count']} шт.")
                    if info['texts']:
                        unique_texts = list(set(info['texts']))[:5]
                        print(f"      Тексты: {', '.join(unique_texts)}")
                else:
                    if info['count'] > 0:
                        print(f"   {selector}: {info['count']} элементов")
                        if 'scrollable' in info:
                            print(f"      Прокручиваемый: {info['scrollable']}")
                            print(
                                f"      Высота: {info.get('scrollHeight', 'N/A')} / {info.get('clientHeight', 'N/A')}")

            print("-" * 40)

        except Exception as e:
            print(f"   ❌ Ошибка дебага: {e}")


# Тестовый запуск
async def test_yandex_scrolling():
    """Тестирование парсера со скроллингом"""
    print("🧪 ТЕСТИРУЕМ ПАРСЕР СО СКРОЛЛИНГОМ")
    parser = YandexParser(headless=False)

    try:
        # Тестируем скроллинг с небольшими лимитами
        results = await parser.parse(max_scrolls=15, max_parkings=20)

        print(f"\n🎉 Тест завершен! Найдено {len(results)} парковок")

        if results:
            # Сохраняем результаты
            with open("yandex_scrolling_results.json", "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print("💾 Результаты сохранены в yandex_scrolling_results.json")

            # Выводим статистику по собранным URL
            print(f"\n📊 СТАТИСТИКА ПО ССЫЛКАМ:")
            print(f"   Всего ссылок собрано: {len(parser.all_parking_urls)}")

            # Сохраняем список URL
            import csv
            with open("yandex_all_urls.csv", "w", encoding="utf-8", newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["URL"])
                for url in parser.all_parking_urls:
                    writer.writerow([url])
            print("💾 Список URL сохранен в yandex_all_urls.csv")



    except Exception as e:
        print(f"\n❌ Ошибка теста: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await parser.close()


# В конце файла, перед if __name__ == "__main__":

async def debug_scrolling_only():
    """Только дебаг скроллинга без парсинга"""
    print("🧪 ДЕБАГ СКРОЛЛИНГА ЯНДЕКС.КАРТ")

    parser = YandexParser(headless=False)

    try:
        await parser.init_browser()

        # Загружаем страницу поиска
        search_url = "https://yandex.ru/maps/2/saint-petersburg/search/парковки/"
        print(f"🔗 Загружаем: {search_url}")

        page = await parser.browser.get(search_url)
        await asyncio.sleep(5)

        # Кликаем кнопку "Показать результаты"
        button = await page.query_selector('span.search-command-view__show-results-button')
        if button:
            print("✅ Кликаем кнопку 'Показать результаты'...")
            await button.click()
            await asyncio.sleep(5)

        # Делаем скриншот ДО скроллинга
        await page.save_screenshot("debug_before_scroll.png")
        print("💾 Скриншот ДО сохранен: debug_before_scroll.png")

        # Собираем ссылки ДО скроллинга
        print("\n📋 СБИРАЕМ ССЫЛКИ ДО СКРОЛЛИНГА:")
        urls_before = await parser._extract_all_urls_from_page(page)
        print(f"   Найдено ссылок: {len(urls_before)}")

        # Выполняем скроллинг несколько раз
        print("\n📜 ВЫПОЛНЯЕМ СКРОЛЛИНГ (5 попыток):")
        for i in range(5):
            print(f"\n   🔄 Попытка скроллинга {i + 1}/5")

            # Пробуем разные методы скроллинга
            await parser._perform_scroll_actions(page)

            # Ждем загрузки
            await asyncio.sleep(4)

            # Делаем скриншот после каждой попытки
            await page.save_screenshot(f"debug_scroll_attempt_{i + 1}.png")
            print(f"   💾 Скриншот сохранен: debug_scroll_attempt_{i + 1}.png")

            # Собираем ссылки после скроллинга
            current_urls = await parser._extract_all_urls_from_page(page)
            print(f"   Найдено ссылок: {len(current_urls)}")

            # Сравниваем
            new_urls = len(current_urls) - len(urls_before)
            if new_urls > 0:
                print(f"   ✅ Добавилось новых ссылок: +{new_urls}")
            else:
                print(f"   ⚠ Новых ссылок нет")

        # Финальный сбор всех ссылок
        print("\n📊 ФИНАЛЬНЫЙ СБОР ССЫЛОК:")
        all_urls = await parser._extract_all_urls_from_page(page)
        print(f"   Всего ссылок: {len(all_urls)}")

        # Сохраняем список URL
        with open("debug_scroll_urls.txt", "w", encoding="utf-8") as f:
            for url in sorted(all_urls):
                f.write(f"{url}\n")
        print("💾 Список URL сохранен в debug_scroll_urls.txt")

        print("\n🎉 Дебаг скроллинга завершен!")
        print("📁 Проверьте файлы:")
        print("   - debug_before_scroll.png")
        print("   - debug_scroll_attempt_*.png")
        print("   - debug_scroll_urls.txt")

    except Exception as e:
        print(f"\n❌ Ошибка дебага: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await parser.close()

if __name__ == "__main__":
    print("Выберите режим:")
    print("1. Полный парсинг (со скроллингом)")
    print("2. Только дебаг скроллинга")

    choice = input("Введите 1 или 2: ").strip()

    if choice == "1":
        asyncio.run(test_yandex_scrolling())
    else:
        asyncio.run(debug_scrolling_only())
