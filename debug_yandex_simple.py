import asyncio
import sys
from pathlib import Path
import json
import re

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import nodriver


async def parse_parkings_from_html(html_content):
    """Парсит данные парковок из HTML"""
    parking_data = []

    # Паттерны для поиска
    coord_pattern = r'data-coordinates="([^"]+)"'
    org_pattern = r'href="(/maps/org/[^"]+)"'
    snippet_pattern = r'<li[^>]*class="[^"]*search-snippet-view[^"]*"[^>]*>.*?</li>'

    # Ищем карточки
    snippets = re.findall(snippet_pattern, html_content, re.DOTALL)

    print(f"Найдено карточек (snippets): {len(snippets)}")

    for i, snippet in enumerate(snippets):
        # Ищем координаты
        coord_match = re.search(coord_pattern, snippet)
        coordinates = coord_match.group(1) if coord_match else None

        # Ищем ссылку
        link_match = re.search(org_pattern, snippet)
        link = link_match.group(1) if link_match else None

        # Ищем название
        name = "Неизвестно"

        # Пробуем найти разные варианты названий
        title_patterns = [
            r'<[^>]*class="[^"]*search-business-snippet-view__title[^"]*"[^>]*>([^<]+)</',
            r'<[^>]*class="[^"]*orgpage-header-view__header[^"]*"[^>]*>([^<]+)</',
            r'<a[^>]*href="/maps/org/[^"]*"[^>]*>([^<]+)</a>',
            r'<[^>]*class="[^"]*business-snippet-view__title[^"]*"[^>]*>([^<]+)</',
        ]

        for pattern in title_patterns:
            match = re.search(pattern, snippet)
            if match:
                name = match.group(1).strip()
                break

        # Ищем адрес
        address = ""
        address_patterns = [
            r'<[^>]*class="[^"]*search-business-snippet-view__address[^"]*"[^>]*>([^<]+)</',
            r'<[^>]*class="[^"]*business-snippet-view__address[^"]*"[^>]*>([^<]+)</',
        ]

        for pattern in address_patterns:
            match = re.search(pattern, snippet)
            if match:
                address = match.group(1).strip()
                break

        if coordinates:
            parking_data.append({
                "index": i + 1,
                "name": name,
                "coordinates": coordinates,
                "latitude": float(coordinates.split(',')[1]) if ',' in coordinates else None,
                "longitude": float(coordinates.split(',')[0]) if ',' in coordinates else None,
                "link": f"https://yandex.ru{link}" if link else None,
                "address": address,
                "source": "yandex_maps"
            })

    return parking_data


async def main_parser():
    """Главный рабочий парсер Яндекс.Карт"""
    print("🚀 Яндекс.Карт Парсер Парковок")
    print("=" * 60)

    browser = None
    page = None

    try:
        # Запускаем браузер
        print("Запускаем браузер...")
        browser = await nodriver.start(
            headless=False,  # Можно поставить True после отладки
            window_size=(1200, 800),
            disable_features=[],
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process"
            ]
        )

        # Переходим на страницу
        url = "https://yandex.ru/maps/2/saint-petersburg/search/парковки/"
        print(f"Переходим: {url}")

        page = await browser.get(url)
        await asyncio.sleep(3)

        # Ищем кнопку "Показать результаты"
        button = await page.query_selector('span.search-command-view__show-results-button')
        if button:
            print("Найдена кнопка 'Показать результаты', кликаем...")
            await button.click()
            await asyncio.sleep(3)
        else:
            print("Кнопка не найдена, продолжаем...")

        # Ждем загрузки
        print("Ожидаем загрузки данных...")
        await asyncio.sleep(5)

        # Получаем HTML
        print("Получаем HTML страницы...")
        html_content = await page.evaluate("document.documentElement.outerHTML")

        if not html_content or not isinstance(html_content, str):
            print("❌ Не удалось получить HTML")
            return

        # Сохраняем HTML для отладки
        debug_html_path = "yandex_parsed.html"
        with open(debug_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"💾 HTML сохранен: {debug_html_path} ({len(html_content)} символов)")

        # Парсим данные
        print("\nПарсим данные парковок...")
        parkings = await parse_parkings_from_html(html_content)

        if parkings:
            print(f"✅ Успешно собрано {len(parkings)} парковок!")

            # Выводим статистику
            print("\n" + "=" * 60)
            print("СТАТИСТИКА:")
            print(f"• Всего парковок: {len(parkings)}")
            print(f"• С координатами: {len([p for p in parkings if p['coordinates']])}")
            print(f"• Со ссылками: {len([p for p in parkings if p['link']])}")
            print(f"• С адресами: {len([p for p in parkings if p['address']])}")

            # Выводим первые 5 парковок
            print("\n" + "=" * 60)
            print("ПЕРВЫЕ 5 ПАРКОВОК:")
            for i, parking in enumerate(parkings[:5]):
                print(f"\n{i + 1}. {parking['name']}")
                print(f"   📍 Координаты: {parking['coordinates']}")
                print(f"   🏢 Адрес: {parking['address']}")
                print(f"   🔗 Ссылка: {parking['link']}")

            # Сохраняем в JSON
            json_path = "yandex_parkings.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(parkings, f, ensure_ascii=False, indent=2)
            print(f"\n💾 JSON сохранен: {json_path}")

            # Сохраняем в CSV
            csv_path = "yandex_parkings.csv"
            import csv
            with open(csv_path, "w", encoding="utf-8", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["index", "name", "coordinates", "latitude", "longitude", "link",
                                                       "address", "source"])
                writer.writeheader()
                writer.writerows(parkings)
            print(f"💾 CSV сохранен: {csv_path}")

            # Сохраняем в Excel (если есть библиотека)
            try:
                import pandas as pd
                df = pd.DataFrame(parkings)
                excel_path = "yandex_parkings.xlsx"
                df.to_excel(excel_path, index=False)
                print(f"💾 Excel сохранен: {excel_path}")
            except ImportError:
                print("ℹ️  Для сохранения в Excel установите pandas: pip install pandas")

        else:
            print("❌ Не найдено данных о парковках")

        print("\n" + "=" * 60)
        print("РАБОТА ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

    finally:
        print("\n🔄 Закрываем браузер...")
        if browser:
            try:
                # Пробуем корректно закрыть
                await browser.stop()
                print("✅ Браузер закрыт")
            except Exception as e:
                print(f"⚠️ Ошибка при закрытии браузера: {e}")
                # Альтернативный способ
                try:
                    # nodriver иногда требует другой подход
                    import gc
                    browser = None
                    page = None
                    gc.collect()
                    print("✅ Ресурсы освобождены")
                except:
                    pass


async def quick_parser():
    """Быстрый парсер с принудительным завершением"""
    print("⚡ БЫСТРЫЙ ПАРСЕР")

    try:
        browser = await nodriver.start(headless=False)
        page = await browser.get("https://yandex.ru/maps/2/saint-petersburg/search/парковки/")
        await asyncio.sleep(3)

        # Кликаем кнопку
        button = await page.query_selector('span.search-command-view__show-results-button')
        if button:
            await button.click()
            await asyncio.sleep(3)

        await asyncio.sleep(5)

        # Получаем HTML
        html = await page.evaluate("document.documentElement.outerHTML")

        # Быстрый парсинг
        parkings = []
        coord_matches = re.findall(r'data-coordinates="([^"]+)"', html)
        link_matches = re.findall(r'href="(/maps/org/[^"]+)"', html)

        # Собираем парковки
        for i, coords in enumerate(coord_matches[:20]):  # Первые 20
            link = link_matches[i] if i < len(link_matches) else None

            # Ищем название вокруг координат
            name = f"Парковка {i + 1}"
            # Ищем текст перед координатами
            text_before = html.split(f'data-coordinates="{coords}"')[0][-200:]
            name_match = re.search(r'>([^<>{}\[\]]{5,50})<', text_before)
            if name_match:
                name = name_match.group(1).strip()

            parkings.append({
                "index": i + 1,
                "name": name,
                "coordinates": coords,
                "link": f"https://yandex.ru{link}" if link else None
            })

        print(f"\n✅ Найдено {len(parkings)} парковок")

        # Быстрый вывод
        for p in parkings[:3]:
            print(f"\n{p['index']}. {p['name']}")
            print(f"   Координаты: {p['coordinates']}")

        # Сохраняем
        with open("quick_parkings.json", "w", encoding="utf-8") as f:
            json.dump(parkings, f, ensure_ascii=False, indent=2)

        print(f"\n💾 Данные сохранены в quick_parkings.json")

    finally:
        # Принудительно завершаем
        print("\nЗавершение работы...")
        import os
        os._exit(0)


def run_with_timeout():
    """Запуск с таймаутом для избежания проблем с закрытием"""
    import threading
    import time

    stop_event = threading.Event()
    result = {"parkings": []}

    def run_parser():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def task():
                browser = await nodriver.start(headless=False)
                page = await browser.get("https://yandex.ru/maps/2/saint-petersburg/search/парковки/")
                await asyncio.sleep(3)

                button = await page.query_selector('span.search-command-view__show-results-button')
                if button:
                    await button.click()
                    await asyncio.sleep(3)

                await asyncio.sleep(5)
                html = await page.evaluate("document.documentElement.outerHTML")

                # Парсинг
                parkings = []
                snippets = re.findall(r'<li[^>]*class="[^"]*search-snippet-view[^"]*"[^>]*>.*?</li>', html, re.DOTALL)

                for i, snippet in enumerate(snippets):
                    coord_match = re.search(r'data-coordinates="([^"]+)"', snippet)
                    if coord_match:
                        parkings.append({
                            "index": i + 1,
                            "coordinates": coord_match.group(1)
                        })

                result["parkings"] = parkings

                # Не закрываем браузер - пусть система сама закроет

            loop.run_until_complete(task())
        except Exception as e:
            print(f"Ошибка: {e}")

    # Запускаем парсер в отдельном потоке
    thread = threading.Thread(target=run_parser)
    thread.start()

    # Ждем максимум 30 секунд
    thread.join(timeout=30)

    if thread.is_alive():
        print("Таймаут! Завершаем принудительно...")
        stop_event.set()

    return result["parkings"]


if __name__ == "__main__":
    print("Выберите вариант парсера:")
    print("1. Полный парсер (рекомендуется)")
    print("2. Быстрый парсер")
    print("3. Парсер с таймаутом (самый стабильный)")

    choice = input("Введите 1, 2 или 3: ").strip()

    if choice == "1":
        asyncio.run(main_parser())
    elif choice == "2":
        asyncio.run(quick_parser())
    else:
        parkings = run_with_timeout()
        print(f"\nНайдено парковок: {len(parkings)}")
        if parkings:
            with open("timeout_parkings.json", "w", encoding="utf-8") as f:
                json.dump(parkings, f, ensure_ascii=False, indent=2)
            print("💾 Данные сохранены в timeout_parkings.json")