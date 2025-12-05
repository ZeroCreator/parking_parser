import asyncio
import sys
from pathlib import Path
import argparse
import json
import glob
import os

# Добавляем путь
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from parsers.twogis_parser import TwoGisParser
from parsers.yandex_parser import YandexParser
from core.excel_writer import ExcelWriter
from core.data_merger import DataMerger


def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Парсинг парковок из Яндекс Карт и 2ГИС')

    parser.add_argument('--headless', action='store_true',
                        help='Запуск в headless режиме (без интерфейса браузера)')

    parser.add_argument('--skip-yandex', action='store_true',
                        help='Пропустить парсинг Яндекс Карт')

    parser.add_argument('--skip-2gis', action='store_true',
                        help='Пропустить парсинг 2ГИС')

    parser.add_argument('--merge-only', action='store_true',
                        help='Только объединение существующих данных (без парсинга)')

    parser.add_argument('--yandex-file', type=str, default='',
                        help='Путь к файлу с данными Яндекс (JSON или Excel)')

    parser.add_argument('--twogis-file', type=str, default='',
                        help='Путь к файлу с данными 2ГИС (JSON или Excel)')

    return parser.parse_args()


def find_latest_file(pattern: str) -> str:
    """Поиск самого свежего файла по паттерну"""
    files = glob.glob(pattern)
    if not files:
        return None

    # Сортируем по времени создания (новые сначала)
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def load_json_data(filepath: str) -> list:
    """Загрузка данных из JSON файла"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Ошибка загрузки JSON {filepath}: {e}")
        return []


# main.py - исправляем функцию load_excel_data

def load_excel_data(filepath: str) -> list:
    """Загрузка данных из Excel файла"""
    try:
        import pandas as pd
        df = pd.read_excel(filepath)

        # Конвертируем все значения в строки
        df = df.astype(str)

        # Заменяем 'nan' на пустые строки
        df = df.replace('nan', '')

        # Конвертируем DataFrame в список словарей
        data = df.to_dict('records')

        # Очищаем данные от лишних пробелов
        for item in data:
            for key, value in item.items():
                if isinstance(value, str):
                    item[key] = value.strip()

        return data
    except Exception as e:
        print(f"❌ Ошибка загрузки Excel {filepath}: {e}")
        import traceback
        traceback.print_exc()
        return []


def load_data(filepath: str) -> list:
    """Загрузка данных из файла (определяет формат автоматически)"""
    if not filepath or not os.path.exists(filepath):
        return []

    if filepath.endswith('.json'):
        return load_json_data(filepath)
    elif filepath.endswith(('.xlsx', '.xls')):
        return load_excel_data(filepath)
    else:
        print(f"❌ Неподдерживаемый формат файла: {filepath}")
        return []


async def merge_existing_data(yandex_file: str = None, twogis_file: str = None):
    """Объединение существующих данных из файлов"""
    print("=" * 70)
    print("🔗 ОБЪЕДИНЕНИЕ СУЩЕСТВУЮЩИХ ДАННЫХ")
    print("=" * 70)

    # Если файлы не указаны, ищем последние в папке results
    if not yandex_file:
        yandex_file = find_latest_file("results/*yandex*.json") or find_latest_file("results/*yandex*.xlsx")

    if not twogis_file:
        twogis_file = find_latest_file("results/*2gis*.json") or find_latest_file("results/*2gis*.xlsx")

    print(f"⚙ Настройки объединения:")
    print(f"   Яндекс файл: {yandex_file or 'Не найден'}")
    print(f"   2ГИС файл: {twogis_file or 'Не найден'}")
    print("-" * 70)

    if not yandex_file and not twogis_file:
        print("❌ Не найдены файлы с данными для объединения")
        print("ℹ️  Используйте:")
        print("   --yandex-file <путь> - указать файл Яндекс")
        print("   --twogis-file <путь> - указать файл 2ГИС")
        print("ℹ️  Или поместите файлы в папку 'results/' с именами:")
        print("   *yandex*.json или *yandex*.xlsx")
        print("   *2gis*.json или *2gis*.xlsx")
        return

    # Загрузка данных
    yandex_data = []
    twogis_data = []

    if yandex_file:
        print(f"\n📥 Загрузка данных Яндекс из: {yandex_file}")
        yandex_data = load_data(yandex_file)
        print(f"   ✅ Загружено {len(yandex_data)} объектов")

    if twogis_file:
        print(f"\n📥 Загрузка данных 2ГИС из: {twogis_file}")
        twogis_data = load_data(twogis_file)
        print(f"   ✅ Загружено {len(twogis_data)} объектов")

    if not yandex_data and not twogis_data:
        print("❌ Нет данных для объединения")
        return

    # Объединение данных
    print("\n🔗 Объединение данных...")
    writer = ExcelWriter()
    merger = DataMerger()

    merged_data = []
    if yandex_data and twogis_data:
        merged_data = merger.merge_data(yandex_data, twogis_data)
        print(f"   ✅ Объединено: {len(merged_data)} объектов")
    elif yandex_data:
        # Только Яндекс данные
        merged_data = [merger._create_unique_object(obj, 'yandex') for obj in yandex_data]
        print(f"   📊 Только Яндекс: {len(merged_data)} объектов")
    elif twogis_data:
        # Только 2ГИС данные
        merged_data = [merger._create_unique_object(obj, '2gis') for obj in twogis_data]
        print(f"   📊 Только 2ГИС: {len(merged_data)} объектов")

    # Сохранение объединенного файла
    print("\n💾 Сохранение объединенного файла...")
    merged_file = writer.save_merged_results(yandex_data, twogis_data, merged_data)

    print("\n" + "=" * 70)
    print("✅ ОБЪЕДИНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 70)
    print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Яндекс: {len(yandex_data)} объектов")
    print(f"   2ГИС: {len(twogis_data)} объектов")
    print(f"   Объединено: {len(merged_data)} объектов")
    print(f"📁 Результат: {merged_file}")
    print("=" * 70)


async def main():
    args = parse_arguments()

    # Если указан режим только объединения
    if args.merge_only:
        await merge_existing_data(args.yandex_file, args.twogis_file)
        return

    print("=" * 70)
    print("🚗 ПАРСИНГ ПАРКОВОК: Яндекс Карты + 2ГИС")
    print("=" * 70)

    print(f"⚙ Настройки:")
    print(f"   Headless режим: {'Да' if args.headless else 'Нет'}")
    print(f"   Яндекс Карты: {'Пропущено' if args.skip_yandex else 'Включено'}")
    print(f"   2ГИС: {'Пропущено' if args.skip_2gis else 'Включено'}")
    print("-" * 70)

    # Инициализация
    writer = ExcelWriter()
    yandex_data = []
    twogis_data = []

    if not args.skip_yandex:
        print("\n1. 📍 Парсинг Яндекс Карт...")
        yandex_parser = YandexParser(headless=args.headless)
        yandex_data = await yandex_parser.parse()
        print(f"   ✅ Яндекс: собрано {len(yandex_data)} объектов")

    if not args.skip_2gis:
        print("\n2. 🗺️ Парсинг 2ГИС...")
        twogis_parser = TwoGisParser(headless=args.headless)
        twogis_data = await twogis_parser.parse()
        print(f"   ✅ 2ГИС: собрано {len(twogis_data)} объектов")

    # Сохранение отдельных файлов
    print("\n3. 💾 Сохранение отдельных файлов...")
    yandex_files = []
    twogis_files = []

    if yandex_data:
        yandex_file = writer.save_parser_results(yandex_data, 'yandex')
        yandex_files.append(yandex_file)
        print(f"   📁 Яндекс сохранен: {yandex_file}")

        # Сохраняем также в JSON для удобства объединения
        import json
        timestamp = writer.create_timestamp()
        json_file = writer.output_dir / f"parking_yandex_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(yandex_data, f, ensure_ascii=False, indent=2)
        yandex_files.append(str(json_file))
        print(f"   📁 Яндекс JSON: {json_file}")
    else:
        print("   ⚠ Нет данных Яндекс Карт для сохранения")

    if twogis_data:
        twogis_file = writer.save_parser_results(twogis_data, '2gis')
        twogis_files.append(twogis_file)
        print(f"   📁 2ГИС сохранен: {twogis_file}")

        # Сохраняем также в JSON для удобства объединения
        import json
        timestamp = writer.create_timestamp()
        json_file = writer.output_dir / f"parking_2gis_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(twogis_data, f, ensure_ascii=False, indent=2)
        twogis_files.append(str(json_file))
        print(f"   📁 2ГИС JSON: {json_file}")
    else:
        print("   ⚠ Нет данных 2ГИС для сохранения")

    # Объединение данных
    print("\n4. 🔗 Объединение данных...")
    if yandex_data and twogis_data:
        merger = DataMerger()
        merged_data = merger.merge_data(yandex_data, twogis_data)

        # Сохранение объединенного файла
        print("\n5. 📊 Создание объединенного отчета...")
        merged_file = writer.save_merged_results(yandex_data, twogis_data, merged_data)
        print(f"   📁 Объединенный файл: {merged_file}")
    else:
        print("   ⚠ Недостаточно данных для объединения")
        merged_data = []

    print("\n" + "=" * 70)
    print("✅ ВЫПОЛНЕНО!")
    print("=" * 70)

    # Итоговая статистика
    print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Яндекс Карт: {len(yandex_data)} объектов")
    print(f"   2ГИС: {len(twogis_data)} объектов")
    if merged_data:
        print(f"   Объединено: {len(merged_data)} объектов")

    # Сообщение о возможности объединения позже
    print(f"\n💡 ДЛЯ ОБЪЕДИНЕНИЯ ПОЗЖЕ:")
    if yandex_files:
        print(f"   Яндекс файлы: {', '.join(yandex_files)}")
    if twogis_files:
        print(f"   2ГИС файлы: {', '.join(twogis_files)}")
    print(f"   Команда для объединения:")
    print(f"   python main.py --merge-only")
    print(f"   Или с указанием файлов:")
    print(f"   python main.py --merge-only --yandex-file <путь> --twogis-file <путь>")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
