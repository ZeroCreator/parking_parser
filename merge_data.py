#!/usr/bin/env python3
"""
Утилита для объединения данных парсинга из разных источников
"""

import sys
from pathlib import Path
import argparse
import json
import glob
import os

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.excel_writer import ExcelWriter
from core.data_merger import DataMerger


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


# merge_data.py - исправляем load_excel_data

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
                elif pd.isna(value):
                    item[key] = ''

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


def merge_files(yandex_file: str = None, twogis_file: str = None, output_file: str = None):
    """Основная функция объединения файлов"""
    print("=" * 70)
    print("🔗 ОБЪЕДИНЕНИЕ ДАННЫХ ПАРКОВОК")
    print("=" * 70)

    # Поиск файлов если не указаны
    if not yandex_file:
        # Ищем в разных местах
        search_patterns = [
            "results/*yandex*.json",
            "results/*yandex*.xlsx",
            "*.json",
            "*.xlsx"
        ]

        for pattern in search_patterns:
            yandex_file = find_latest_file(pattern)
            if yandex_file and ('yandex' in yandex_file.lower() or 'яндекс' in yandex_file.lower()):
                break

    if not twogis_file:
        search_patterns = [
            "results/*2gis*.json",
            "results/*2gis*.xlsx",
            "results/*twogis*.json",
            "results/*twogis*.xlsx",
            "*.json",
            "*.xlsx"
        ]

        for pattern in search_patterns:
            twogis_file = find_latest_file(pattern)
            if twogis_file and ('2gis' in twogis_file.lower() or 'twogis' in twogis_file.lower()):
                break

    print(f"⚙ Найденные файлы:")
    print(f"   Яндекс: {yandex_file or 'Не найден'}")
    print(f"   2ГИС: {twogis_file or 'Не найден'}")
    print("-" * 70)

    # Загрузка данных
    yandex_data = []
    twogis_data = []

    if yandex_file:
        print(f"\n📥 Загрузка данных Яндекс из: {os.path.basename(yandex_file)}")
        yandex_data = load_data(yandex_file)
        print(f"   ✅ Загружено {len(yandex_data)} объектов")

    if twogis_file:
        print(f"\n📥 Загрузка данных 2ГИС из: {os.path.basename(twogis_file)}")
        twogis_data = load_data(twogis_file)
        print(f"   ✅ Загружено {len(twogis_data)} объектов")

    if not yandex_data and not twogis_data:
        print("\n❌ Нет данных для объединения")
        print("\n💡 СОВЕТЫ:")
        print("1. Укажите файлы вручную:")
        print("   python merge_data.py --yandex-file путь/к/файлу.json --twogis-file путь/к/файлу.json")
        print("\n2. Положите файлы в папку 'results/' с именами содержащими:")
        print("   'yandex' или 'яндекс' для Яндекс данных")
        print("   '2gis' или 'twogis' для 2ГИС данных")
        print("\n3. Форматы файлов: .json или .xlsx")
        return

    # Объединение
    print("\n🔗 Объединение данных...")
    writer = ExcelWriter()
    merger = DataMerger()

    merged_data = []
    if yandex_data and twogis_data:
        merged_data = merger.merge_data(yandex_data, twogis_data)
        print(f"   ✅ Объединено: {len(merged_data)} объектов")
    elif yandex_data:
        merged_data = [merger._create_unique_object(obj, 'yandex') for obj in yandex_data]
        print(f"   📊 Только Яндекс: {len(merged_data)} объектов")
    elif twogis_data:
        merged_data = [merger._create_unique_object(obj, '2gis') for obj in twogis_data]
        print(f"   📊 Только 2ГИС: {len(merged_data)} объектов")

    # Сохранение
    print("\n💾 Сохранение результатов...")
    if output_file:
        # Используем указанное имя файла
        output_path = Path(output_file)
        if not output_path.suffix:
            output_path = output_path.with_suffix('.xlsx')
    else:
        # Автоматическое имя
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = writer.output_dir / f"merged_parking_{timestamp}.xlsx"

    # Используем стандартный метод сохранения
    merged_file = writer.save_merged_results(yandex_data, twogis_data, merged_data)

    print("\n" + "=" * 70)
    print("✅ ОБЪЕДИНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 70)
    print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Яндекс: {len(yandex_data)} объектов")
    print(f"   2ГИС: {len(twogis_data)} объектов")
    print(f"   Объединено: {len(merged_data)} объектов")
    print(f"\n📁 Результат сохранен в:")
    print(f"   {merged_file}")
    print("\n📋 Листы в файле:")
    print("   1. Объединенные данные (сравнение Яндекс и 2ГИС)")
    print("   2. Яндекс Карты (оригинальные данные)")
    print("   3. 2ГИС (оригинальные данные)")
    print("   4. Сводка (статистика)")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Объединение данных парсинга парковок из Яндекс Карт и 2ГИС'
    )

    parser.add_argument('--yandex-file', '-y', type=str,
                        help='Путь к файлу с данными Яндекс (JSON или Excel)')

    parser.add_argument('--twogis-file', '-t', type=str,
                        help='Путь к файлу с данными 2ГИС (JSON или Excel)')

    parser.add_argument('--output', '-o', type=str,
                        help='Путь для сохранения объединенного файла (по умолчанию: results/merged_...xlsx)')

    parser.add_argument('--auto', action='store_true',
                        help='Автоматический поиск последних файлов в папке results/')

    parser.add_argument('--list-files', action='store_true',
                        help='Показать доступные файлы для объединения')

    args = parser.parse_args()

    # Показать список файлов
    if args.list_files:
        print("📁 Доступные файлы для объединения:")
        print("\nФайлы Яндекс:")
        for file in glob.glob("results/*yandex*") + glob.glob("*.json"):
            if os.path.isfile(file):
                mtime = os.path.getmtime(file)
                from datetime import datetime
                date_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                print(f"  {file} ({date_str})")

        print("\nФайлы 2ГИС:")
        for file in glob.glob("results/*2gis*") + glob.glob("results/*twogis*"):
            if os.path.isfile(file):
                mtime = os.path.getmtime(file)
                from datetime import datetime
                date_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                print(f"  {file} ({date_str})")
        return

    # Объединение
    merge_files(args.yandex_file, args.twogis_file, args.output)


if __name__ == "__main__":
    main()
