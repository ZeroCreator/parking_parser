import asyncio
import sys
from pathlib import Path
import argparse

# Добавляем путь
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from parsers.twogis_parser import TwoGisParser
from parsers.yandex_parser import YandexParser
from core.excel_writer import ExcelWriter
from core.data_merger import DataMerger


def parse_arguments():
    """Парсинг аргументов командной строки - УПРОЩЕННАЯ ВЕРСИЯ"""
    parser = argparse.ArgumentParser(description='Парсинг парковок из Яндекс Карт и 2ГИС')

    parser.add_argument('--headless', action='store_true',
                        help='Запуск в headless режиме (без интерфейса браузера)')

    parser.add_argument('--skip-yandex', action='store_true',
                        help='Пропустить парсинг Яндекс Карт')

    parser.add_argument('--skip-2gis', action='store_true',
                        help='Пропустить парсинг 2ГИС')

    return parser.parse_args()


async def main():
    args = parse_arguments()

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
    if yandex_data:
        yandex_file = writer.save_parser_results(yandex_data, 'yandex')
        print(f"   📁 Яндекс сохранен: {yandex_file}")
    else:
        print("   ⚠ Нет данных Яндекс Карт для сохранения")

    if twogis_data:
        twogis_file = writer.save_parser_results(twogis_data, '2gis')
        print(f"   📁 2ГИС сохранен: {twogis_file}")
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
    print(f"📁 Результаты сохранены в папке 'results/'")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())


# python main.py --skip-2gis
# python main.py --skip-yandex
