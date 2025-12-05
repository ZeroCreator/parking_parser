import math
import re
from typing import List, Dict, Any, Tuple, Optional
from difflib import SequenceMatcher


class DataMerger:
    """Класс для объединения данных из разных источников"""

    def __init__(self, coord_tolerance: float = 0.001, name_similarity: float = 0.7):
        """
        Инициализация мерджера

        Args:
            coord_tolerance: Допуск по координатам в градусах
            name_similarity: Порог схожести названий (0.0-1.0)
        """
        self.coord_tolerance = coord_tolerance
        self.name_similarity = name_similarity

    def parse_coordinates(self, coord_str: str) -> Optional[Tuple[float, float]]:
        """Парсинг строки с координатами в числа"""
        if not coord_str:
            return None

        coord_str = str(coord_str)

        if coord_str.lower() in ['nan', 'none', 'null', '']:
            return None

        try:
            # Убираем пробелы, скобки и т.д.
            clean = re.sub(r'[^\d.,\s-]', '', coord_str)
            parts = re.split(r'[,;\s]+', clean)

            if len(parts) >= 2:
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
                return (lat, lon)
        except:
            pass

        return None

    def coordinates_match(self, coord1: str, coord2: str) -> bool:
        """Проверка совпадения координат"""
        if not coord1 or not coord2:
            return False

        # Проверяем на NaN и другие невалидные значения
        coord1_str = str(coord1)
        coord2_str = str(coord2)

        if not coord1_str or not coord2_str or coord1_str.lower() in ['nan', 'none', 'null'] or coord2_str.lower() in [
            'nan', 'none', 'null']:
            return False

        parsed1 = self.parse_coordinates(coord1_str)
        parsed2 = self.parse_coordinates(coord2_str)

        if not parsed1 or not parsed2:
            return False

        lat1, lon1 = parsed1
        lat2, lon2 = parsed2

        lat_diff = abs(lat1 - lat2)
        lon_diff = abs(lon1 - lon2)

        return lat_diff <= self.coord_tolerance and lon_diff <= self.coord_tolerance

    def normalize_text(self, text: Any) -> str:
        """Нормализация текста для сравнения"""
        if text is None:
            return ""

        # Конвертируем в строку если не строка
        if not isinstance(text, str):
            text = str(text)

        # Приводим к нижнему регистру
        text = text.lower()

        # Убираем лишние символы и слова
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text)

        # Убираем стоп-слова
        stop_words = {'ооо', 'зао', 'оао', 'торговый', 'центр', 'тц', 'тк', 'парковка', 'стоянка'}
        words = text.split()
        words = [w for w in words if w not in stop_words]

        return ' '.join(words).strip()

    def text_similarity(self, text1: Any, text2: Any) -> float:
        """Вычисление схожести текстов"""
        norm1 = self.normalize_text(text1)
        norm2 = self.normalize_text(text2)

        if not norm1 or not norm2:
            return 0.0

        return SequenceMatcher(None, norm1, norm2).ratio()

    def address_match(self, addr1: Any, addr2: Any) -> bool:
        """Проверка совпадения адресов"""
        addr1_str = str(addr1) if addr1 is not None else ""
        addr2_str = str(addr2) if addr2 is not None else ""

        if not addr1_str or not addr2_str:
            return False

        # Убираем номера домов, квартир и т.д.
        clean1 = re.sub(r'[,\d-]', '', addr1_str).lower().strip()
        clean2 = re.sub(r'[,\d-]', '', addr2_str).lower().strip()

        # Сравниваем ключевые слова
        words1 = set(clean1.split())
        words2 = set(clean2.split())

        common = words1.intersection(words2)
        return len(common) >= 2  # Хотя бы 2 общих слова

    def merge_objects(self, yandex_obj: Dict[str, Any], twogis_obj: Dict[str, Any]) -> Dict[str, Any]:
        """Объединение двух объектов в один"""
        # Проверяем что объекты не None
        if not yandex_obj:
            yandex_obj = {}
        if not twogis_obj:
            twogis_obj = {}

        # Очищаем значения от None
        def clean_value(val):
            if val is None:
                return ''
            if isinstance(val, float) and math.isnan(val):
                return ''
            return str(val).strip()

        merged = {
            'Объект': clean_value(yandex_obj.get('Название объекта')) or clean_value(
                twogis_obj.get('Название объекта')),
            'Координаты (общие)': clean_value(yandex_obj.get('Координаты')) or clean_value(
                twogis_obj.get('Координаты')),
            'Адрес (общий)': clean_value(yandex_obj.get('Адрес')) or clean_value(twogis_obj.get('Адрес')),
            'Телефон (общий)': clean_value(yandex_obj.get('Телефон')) or clean_value(twogis_obj.get('Телефон')),
            'Сайт (общий)': clean_value(yandex_obj.get('Сайт')) or clean_value(twogis_obj.get('Сайт')),
            'Тип объекта (общий)': clean_value(yandex_obj.get('Тип объекта')) or clean_value(
                twogis_obj.get('Тип объекта')),
            'Название парковки (общее)': clean_value(yandex_obj.get('Название парковки')) or clean_value(
                twogis_obj.get('Название парковки')),
            'Описание Яндекс': clean_value(yandex_obj.get('Описание', '')),
            'Данные Яндекс Карт': self._format_yandex_data(yandex_obj),
            'Данные 2ГИС': self._format_twogis_data(twogis_obj),
            'Конфликт данных': '',
            'Примечания': ''
        }

        # Объединение числовых данных
        merged.update(self._merge_numeric_data(yandex_obj, twogis_obj))

        # Проверка конфликтов
        conflicts = self._find_conflicts(yandex_obj, twogis_obj)
        if conflicts:
            merged['Конфликт данных'] = '; '.join(conflicts)
            merged['Примечания'] = 'Требуется ручная проверка'

        return merged

    def _format_yandex_data(self, data: Dict[str, Any]) -> str:
        """Форматирование данных Яндекс для ячейки"""
        parts = []

        if data.get('Название объекта'):
            parts.append(data['Название объекта'])

        if data.get('Ссылка'):
            parts.append(data['Ссылка'])

        if data.get('Тип объекта'):
            parts.append(data['Тип объекта'])

        if data.get('Адрес'):
            parts.append(data['Адрес'])

        if data.get('Телефон'):
            parts.append(f"Телефон: {data['Телефон']}")

        if data.get('Тип парковки'):
            parts.append(f"Тип: {data['Тип парковки']}")

        if data.get('Тарифы'):
            parts.append(f"Тарифы: {data['Тарифы']}")

        return '\n'.join(parts)

    def _format_twogis_data(self, data: Dict[str, Any]) -> str:
        """Форматирование данных 2ГИС для ячейки"""
        parts = []

        if data.get('Название объекта'):
            parts.append(data['Название объекта'])

        if data.get('Ссылка'):
            parts.append(data['Ссылка'])

        if data.get('Тип объекта'):
            parts.append(data['Тип объекта'])

        if data.get('Адрес'):
            parts.append(data['Адрес'])

        if data.get('Телефон'):
            parts.append(f"Телефон: {data['Телефон']}")

        if data.get('Тип парковки'):
            parts.append(f"Тип: {data['Тип парковки']}")

        if data.get('Тарифы'):
            parts.append(f"Тарифы: {data['Тарифы']}")

        if data.get('Оценка'):
            parts.append(f"Оценка: {data['Оценка']}")

        if data.get('Количество оценок'):
            parts.append(f"Оценок: {data['Количество оценок']}")

        if data.get('Время работы'):
            parts.append(f"Время работы: {data['Время работы']}")

        if data.get('Вместимость'):
            parts.append(f"Мест: {data['Вместимость']}")

        if data.get('Отзывы'):
            parts.append(f"Отзывы: {data['Отзывы']}")

        return '\n'.join(parts)

    # data_merger.py - исправляем метод _merge_numeric_data

    def _merge_numeric_data(self, yandex: Dict, twogis: Dict) -> Dict[str, Any]:
        """Объединение числовых и текстовых данных"""
        result = {}

        # Тип парковки - приоритет у более конкретного
        type1 = str(yandex.get('Тип парковки', '')).lower()
        type2 = str(twogis.get('Тип парковки', '')).lower()

        if 'закрытая' in type2 or 'охраняемая' in type2:
            result['Тип парковки (итоговый)'] = twogis.get('Тип парковки', '')
            result['Доступ (итоговый)'] = twogis.get('Доступ', '')
        elif 'закрытая' in type1 or 'охраняемая' in type1:
            result['Тип парковки (итоговый)'] = yandex.get('Тип парковки', '')
            result['Доступ (итоговый)'] = yandex.get('Доступ', '')
        elif type1 != 'unknown':
            result['Тип парковки (итоговый)'] = yandex.get('Тип парковки', '')
        else:
            result['Тип парковки (итоговый)'] = twogis.get('Тип парковки', '')

        # Тарифы и цены - объединяем
        tariffs = []
        if yandex.get('Тарифы'):
            tariffs.append(f"Яндекс: {yandex['Тарифы']}")
        if twogis.get('Тарифы'):
            tariffs.append(f"2ГИС: {twogis['Тарифы']}")

        result['Тарифы (итоговые)'] = ' | '.join(tariffs) if tariffs else ''
        result['Цены (итоговые)'] = yandex.get('Цены') or twogis.get('Цены') or ''

        # Время работы - приоритет у Яндекс (обычно точнее)
        result['Время работы (итоговое)'] = yandex.get('Время работы') or twogis.get('Время работы') or ''

        # Вместимость - берем максимальную, но только если оба числа или один не None
        cap1 = self._extract_number(yandex.get('Вместимость', ''))
        cap2 = self._extract_number(twogis.get('Вместимость', ''))

        if cap1 is not None and cap2 is not None:
            capacity = max(cap1, cap2)
            result['Вместимость (итоговая)'] = str(capacity)
        elif cap1 is not None:
            result['Вместимость (итоговая)'] = str(cap1)
        elif cap2 is not None:
            result['Вместимость (итоговая)'] = str(cap2)
        else:
            result['Вместимость (итоговая)'] = ''

        # Оценки - среднее арифметическое
        rating1 = self._extract_float(yandex.get('Оценка', ''))
        rating2 = self._extract_float(twogis.get('Оценка', ''))

        if rating1 is not None and rating2 is not None:
            result['Оценка (средняя)'] = f"{(rating1 + rating2) / 2:.1f}"
        elif rating1 is not None:
            result['Оценка (средняя)'] = str(rating1)
        elif rating2 is not None:
            result['Оценка (средняя)'] = str(rating2)
        else:
            result['Оценка (средняя)'] = ''

        # Количество оценок - сумма
        count1 = self._extract_number(yandex.get('Количество оценок', ''))
        count2 = self._extract_number(twogis.get('Количество оценок', ''))

        total = 0
        if count1 is not None:
            total += count1
        if count2 is not None:
            total += count2

        result['Количество оценок (сумма)'] = str(total) if total > 0 else ''

        return result

    def _extract_number(self, text: Any) -> Optional[int]:
        """Извлечение числа из текста"""
        if text is None:
            return None

        text_str = str(text)
        if not text_str or text_str.lower() in ['nan', 'none', 'null', '']:
            return None

        match = re.search(r'(\d+)', text_str)
        return int(match.group(1)) if match else None

    def _extract_float(self, text: Any) -> Optional[float]:
        """Извлечение дробного числа из текста"""
        if text is None:
            return None

        text_str = str(text)
        if not text_str or text_str.lower() in ['nan', 'none', 'null', '']:
            return None

        match = re.search(r'(\d+\.?\d*)', text_str)
        return float(match.group(1)) if match else None

    def _find_conflicts(self, yandex: Dict, twogis: Dict) -> List[str]:
        """Поиск конфликтующих данных"""
        conflicts = []

        # Конфликт по типу парковки
        type1 = str(yandex.get('Тип парковки', '')).lower()
        type2 = str(twogis.get('Тип парковки', '')).lower()

        if type1 and type2 and type1 != 'unknown' and type2 != 'unknown':
            if ('платная' in type1) != ('платная' in type2):
                conflicts.append('Конфликт платности')
            if ('закрытая' in type1) != ('закрытая' in type2):
                conflicts.append('Конфликт типа доступа')

        # Конфликт по ценам
        price1 = yandex.get('Цены', '')
        price2 = twogis.get('Цены', '')

        if price1 and price2 and price1 != price2:
            conflicts.append('Разные цены')

        return conflicts

    def find_matches(self, yandex_data: List[Dict], twogis_data: List[Dict]) -> List[Tuple[int, int, float]]:
        """
        Поиск совпадений между данными

        Returns:
            Список кортежей (индекс_яндекс, индекс_2гис, уверенность)
        """
        matches = []

        for i, y_obj in enumerate(yandex_data):
            best_match = None
            best_score = 0

            for j, t_obj in enumerate(twogis_data):
                score = self.calculate_match_score(y_obj, t_obj)

                if score > best_score and score >= 0.5:  # Порог совпадения
                    best_score = score
                    best_match = (i, j, score)

            if best_match:
                matches.append(best_match)

        return matches

    # data_merger.py - в методе calculate_match_score

    def calculate_match_score(self, yandex_obj: Dict, twogis_obj: Dict) -> float:
        """Вычисление оценки совпадения объектов"""
        scores = []
        weights = []

        # 1. Координаты (самый важный критерий)
        coord1 = yandex_obj.get('Координаты', '')
        coord2 = twogis_obj.get('Координаты', '')

        if coord1 and coord2:
            # Убедимся что это строки
            coord1_str = str(coord1) if coord1 is not None else ''
            coord2_str = str(coord2) if coord2 is not None else ''

            if coord1_str and coord2_str:
                if self.coordinates_match(coord1_str, coord2_str):
                    scores.append(1.0)
                    weights.append(3.0)  # Высокий вес
                else:
                    scores.append(0.0)
                    weights.append(3.0)

        # 2. Название
        name1 = yandex_obj.get('Название объекта', '')
        name2 = twogis_obj.get('Название объекта', '')

        if name1 and name2:
            similarity = self.text_similarity(name1, name2)
            scores.append(similarity)
            weights.append(2.0)  # Средний вес

        # 3. Адрес
        addr1 = yandex_obj.get('Адрес', '')
        addr2 = twogis_obj.get('Адрес', '')

        if addr1 and addr2:
            addr_sim = self.text_similarity(addr1, addr2)
            scores.append(addr_sim)
            weights.append(1.5)

        # 4. Телефон (если есть)
        phone1 = yandex_obj.get('Телефон', '')
        phone2 = twogis_obj.get('Телефон', '')

        if phone1 and phone2:
            # Конвертируем в строки
            phone1_str = str(phone1) if phone1 is not None else ''
            phone2_str = str(phone2) if phone2 is not None else ''

            # Нормализуем телефоны
            norm1 = re.sub(r'[^\d]', '', phone1_str)
            norm2 = re.sub(r'[^\d]', '', phone2_str)

            if norm1 and norm2:
                # Проверяем полное совпадение или совпадение последних 7 цифр
                if norm1 == norm2 or norm1[-7:] == norm2[-7:]:
                    scores.append(1.0)
                else:
                    scores.append(0.0)
                weights.append(2.0)

        # Если нет данных для сравнения, возвращаем 0
        if not scores:
            return 0.0

        # Взвешенное среднее
        total_weight = sum(weights)
        weighted_sum = sum(s * w for s, w in zip(scores, weights))

        return weighted_sum / total_weight if total_weight > 0 else 0.0

    def merge_data(self, yandex_data: List[Dict], twogis_data: List[Dict]) -> List[Dict]:
        """
        Основной метод объединения данных

        Returns:
            Список объединенных объектов
        """
        if not yandex_data or not twogis_data:
            print("⚠ Нет данных для объединения")
            return []

        print(f"Объединение: Яндекс={len(yandex_data)}, 2ГИС={len(twogis_data)}")

        # Находим совпадения
        matches = self.find_matches(yandex_data, twogis_data)
        print(f"Найдено совпадений: {len(matches)}")

        # Собираем индексы уже использованных объектов
        used_yandex = set()
        used_twogis = set()
        merged_results = []

        # Объединяем совпадающие объекты
        for y_idx, t_idx, score in matches:
            if y_idx not in used_yandex and t_idx not in used_twogis:
                merged_obj = self.merge_objects(yandex_data[y_idx], twogis_data[t_idx])
                merged_obj['Уверенность совпадения'] = f"{score:.2f}"
                merged_results.append(merged_obj)

                used_yandex.add(y_idx)
                used_twogis.add(t_idx)

        # Добавляем уникальные объекты из Яндекс
        for i, obj in enumerate(yandex_data):
            if i not in used_yandex:
                merged_obj = self._create_unique_object(obj, source='yandex')
                merged_results.append(merged_obj)

        # Добавляем уникальные объекты из 2ГИС
        for i, obj in enumerate(twogis_data):
            if i not in used_twogis:
                merged_obj = self._create_unique_object(obj, source='2gis')
                merged_results.append(merged_obj)

        print(f"Итоговых объектов: {len(merged_results)}")
        return merged_results

    def _create_unique_object(self, obj: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Создание записи для уникального объекта (без совпадения)"""
        if source == 'yandex':
            return {
                'Объект': obj.get('Название объекта', ''),
                'Координаты (общие)': obj.get('Координаты', ''),
                'Адрес (общий)': obj.get('Адрес', ''),
                'Телефон (общий)': obj.get('Телефон', ''),
                'Сайт (общий)': obj.get('Сайт', ''),
                'Тип объекта (общий)': obj.get('Тип объекта', ''),
                'Название парковки (общее)': obj.get('Название парковки', ''),
                'Тип парковки (итоговый)': obj.get('Тип парковки', ''),
                'Доступ (итоговый)': obj.get('Доступ', ''),
                'Тарифы (итоговые)': obj.get('Тарифы', ''),
                'Цены (итоговые)': obj.get('Цены', ''),
                'Время работы (итоговое)': obj.get('Время работы', ''),
                'Вместимость (итоговая)': obj.get('Вместимость', ''),
                'Оценка (средняя)': obj.get('Оценка', ''),
                'Количество оценок (сумма)': obj.get('Количество оценок', ''),
                'Описание Яндекс': obj.get('Описание', ''),
                'Данные Яндекс Карт': self._format_yandex_data(obj),
                'Данные 2ГИС': '',
                'Конфликт данных': 'Только в Яндекс Картах',
                'Примечания': 'Нет данных в 2ГИС'
            }
        else:  # 2gis
            return {
                'Объект': obj.get('Название объекта', ''),
                'Координаты (общие)': obj.get('Координаты', ''),
                'Адрес (общий)': obj.get('Адрес', ''),
                'Телефон (общий)': obj.get('Телефон', ''),
                'Сайт (общий)': obj.get('Сайт', ''),
                'Тип объекта (общий)': obj.get('Тип объекта', ''),
                'Название парковки (общее)': obj.get('Название парковки', ''),
                'Тип парковки (итоговый)': obj.get('Тип парковки', ''),
                'Доступ (итоговый)': obj.get('Доступ', ''),
                'Тарифы (итоговые)': obj.get('Тарифы', ''),
                'Цены (итоговые)': obj.get('Цены', ''),
                'Время работы (итоговое)': obj.get('Время работы', ''),
                'Вместимость (итоговая)': obj.get('Вместимость', ''),
                'Оценка (средняя)': obj.get('Оценка', ''),
                'Количество оценок (сумма)': obj.get('Количество оценок', ''),
                'Описание Яндекс': '',
                'Данные Яндекс Карт': '',
                'Данные 2ГИС': self._format_twogis_data(obj),
                'Конфликт данных': 'Только в 2ГИС',
                'Примечания': 'Нет данных в Яндекс Картах'
            }

    def merge_objects_for_excel(self, yandex_obj: Dict[str, Any], twogis_obj: Dict[str, Any]) -> Dict[str, Any]:
        """Объединение двух объектов для Excel с двухстрочным заголовком"""
        merged = {
            'Объект': yandex_obj.get('Название объекта') or twogis_obj.get('Название объекта'),

            # Для Excel с двумя колонками на поле
            'Данные Яндекс Карт': self._format_for_excel(yandex_obj),
            'Данные 2ГИС': self._format_for_excel(twogis_obj),

            # Индивидуальные поля для сравнения
            'Адрес Яндекс': yandex_obj.get('Адрес', ''),
            'Адрес 2ГИС': twogis_obj.get('Адрес', ''),

            'Телефон Яндекс': yandex_obj.get('Телефон', ''),
            'Телефон 2ГИС': twogis_obj.get('Телефон', ''),

            'Тип парковки Яндекс': yandex_obj.get('Тип парковки', ''),
            'Тип парковки 2ГИС': twogis_obj.get('Тип парковки', ''),

            'Описание Яндекс': yandex_obj.get('Описание', ''),

            'Конфликт данных': '',
            'Примечания': '',
            'Уверенность совпадения': ''
        }

        # Добавляем координаты если есть
        if yandex_obj.get('Координаты'):
            merged['Координаты Яндекс'] = yandex_obj['Координаты']
        if twogis_obj.get('Координаты'):
            merged['Координаты 2ГИС'] = twogis_obj['Координаты']

        return merged

    def _format_for_excel(self, data: Dict[str, Any]) -> str:
        """Форматирование данных для компактного отображения в Excel"""
        if not data:
            return ""

        parts = []

        # Основные поля
        fields_to_show = [
            ('Название', 'Название объекта'),
            ('Адрес', 'Адрес'),
            ('Телефон', 'Телефон'),
            ('Сайт', 'Сайт'),
            ('Тип', 'Тип парковки'),
            ('Тарифы', 'Тарифы'),
            ('Время работы', 'Время работы'),
            ('Мест', 'Вместимость'),
            ('Оценка', 'Оценка'),
        ]

        for label, field in fields_to_show:
            value = data.get(field)
            if value:
                parts.append(f"{label}: {value}")

        # Ссылка в конце
        if data.get('Ссылка'):
            parts.append(f"Ссылка: {data['Ссылка']}")

        return '\n'.join(parts)