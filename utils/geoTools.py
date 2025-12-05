import math


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Вычисляет расстояние между двумя точками на Земле в километрах.
    Формула гаверсинусов.
    """
    R = 6371  # Радиус Земли в км

    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def parse_coordinates(coord_str):
    """
    Парсит строку координат в формате 'широта, долгота'.
    Возвращает кортеж (lat, lon) или None при ошибке.
    """
    if not coord_str:
        return None

    try:
        # Убираем пробелы и разбиваем по разделителям
        coord_str = coord_str.strip()
        for sep in [',', ';', '|', ' ']:
            if sep in coord_str:
                parts = [p.strip() for p in coord_str.split(sep) if p.strip()]
                if len(parts) >= 2:
                    lat = float(parts[0])
                    lon = float(parts[1])
                    return (lat, lon)

        # Если не нашли разделитель
        parts = coord_str.split()
        if len(parts) >= 2:
            return (float(parts[0]), float(parts[1]))

    except (ValueError, AttributeError):
        pass

    return None


def are_coordinates_close(coord1, coord2, threshold_km=0.1):
    """
    Проверяет, находятся ли две координаты близко друг к другу.
    threshold_km: порог в километрах (по умолчанию 100 метров).
    """
    if not coord1 or not coord2:
        return False

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    distance = haversine_distance(lat1, lon1, lat2, lon2)
    return distance <= threshold_km
