class Config:
    # Основные настройки
    SEARCH_QUERY = "закрытая парковка"
    REGION = "Санкт-Петербург"
    CITY_2GIS = "spb"

    # Настройки nodriver
    NODRIVER = {
        'headless': False,  # False для отладки
        'window_size': (1280, 720),
        'timeout': 30000,
    }

    # Настройки парсинга
    PARSING = {
        'max_items_per_source': 3,  # Начинаем с 3 объектов
    }


config = Config()
