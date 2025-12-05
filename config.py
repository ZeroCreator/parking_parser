class Config:
    # Основные настройки
    REGION = "Санкт-Петербург"
    CITY_2GIS = "spb"

    # Настройки nodriver
    NODRIVER = {
        'headless': False,  # False для отладки
        'window_size': (1280, 720),
        'timeout': 30000,
    }

config = Config()
