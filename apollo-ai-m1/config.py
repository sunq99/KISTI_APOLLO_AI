import os

DB_INFO = {
    "USER" : os.getenv("DB_USER"),
    "PASSWORD" : os.getenv("DB_PASSWORD"),
    "HOST" : os.getenv("DB_HOST"),
    "PORT" : int(os.getenv("DB_PORT")),
    "NAME" : os.getenv("DB_NM"),
}

