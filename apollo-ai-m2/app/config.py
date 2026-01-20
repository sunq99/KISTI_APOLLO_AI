import os

# DB정보
DB_INFO = {
    "ID" : os.getenv("DB_USER"),
    "PW" : os.getenv("DB_PASSWORD"),
    "HOST" : os.getenv("DB_HOST"),
    "PORT" : int(os.getenv("DB_PORT")),
    "DB" : os.getenv("DB_NM"),
}
