import logging
import pymysql
import contextvars
from pymysql.cursors import DictCursor
from src.config import settings


logger = logging.getLogger(__name__)
db_conn_ctx = contextvars.ContextVar('db_conn', default=None)

def get_db_conn():
    return db_conn_ctx.get()

# ---------------------------------------------------------------------
# DB Connection 생성
# ---------------------------------------------------------------------
def transaction(func):
    def wrapper(*args, **kwargs):
        result = None
        conn = get_db_conn()
        if conn is None:
            with pymysql.connect(
                    host=settings.DB_HOST,
                    port=settings.DB_PORT,
                    user=settings.DB_USER,
                    password=settings.DB_PWD,
                    database=settings.DB_NAME,
                    cursorclass=DictCursor
            ) as conn:
                token = db_conn_ctx.set(conn)
                try:
                    result = func(*args, **kwargs)
                    conn.commit()
                except Exception as e:
                    logger.error(e)
                    conn.rollback()
                finally:
                    db_conn_ctx.reset(token)
        else:
            result = func(*args, **kwargs)

        return result
    return wrapper

# ---------------------------------------------------------------------
# DB Session 생성
# ---------------------------------------------------------------------
def repository(func):
    def wrapper(*args, **kwargs):
        conn = get_db_conn()
        if conn is None:
            raise RuntimeError('Database connection failed.')

        with conn.cursor() as cursor:
            kwargs['cursor'] = cursor
            result = func(*args, **kwargs)

        return result
    return wrapper
