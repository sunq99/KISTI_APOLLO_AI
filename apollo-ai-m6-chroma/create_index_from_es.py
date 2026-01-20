import logging
from src.search_engine import create_wiki_index, update_wiki_index
from src.database import repository, transaction


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Elastic Search에 인덱스 추가
# ---------------------------------------------------------------------
@transaction
def insert_indexes():
    create_wiki_index()
    logger.info('Create wiki index')

    total_cnt = 0
    page = 0

    while True:
        rows = select_all_wiki(page=page)
        if not rows:
            break

        update_wiki_index(rows)
        total_cnt += len(rows)
        page += 1
        logger.info(f'Updating {total_cnt} indexes into ES')

    return total_cnt

# ---------------------------------------------------------------------
# wiki item 조회
# ---------------------------------------------------------------------
@repository
def select_all_wiki(page: int = 0, batch_size: int = 500, cursor=None):
    sql = f"""
        SELECT id, title, title_kor 
          FROM wiki_item_info 
         LIMIT %s OFFSET %s
    """
    offset = page * batch_size
    cursor.execute(sql, (batch_size, offset))

    return cursor.fetchmany(batch_size)