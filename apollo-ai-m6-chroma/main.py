import logging
from flask import Flask, request, jsonify
from plib_new import kwd, kwdForName
from src.config import settings
from src.vector_db import init_vector_db
from src.search_engine import init_search_engine
from create_index_from_es import insert_indexes


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------------------
# 서버 시작
# ---------------------------------------------------------------------------------------
def startup():
    app = Flask(__name__)

    init_vector_db()
    init_search_engine()

    return app

# Flask 시작
app = startup()

@app.errorhandler(Exception)
def internal_server_error(error):
    logger.error(f'Error: {str(error)}')
    return jsonify([])

@app.route('/')
def index():
    settings.print()
    return { "status": 'ok' }

@app.route('/suggest', methods=['POST'])
def suggest():
    keyword = request.json['keyword']
    k = request.json['k']
    query_type = request.json.get('query_type', "NAME") # NAME, SUMMARY
    print(f'Keyword : {keyword}, k : {k}, query_type : "{query_type}"')

    if query_type.strip().upper() == "SUMMARY":
        suggestions = kwd(keyword, k)
    else:
        suggestions = kwdForName(keyword, k)

    return jsonify(suggestions)

@app.route('/indexes', methods=['POST'])
def indexes():
    counts = insert_indexes()
    return jsonify({ "total_count": counts })

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=8004)