import os

# path
data_config={
    "data":{
        "mysql":{ 
            "server":os.getenv("DB_HOST"),
            "port":int(os.getenv("DB_PORT")),
            "user":os.getenv("DB_USER"),
            "password":os.getenv("DB_PASSWORD"),
            "db":os.getenv("DB_NM"),
        }
    },   
    "table":{
        "GET_API":{
            "base_dir":"", 
            "source_table":["wiki_search_tb","wiki_item_info","wiki_seealso_filter","wiki_itemlist_tb","wiki_item_stat_tb"],
            "source_column":["ID,TITLE,REDIRECT,REDIRECT_TYPE,TECH_RANK,TECH_CNT","ID,TITLE,SECTION_TEXT,TRANSLATED,TECH_CLASS12,CATEGORY,SUB_CATEGORY","","ID,pagerank,pageviews,EPV","ID,BASE_YEAR,NORM_PAGEVIEWS,NORM_EPV"],
            "query":["","","","",""]
        }
    }
}
