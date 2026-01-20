import random
import pandas as pd

def convert_data(keyword: str, data: dict, link: list, category_dict: dict) :
    graphs = {}
    graphs['links'] = []
    graphs['nodes'] = []
    graphs['categories'] = []
    graphs['max_cnt'] = max(category_dict.values())
    if link :
        for x, y in link :    
            graphs['links'].append({"source": x, "target" : y})
    for index, row in pd.DataFrame(data).iterrows():
        if row['TITLE'] == keyword :
          category = 0
        else:
            category = category_dict[row['ID']]
        try:
            graphs['nodes'].append({
                'id': row['ID'],
                'name': row['TITLE'],
                'category':category,
                'node_value': row['node_value'],
                'tech_cate': row['tech_cate'],
                'item_cate': row['item_cate'],
                'item_sub_cate': row['item_sub_cate'],
                'pagerank': row['PAGERANK'],
                'pageviews': row['PAGEVIEWS'],
                'EPV': row['EPV']
            })
        except:
            graphs['nodes'].append({
                'id': row['ID'],
                'name': row['TITLE']
            })
    for alpb in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'] :
        graphs['categories'].append({'name' : alpb})

    return graphs
