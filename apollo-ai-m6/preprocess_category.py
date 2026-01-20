def replace_category(x):
    if x[1] is None:
        result='기타'
    else:
        list_tech12=['반도체디스플레이','이차전지','모빌리티','차세대원자력','첨단바이오','우주항공해양','수소','사이버보안','인공지능','차세대통신','첨단로봇제조','양자']
        category=x[0]
        tech_class12=x[1].replace(' ','')
        if (category.upper().startswith('ITEM')) or (tech_class12 in list_tech12):
            result='아이템'
        elif category.upper().startswith('NON-ITEM'):
            result='비아이템'
        elif category.upper().startswith('ETC'):
            result='기타'
        else:
            result='기타'
    return result

def replace_tech12(x):
    if x[1] is None:
        result='기타'
    else:
        tech_class12=x[1]
        if ('반도체' in tech_class12) or ('디스플레이' in tech_class12):
            result='반도체 디스플레이'
        elif ('이차전지' in tech_class12.replace(" ","")) or ('이차에너지' in tech_class12.replace(" ","")):
            result='이차전지'
        elif ('모빌리티' in tech_class12) or ('자동차' in tech_class12):
            result='모빌리티'
        elif ('원자력' in tech_class12) or ('원자로' in tech_class12):
            result='차세대 원자력'
        elif ('바이오' in tech_class12) or ('의료' in tech_class12):
            result='첨단 바이오'
        elif (('우주' in tech_class12) or ('항공' in tech_class12) or ('해양' in tech_class12)):
            result='우주항공해양'
        elif ('수소' in tech_class12) or ('친환경 에너지' in tech_class12):
            result='수소'
        elif ('사이버' in tech_class12) or ('소프트웨어 보안' in tech_class12):
            result='사이버보안'
        elif ('인공지능' in tech_class12.replace(" ","")) or ('AI' in tech_class12):
            result='인공지능'
        elif ('통신' in tech_class12.replace(" ","")) or ('네트워킹' in tech_class12.replace(" ","")) or ('네트워크' in tech_class12.replace(" ","")):
            result='차세대통신'
        elif ('로봇' in tech_class12.replace(" ","")) or ('첨단제조' in tech_class12.replace(" ","")):
            result='첨단 로봇 제조'
        elif ('양자' in tech_class12):
            result='양자'
        else:
            result='기타'
    return result
