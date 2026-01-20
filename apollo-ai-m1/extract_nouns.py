# -*- coding: utf-8 -*-
import re
import pandas as pd
from konlpy.tag import Okt

# ===== 불용어 =====
STOPWORDS_BASE = {
    '및', '위한', '통한', '의', '등'
}
# 도메인(과제 공통) 불용어
STOPWORDS_DOMAIN = {
    # '연구', '개발', '구축', '인프라', '플랫폼', '시스템', '표준', '표준화',
    # '시험', '운영', '운용', '관리', '사업', '사업화', '평가', '기반', '핵심', '기술'
}

okt = Okt()

__all__ = ["extract_nouns_simple_robust", "extract_project_title_keywords"]

def _drop_redundant_measure_tokens(tokens):
    """
    PM1 vs PM1.0 같이 점 포함 긴 형식이 있으면 짧은 형식(PM1)을 제거.
    또한 'Level 3'가 있으면 'Level' 단독 토큰 제거.
    """
    out = []
    set_tokens = set(tokens)

    # PM류(대문자+숫자[.숫자]) 그룹핑
    # 예: PM1, PM1.0 / NO2, NO2.5 등 일반화
    # 공백은 비교에서 제거(예: 'Level 3' 처리용 별도 규칙 아래)
    measure_re = re.compile(r'^([A-Z]{2,})(\d+(?:\.\d+)?)$')

    # 점 포함 긴 형식들의 "접두+정수부"를 기록(예: PM + 1)
    longer_roots = set()
    for t in tokens:
        m = measure_re.match(t.replace(' ', ''))
        if m:
            prefix, num = m.groups()
            if '.' in num:
                int_part = num.split('.')[0]
                longer_roots.add(prefix + int_part)

    for t in tokens:
        t_comp = t.replace(' ', '')
        m = measure_re.match(t_comp)
        if m:
            prefix, num = m.groups()
            # 짧은 형식(PM1)이고 같은 루트의 긴 형식(PM1.0)이 존재하면 스킵
            if '.' not in num and (prefix + num) in longer_roots:
                continue

        # 'Level 3' 있으면 'Level' 단독 제거
        if t.lower() == 'level' and any(re.match(r'^Level\s*\d+$', x, re.I) for x in set_tokens):
            continue

        out.append(t)
    return out

def extract_nouns_simple_robust(text, extra_stopwords=None):
    """
    간단·안정 명사 추출:
      - (세부/총괄/과제) 괄호 제거
      - Level 3, PM1.0 등 특수 토큰 보존
      - 복합명사(자율운항선박, 극미세먼지 등) 후처리 결합
      - 중복·불필요 토큰 제거 + 불용어 필터
    """
    # 0) 불용어 결합
    if extra_stopwords is None:
        stopwords = set()
    else:
        stopwords = set(extra_stopwords)
    stopwords |= STOPWORDS_BASE | STOPWORDS_DOMAIN

    # 1) (세부/총괄/과제) 포함 괄호 블록만 먼저 제거 대상에서 분리
    clean_text = text
    for pattern in [r'\([^)]*(?:세부|총괄|과제)[^)]*\)',
                    r'\[[^\]]*(?:세부|총괄|과제)[^\]]*\]']:
        clean_text = re.sub(pattern, ' ', clean_text)

    # 2) 보존할 특수 토큰 수집
    special_tokens = []

    # 2-1) 괄호 안의 영문/약어(+숫자/점/슬래시 허용)
    special_tokens.extend(re.findall(r'[(\[]([A-Z][A-Z0-9\./]*)[)\]]', text))

    # 2-2) Level + 숫자(공백 허용, 저장 시 공백 유지)
    level_hits = re.findall(r'\bLevel\s*\d+\b', clean_text, flags=re.I)
    level_hits = [re.sub(r'\s+', ' ', h).strip() for h in level_hits]  # 'Level    3' -> 'Level 3'
    special_tokens.extend(level_hits)

    # 2-3) 대문자+숫자(선택적 소수) 패턴 (예: PM2.5, NO2, H2, OPPAV는 제외-아래에서 대문자만 따로)
    special_tokens.extend(re.findall(r'\b[A-Z]{2,}\d+(?:\.\d+)?\b', clean_text))

    # 2-4) ICT계열, Edge/AIoT, 슬래시 약어(AR/VR)
    special_tokens.extend(re.findall(r'\bICT\w*\b', clean_text))   # ICT, ICT융합 등
    special_tokens.extend(re.findall(r'\bEdge\b', clean_text))
    special_tokens.extend(re.findall(r'\bAIoT\b', clean_text))
    special_tokens.extend(re.findall(r'\b[A-Z]+/[A-Z]+\b', clean_text))  # AR/VR

    # 2-5) 순수 대문자 약어(IMO, BCI, SW 등)
    special_tokens.extend(re.findall(r'\b[A-Z]{2,}\b', clean_text))

    # 3) 이후 명사 추출을 위해 모든 괄호 제거, 구분자 정리
    clean_text = re.sub(r'[(\[{][^)\]}]*[)\]}]', ' ', clean_text)
    clean_text = re.sub(r'[-·]', ' ', clean_text)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()

    # 4) Okt로 명사 추출
    nouns = okt.nouns(clean_text) if clean_text else []

    # 5) 복합명사 결합(후처리)
    processed_nouns = []
    skip_next = False
    for i, noun in enumerate(nouns):
        if skip_next:
            skip_next = False
            continue
        if noun is None:
            continue

        next_noun = nouns[i + 1] if i < len(nouns) - 1 else None

        compound_pairs = {
            ('인공', '지능'): '인공지능',
            ('머신', '비전'): '머신비전',
            ('머신', '러닝'): '머신러닝',
            ('스마트', '시티'): '스마트시티',
            ('멀티', '모달'): '멀티모달',
            ('인터', '랙션'): '인터랙션',
            ('자율', '운항'): '자율운항',
            ('운항', '선박'): '운항선박',
            ('극', '미세'): '극미세',
            ('미세', '먼지'): '미세먼지',
            ('바이오', '플라스틱'): '바이오플라스틱',
            ('지식', '베이스'): '지식베이스',
            ('생육', '진단'): '생육진단',
            ('스마트', '농업'): '스마트농업',
        }

        # 3-gram 결합
        if i < len(nouns) - 2:
            n2 = nouns[i + 2]
            triple = (noun, next_noun, n2)
            if triple == ('자율', '운항', '선박'):
                processed_nouns.append('자율운항선박')
                skip_next = True
                nouns[i + 2] = None
                continue
            if triple == ('극', '미세', '먼지'):
                processed_nouns.append('극미세먼지')
                skip_next = True
                nouns[i + 2] = None
                continue

        # 2-gram 결합
        if next_noun is not None and (noun, next_noun) in compound_pairs:
            processed_nouns.append(compound_pairs[(noun, next_noun)])
            skip_next = True
        else:
            processed_nouns.append(noun)

    # 6) 특수 토큰 + 복합명사 통합 후 정리
    #    - Level 3는 공백 유지
    #    - 중복/불필요 토큰 제거
    all_tokens = special_tokens + processed_nouns

    # 7) 필터링 및 Dedup
    result = []
    seen_lower = set()

    for token in all_tokens:
        if not token:
            continue

        # 길이 1 제거(숫자 '0' 등)
        if len(token) <= 1:
            continue

        # '세부/총괄/과제' 포함 토큰 제거
        if any(x in token for x in ['세부', '총괄', '과제']):
            continue

        # 불용어 제거
        if token in stopwords:
            continue

        # '수준의' -> '수준'만 남도록, 조사는 기본 stopwords에 의해 정리됨
        # (Okt가 이미 '수준', '의'로 분리하는 편)

        # 중복 제거(대소문자 무시)
        key = token.lower()
        if key not in seen_lower:
            result.append(token)
            seen_lower.add(key)

    # 8) PM1 vs PM1.0 등 중복 축약형 제거 + Level 단독 제거
    result = _drop_redundant_measure_tokens(result)

    return result

def extract_project_title_keywords(title, extra_stopwords=None):
    """과제명에서 핵심 명사 리스트를 추출합니다.

    extract_nouns_simple_robust의 전처리 규칙을 그대로 재사용합니다.
    """
    if not title:
        return []
    return extract_nouns_simple_robust(str(title), extra_stopwords=extra_stopwords)


# ===== 아래는 기존 파이프라인 그대로 사용 =====

if __name__ == "__main__":
    try:
        df = pd.DataFrame(rows)
    except NameError as exc:
        raise RuntimeError("`rows` 전역 리스트를 먼저 정의해 주세요.") from exc
    df["과제명_nouns"] = df["과제명"].apply(extract_nouns_simple_robust)
    df["과제명_nouns_str"] = df["과제명_nouns"].apply(lambda x: " ".join(x))
    print(df[["과제명", "과제명_nouns", "과제명_nouns_str"]])

