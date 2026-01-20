import re
from langdetect import detect, DetectorFactory


re_hangul = re.compile(r"[\uac00-\ud7a3]")
DetectorFactory.seed = 0

def _detect_lang(text: str):
    try:
        return detect(text)
    except Exception as e:
        return "en"

# ---------------------------------------------------------------------------------------
# 한글 여부
# ---------------------------------------------------------------------------------------
def is_kor(text: str) -> bool:
    if not text:
        return False
    hits = len(re_hangul.findall(text))
    # 길이 기반 완충: 너무 짧은 질의의 오탐 방지
    if hits >= max(1, len(text) // 3):
        return True

    return _detect_lang(text) == "ko"