# 사용할 Python 버전의 기본 이미지를 지정 (Python 3.9)
FROM python:3.9-slim

# 컨테이너 내에서 작업할 디렉터리를 설정
WORKDIR /app

# Poetry 설치
RUN pip install poetry

# Poetry가 가상 환경을 생성하지 않도록 설정
RUN poetry config virtualenvs.create false

# Poetry 의존성 파일들을 복사
COPY pyproject.toml poetry.lock ./

# Poetry 의존성 패키지 설치
RUN poetry install --no-root

# 나머지 모든 애플리케이션 소스 코드를 복사
COPY . .

# .env 환경 파일 복사
ARG MODE="production"
COPY .env.${MODE} .env

# FastAPI 서버 포트 설정 (기본 포트 : 5000)
EXPOSE 5000

# uvicorn (ASGI 서버) 구동
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]