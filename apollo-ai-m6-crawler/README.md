## Apollo Model6을 위한 Wiki Crawler

- dbscript의 ddl.sql 실행
- streamlit run ./run.py 로 실행
- 각 단계별 수집 기능 실행

### Local Crawler 설치
1. MariaDB 설치
    - 데이터베이스 생성
      <pre><code>CREATE DATABASE apollo
      CHARACTER SET utf8mb4
      COLLATE utf8mb4_unicode_ci;
      </code></pre>
    - 사용자 생성
      <pre><code>CREATE USER apollo@'%' identified by 'apollo';</code></pre>
    - 권한 부여
      <pre><code>GRANT ALL PRIVILEGES ON apollo.* TO apollo@'%';</code></pre>
2. MariaDB 대소문자 구분 옵션 설정
    - /etc/mysql/mariadb.cnf
      <pre><code>[mysqld]
      lower_case_table_names = 1</code></pre> 
3. ./dbscript/ddl.sql 실행
4. ./config/path.py의 dump파일 폴더 설정
5. ./config/data_test.conf파일 수정
6. Crawling 관련 데이터
    - /config/M6 시드.xlsx
    - /config/wiki_rule.xlsx
7. Git clone
   <pre>git clone https://github.com/solideos/apollo-ai-m6-crawler.git</pre>
8. PyCharm Interpreter 설정
    - Python : 3.12.10
    - Poetry : 2.2.2
9. poetry install
10. streamlit run ./run.py