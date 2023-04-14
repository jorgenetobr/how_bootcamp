import json
import psycopg2

# abre o arquivo JSON
with open('/home/jorge/eng_dados_bootcamp/files/json_data.json', 'r') as f:
    dados_json = json.load(f)

# estabelece a conexão com o banco de dados PostgreSQL
conexao = psycopg2.connect(host='localhost', dbname='test_db', user='root', password='root')

# abre um cursor para executar comandos SQL
cursor = conexao.cursor()

# cria a tabela no banco de dados
cursor.execute("""
    CREATE TABLE tech.tech_startups (
        id SERIAL PRIMARY KEY,
        name TEXT,
        description TEXT,
        founded_year INTEGER,
        category_code TEXT,
        country_code TEXT);
""")

# percorre os dados do arquivo JSON e insere no banco de dados
for dado in dados_json:
    # extrai as informações do JSON
    name = dado.get('name')
    description = dado.get('description')
    founded_year = dado.get('founded_year')
    category_code = dado.get('category_code')
    country_code = dado.get('country_code')
    
    # insere os dados no banco de dados
    cursor.execute("INSERT INTO tech.tech_startups (name, description, founded_year, category_code, country_code) VALUES (%s, %s, %s, %s, %s)", 
                    (name, description, founded_year, category_code, country_code))

# commita as mudanças no banco de dados
conexao.commit()

# fecha o cursor e a conexão com o banco de dados
cursor.close()
conexao.close()
