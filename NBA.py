import psycopg2
import csv
from datetime import datetime

# define tables
def create_table_nba_payroll(cursor):
    cursor.execute('''CREATE TABLE nba.nba_payroll (
                    id SERIAL PRIMARY KEY,
                    row_number INTEGER,
                    team_name VARCHAR(255),
                    seasonstartyear INTEGER,
                    payroll MONEY,
                    inflationadjpayroll MONEY)''')
    print("Tabela table_nba_payroll criada com sucesso!")

def create_table_nba_salary(cursor):
    cursor.execute('''CREATE TABLE nba.nba_salary (
                    id SERIAL PRIMARY KEY,
                    row_number INTEGER,
                    playerName VARCHAR(255),
                    seasonStartYear INTEGER,
                    salary MONEY,
                    inflationAdjSalary MONEY)''')
    print("Tabela table_nba_salary criada com sucesso!")

def create_table_nba_score_stats(cursor):
    cursor.execute('''CREATE TABLE nba.nba_score_stats (
                    season VARCHAR(255),
                    game_id VARCHAR(255),
                    player_name VARCHAR(255),
                    team VARCHAR(255),
                    game_date VARCHAR(255),
                    matchup VARCHAR(255),
                    wl VARCHAR(255),
                    min VARCHAR(255),
                    fgm VARCHAR(255),
                    fga VARCHAR(255),
                    fg_pct VARCHAR(255),
                    fg3m VARCHAR(255),
                    fg3a VARCHAR(255),
                    fg3_pct VARCHAR(255),
                    ftm VARCHAR(255),
                    fta VARCHAR(255),
                    ft_pct VARCHAR(255),
                    oreb VARCHAR(255),
                    dreb VARCHAR(255),
                    reb VARCHAR(255),
                    ast VARCHAR(255),
                    stl VARCHAR(255),
                    blk VARCHAR(255),
                    tov VARCHAR(255),
                    pf VARCHAR(255),
                    pts VARCHAR(255),
                    plus_minus VARCHAR(255),
                    video_available VARCHAR(255))
''')
    print("Tabela table_nba_score_stats criada com sucesso!")

# db connection open
conn = psycopg2.connect(host='localhost',
                        dbname='test_db',
                        user='root',
                        password='root') 
cursor = conn.cursor()
print("Conexão sucesso")

# Create table nba payroll
create_table_nba_payroll(cursor)

file_source = '/home/jorge/eng_dados_bootcamp/files/NBA Payroll(1990-2023).csv'
csv.register_dialect('csv_dialect',
                    delimiter=',',
                    skipinitialspace=True,
                    quoting=csv.QUOTE_ALL)
with open(file_source, 
          'r', 
          encoding= 'utf-8') as csvfile:
    csv_reader = csv.reader(csvfile, dialect='csv_dialect')
    next(csv_reader)
    for row in csv_reader:
        row[0] = int(row[0])                                     #row_number
        row[2] = int(row[2])                                     #seasonstartyear
        row[3] = int(row[3].replace("$", "").replace(",", ""))   #payroll
        row[4] = int(row[4].replace("$", "").replace(",", ""))   #inflationadjpayroll

        cursor.execute("INSERT INTO nba.nba_payroll (row_number, team_name, seasonstartyear, payroll, inflationadjpayroll) VALUES (%s, %s, %s, %s, %s)", (row[0], row[1], row[2], row[3], row[4]))

# Confirm changes
conn.commit()
print("Payroll sucesso")
# Create table nba salary
create_table_nba_salary(cursor)

file_source = '/home/jorge/eng_dados_bootcamp/files/NBA Salaries(1990-2023).csv'
csv.register_dialect('csv_dialect',
                    delimiter=',',
                    skipinitialspace=True,
                    quoting=csv.QUOTE_ALL)
with open(file_source, 
          'r', 
          encoding= 'utf-8') as csvfile:
    csv_reader = csv.reader(csvfile, dialect='csv_dialect')
    next(csv_reader)
    for row in csv_reader:
        row[0] = int(row[0])                                     #row_number
        row[2] = int(row[2])                                     #seasonstartyear
        row[3] = int(row[3].replace("$", "").replace(",", ""))   #payroll
        row[4] = int(row[4].replace("$", "").replace(",", ""))   #inflationAdjSalary

        cursor.execute("INSERT INTO nba.nba_salary (row_number, playerName, seasonstartyear, salary, inflationAdjSalary) VALUES (%s, %s, %s, %s, %s)", (row[0], row[1], row[2], row[3], row[4]))

# Confirm changes
conn.commit()
print("Salary sucesso")
create_table_nba_score_stats(cursor)

file_source = '/home/jorge/eng_dados_bootcamp/files/NBA Player Box Score Stats(1950 - 2022).csv'
csv.register_dialect('csv_dialect',
                    delimiter=',',
                    skipinitialspace=True,
                    quoting=csv.QUOTE_ALL)
with open(file_source, 
          'r', 
          encoding= 'utf-8') as csvfile:
    csv_reader = csv.reader(csvfile, dialect='csv_dialect')
    next(csv_reader)
    for row in csv_reader:
        cursor.execute("INSERT INTO nba.jnba_score_stats (season,game_id,player_name,team,game_date,matchup,wl,min,fgm,fga,fg_pct,fg3m,fg3a,fg3_pct,ftm,fta,ft_pct,oreb,dreb,reb,ast,stl,blk,tov,pf,pts,plus_minus,video_available) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],row[28]))
print("stats insert sucesso")
# Confirm changes
conn.commit()
print("stats sucesso")

# Close db connection
cursor.close()


conn.close()
print("Dados enviados com sucesso!")