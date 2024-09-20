import re
import psycopg2
import psycopg2.sql
import itertools

###########################################

#conectando ao banco de dados
conexao = psycopg2.connect(database='postgres', user='postgres', password='postgres', host='localhost', port='5432')
cursor = conexao.cursor()

cursor.execute('''
    CREATE SCHEMA IF NOT EXISTS esquema;
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS esquema.product (
        asin CHAR(10) PRIMARY KEY,
        title TEXT,
        salesrank INTEGER,
        product_group VARCHAR(15)        
    );
''')


cursor.execute('''
    CREATE TABLE IF NOT EXISTS esquema.similars (
        asin_X char(10),
        asin_Y char(10),
        PRIMARY KEY (asin_X, asin_Y),
        FOREIGN KEY (asin_X) REFERENCES esquema.product(asin));
''')


cursor.execute('''
    CREATE TABLE IF NOT EXISTS esquema.review (
    review_id SERIAL, 
    product_asin VARCHAR(10),
    cutomer_ID TEXT,
    date DATE,
    rating INTEGER,
    votes SMALLINT,
    helpful SMALLINT,
    PRIMARY KEY (review_id, product_asin, cutomer_ID),
    FOREIGN KEY (product_asin) REFERENCES esquema.product(asin));
''')


cursor.execute('''
    CREATE TABLE IF NOT EXISTS esquema.category(
        id INTEGER PRIMARY KEY,
        name TEXT);
''')


cursor.execute('''
    CREATE TABLE IF NOT EXISTS esquema.product_category (
        product_asin CHAR(10),
        category_ID INTEGER,
        PRIMARY KEY (product_asin, category_ID),
        FOREIGN KEY (product_asin) REFERENCES esquema.product(asin),
        FOREIGN KEY (category_ID) REFERENCES esquema.category(id));
''')

conexao.commit()

# função para inserir as informações de um produto nas tabelas do banco de dados a partir de um dicionario
def inserir_produto_no_bd(produto):
    try:
        # inserindo na tabela product
        cursor.execute('''
            INSERT INTO esquema.product (asin, title, salesrank, product_group)
            VALUES (%s, %s, %s, %s)
        ''', (produto["ASIN"], produto["title"], produto["salesrank"], produto["group"]))

        # inserindo os similares do produto na tabela 'similars'
        for similar in produto["similar"]:
            cursor.execute('''
                INSERT INTO esquema.similars (asin_X, asin_Y)
                VALUES (%s, %s)
            ''', (produto["ASIN"], similar))

        # inserindo as reviews do produto na tabela 'review'
        for review in produto["reviews"]:
            cursor.execute('''
                INSERT INTO esquema.review (product_asin, cutomer_ID, date, rating, votes, helpful)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (produto["ASIN"], review["customer_id"], review["date"], review["rating"], review["votes"], review["helpful"]))

        # inserindo as categorias do produto e seus ids na tabela category
        for categoria in produto["categorias"]:
            cursor.execute('''
                INSERT INTO esquema.category (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            ''', (categoria[1], categoria[0]))

            # inserindo os ids de categorias relacionadas ao produto na tabela product_category
            cursor.execute('''
                INSERT INTO esquema.product_category (product_asin, category_ID)
                VALUES (%s, %s)
            ''', (produto["ASIN"], categoria[1]))

    except Exception as e: # para caso de erro ao inserir
        
        conexao.rollback()
        return False
    finally:
        return True

# função para ler os dados de um batch
def extrair_dados_batch(batch):
    # dicionário para armazenar as informações de produtos no loop
    produto_atual = {}

    # expressão regular para verificar se uma linha é de review
    review = re.compile(r'^(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+(\S+)\s+rating:\s+(\d+)\s+votes:\s+(\d+)\s+helpful:\s+(\d+)') 
    
    for linha in batch:
        linha = linha.strip()

        if linha.startswith("ASIN:"): # inicio da leitura de um novo produto

            if produto_atual: # insere o produto anterior nas tabelas se não for False (discontinuado)
                inserir_produto_no_bd(produto_atual) 

            # iniciando novo produto e extraindo seu ASIN
            produto_atual = {"reviews": [], "categorias": []}
            produto_atual["ASIN"] = linha.split(":", 1)[1].strip()

        elif linha.startswith('discontinued'):  # discontinued product - define produto_atual como False para não inserir no banco
            produto_atual = False

        # verificação para as linhas de título, grupo ou rank de vendas do produto
        elif linha.startswith(("title:", "group:", "salesrank:")):
            chave, valor = linha.split(":", 1)
            if chave == "salesrank": # convertendo para inteiro se for salesrank, caso contrário insere a string mesmo
                produto_atual[chave.lower()] = int(valor.strip())
            else:
                produto_atual[chave.lower()] = valor.strip()

        # extraindo ASINs de produtos similares 
        elif linha.startswith("similar:"):
            produto_atual["similar"] = linha.split()[2:]

        # extraindo categorias e seus ids das linhas de categorias
        elif "|" in linha:
            categorias = [cat.rsplit("[", 1) for cat in linha.split("|")[1:]]
            produto_atual["categorias"].extend(
                (cat[0], int(cat[1][:-1])) for cat in categorias if (cat[0], int(cat[1][:-1])) not in produto_atual["categorias"]
            )

        # verificando se o padrão da linha segue o padrão de review para então extrair os dados de review
        elif review.match(linha):
            data_review, cliente_id, rating, votos, helpful = review.match(linha).groups()
            produto_atual["reviews"].append({
                "date": "-".join(f"{int(part):02d}" for part in data_review.split('-')),
                "customer_id": cliente_id,
                "rating": int(rating),
                "votes": int(votos),
                "helpful": int(helpful)
            })
    if produto_atual: # para inserir o último produto (do arquivo) lido
        inserir_produto_no_bd(produto_atual)
    
    conexao.commit() # salvando as alterações a cada batch de dados processado


# leitura do arquivo por batches
# são necessários 302 batches para ler o arquivo todo

with open("amazon-meta.txt","r", encoding="utf-8") as f:
    restante = []
    while True:    

        batch = restante + list(itertools.islice(f, 50000)) # concatenando o restante do batch anterior ao batch de agora

        for i in range(len(batch)-1, 0, -1): # retirando algum produto que foi pego pela metade para usar na próxima iteração
            if batch[i] == '\n':
                restante = batch[i:]
                batch = batch[:i]   
                break
        if len(batch) == 1:
            break
        extrair_dados_batch(batch)
    f.close()
