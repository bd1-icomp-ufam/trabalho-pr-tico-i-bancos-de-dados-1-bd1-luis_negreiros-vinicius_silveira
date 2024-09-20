  
import psycopg2
from datetime import timedelta

# função para exibir no console as opções de consulta
def mostrar_opcoes():
    print(" - 'a <ASIN_do_produto>': Listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação;")
    print(" - 'b <ASIN_do_produto>': listar os produtos similares com maiores vendas do que o produto selecionado;")
    print(" - 'c <ASIN_do_produto>': mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada;")
    print(" - 'd': listar os 10 produtos líderes de venda em cada grupo de produtos;")
    print(" - 'e': listar os 10 produtos com a maior média de avaliações úteis positivas por produto;")
    print(" - 'f': listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto;")
    print(" - 'g': listar os 10 clientes que mais fizeram comentários por grupo de produto;")

    print(" - '?': mostrar as opções de consulta;")
    print(" - 'x': sair do programa.")

# função com o loop principal para rodar o programa
def main():
    print("Bem-vindo(a)!")
    print("Selecione uma opção de consulta:")
    mostrar_opcoes()
    
    opcao = input("\nDigite a opção (Enter para executar): ")
    opcao = opcao[0].lower() + opcao[1:]

    while opcao != 'x':
        if opcao.startswith('a'):
            if len(opcao) <= 2 or not ' ' in opcao:
                print("Digite um produto para ser consultado!")
            else:
                produto = opcao.split(' ',1)[1]
                a(produto)
        elif opcao.startswith('b'):
            if len(opcao) <= 2 or not ' ' in opcao:
                print("Digite um produto para ser consultado!")
            else:
                produto = opcao.split(' ',1)[1]
                b(produto)
        elif opcao.startswith('c'):
            if len(opcao) <= 2 or not ' ' in opcao:
                print("Digite um produto para ser consultado!")
            else:
                produto = opcao.split(' ',1)[1]
                c(produto)
        elif opcao.startswith('d'):
            d()
        elif opcao.startswith('e'):
            e()
        elif opcao.startswith('f'):
            f()
        elif opcao.startswith('g'):
            g()
        elif opcao.startswith('?'):
            mostrar_opcoes()
        else:
            print(f"Opção '{opcao}' inválida!")

        opcao = input("\nDigite a opção (Enter para executar): ")
        opcao = opcao[0].lower() + opcao[1:]

# Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação 
def a(asin):
    try:
        cursor.execute(f"SELECT * FROM esquema.product WHERE asin = '{asin}';")
        if not cursor.fetchone():
            print(f"produto de ASIN '{asin}' não encontrado.")
            return False

        cinco_positivos = '''
        SELECT cutomer_ID, rating, helpful, votes, date 
        FROM esquema.review 
        WHERE product_asin = %s
        ORDER BY helpful DESC, rating DESC
        LIMIT 5;
        '''
        
        cinco_negativos = '''
        SELECT cutomer_ID, rating, helpful, votes, date 
        FROM esquema.review 
        WHERE product_asin = %s
        ORDER BY helpful DESC, rating ASC
        LIMIT 5;
        '''
        
        cursor.execute(cinco_positivos, (asin,))
        comentarios_positivos = cursor.fetchall()
        print("5 comentários mais úteis e com maior avaliação:")
        for comentario in comentarios_positivos:
            print(f" - Cliente: {comentario[0]}, Avaliação: {comentario[1]}, Útil: {comentario[2]}, Votos: {comentario[3]}, Data: {comentario[4]}")
        
        cursor.execute(cinco_negativos, (asin,))
        comentarios_negativos = cursor.fetchall()
        print("5 comentários mais úteis e com menor avaliação:")
        for comentario in comentarios_negativos:
            print(f" - Cliente: {comentario[0]}, Avaliação: {comentario[1]}, Útil: {comentario[2]}, Votos: {comentario[3]}, Data: {comentario[4]}")
        
    except Exception as e:
        print("Erro ao listar os comentários:", e)
    finally:
        return True

# Dado um produto, listar os produtos similares com maiores vendas do que ele
def b(produto):
    cursor.execute(f"SELECT title, salesrank FROM esquema.product WHERE asin = '{produto}';")
    buscado = cursor.fetchall()
    if buscado:
        cursor.execute(f'''
            SELECT p_similar.asin, p_similar.title, p_similar.salesrank
            FROM esquema.product p
            JOIN esquema.similars s ON p.asin = s.asin_X
            JOIN esquema.product p_similar ON s.asin_Y = p_similar.asin
            WHERE p.asin = '{produto}'
            AND p_similar.salesrank < p.salesrank
            ORDER BY p_similar.salesrank ASC;
        ''')
        produtos = cursor.fetchall()
        if produtos:
            print(f"\nProdutos com maior classificação no rank de vendas que o produto {buscado[0][0]} (Classificação {buscado[0][1]}):")
            for p in produtos:
                print(f" - NOME: {p[1]}; ASIN: {p[0]}; CLASSIFICAÇÃO: {p[2]};")
        else:
            print(f'\nNão há produtos similares a {produto} com mais vendas que ele.')
    else:
        print(f"Produto '{produto}' não encontrado.")
    
    return True

# Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada 
def c(produto):

    cursor.execute(f"SELECT title FROM esquema.product WHERE asin = '{produto}'")
    if not cursor.fetchone():
        print(f"Produto '{produto}' não encontrado.")
        return False

    cursor.execute(f'''
        SELECT 
            r.date, 
            AVG(r.rating) OVER (PARTITION BY r.product_asin ORDER BY r.date) AS media
        FROM 
            esquema.review r
        WHERE 
            r.product_asin = '{produto}'
        ORDER BY 
            r.date ASC;
    ''')
        
    avals = cursor.fetchall()

    if len(avals) == 0:
        print(f"O produto '{produto}' não tem nenhuma review, portanto não tem média de avaliações.")
        return False
    else:
        print(f"Evolução das médias de avaliação do produto '{produto}':")

        data_inicial = avals[0][0]
        data_final = avals[-1][0]
        id = 0
        media = avals[0][1]

        while data_inicial <= data_final:
            if id < len(avals) and data_inicial == avals[id+1][0]:
                media = avals[id+1][1]
                id += 1

            print(f" - Data: {data_inicial.strftime("%Y-%m-%d")} - Média: {round(media,2)}")  
                        
            data_inicial += timedelta(days=1)
    return True

# Listar os 10 produtos líderes de venda em cada grupo de produtos
def d():
    consulta = '''
        WITH RankedProducts AS (
            SELECT 
                asin, 
                title, 
                salesrank, 
                product_group,
                ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY salesrank ASC) AS rank
            FROM esquema.product
            WHERE salesrank > 0
        )
        SELECT asin, title, salesrank, product_group
        FROM RankedProducts
        WHERE rank <= 10
        ORDER BY product_group, salesrank;
    '''
    
    cursor.execute(consulta)
    melhores_produtos = cursor.fetchall()
    
    print("\nOs 10 produtos líderes de venda em cada grupo de produtos:")
    
    grupo_atual = melhores_produtos[0][3]
    print(f"Grupo {grupo_atual}")

    for i in melhores_produtos:
        if i[3] != grupo_atual:
            grupo_atual = i[3]
            print(f"Grupo {grupo_atual}")
        print(f"\t- Título: {i[1]} [{i[0]}] - Posição no ranking de vendas: {i[2]}")

    return True

# Listar os 10 produtos com a maior média de avaliações úteis positivas por produto
def e():
    cursor.execute('''
        SELECT p.title, AVG(r.helpful) AS avg_helpful
        FROM esquema.product p
        JOIN esquema.review r ON p.asin = r.product_asin
        GROUP BY p.asin, p.title
        ORDER BY avg_helpful DESC
        LIMIT 10;
    ''')
    resultados = cursor.fetchall()
    print("Os 10 produtos com maior média de avaliações úteis positivas:")
    for res in resultados:
        print(f" - Produto: {res[0]} - média de avaliações úteis positivas: {round(res[1], 3)}")
    return True

# Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto
def f():
    try:
        consulta = '''
            SELECT
                c.name AS category_name,
                AVG(r.helpful) AS avg_helpful_votes
            FROM
                esquema.category c
            JOIN
                esquema.product_category pc ON c.id = pc.category_ID
            JOIN
                esquema.review r ON pc.product_asin = r.product_asin
            GROUP BY
                c.name
            ORDER BY
                avg_helpful_votes DESC
            LIMIT 5;
        '''
        
        cursor.execute(consulta)
    
        resultados = cursor.fetchall()
        print("As 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
        for resultado in resultados:
            print(f" - Categoria: {resultado[0]}, Média de votos \"helpful\": {resultado[1]:.2f}")
    
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    
    finally:
        return True

# Listar os 10 clientes que mais fizeram comentários por grupo de produto
def g():
    cursor.execute("""
        SELECT cutomer_ID, product_group, reviews
        FROM (
            SELECT r.cutomer_ID, p.product_group, COUNT(r.cutomer_ID) AS reviews,
                ROW_NUMBER() OVER (PARTITION BY p.product_group ORDER BY COUNT(r.cutomer_ID) DESC) AS rank
            FROM esquema.review r 
            JOIN esquema.product p ON p.asin = r.product_asin
            GROUP BY p.product_group, r.cutomer_ID
        ) AS ranked_reviews
        WHERE rank <= 10
        ORDER BY product_group, reviews DESC;
    """)
    grupos = cursor.fetchall()

    print("Clientes que mais fizeram comentários por grupo de produto:\n")

    for i in range(len(grupos)):
        print(f" - Grupo: {grupos[i][1]} - ID do cliente: {grupos[i][0]} - Número de comentários: {grupos[i][2]}")
        if i < len(grupos)-1 and grupos[i][1] != grupos[i+1][1]:
            print("\n")
    return True

# conectando ao banco de dados "BDTP1"
conexao = psycopg2.connect(database='postgres', user='postgres', password='postgres', host='localhost', port='5432')
cursor = conexao.cursor()

main()

cursor.close()
conexao.close()
