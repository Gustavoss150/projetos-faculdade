# mysql-connector
import mysql.connector
import time
import pandas as pd

class SGBD:
    def __init__(self, host, user, passwd, database):
        self.conector = mysql.connector.connect(
            host=host,
            user=user,
            passwd=passwd,
            database=database
        )
        self.cursor = self.conector.cursor()


    def inserir_tabela(self):
        tabela = ''
        add = 'S'
        var = 0
        while tabela != "0":
            nome_da_tabela = input("\nQual nome da tabela que você quer criar ? (Digite '0' para sair) >>> ")
            if nome_da_tabela == '0':
                break
            self.cursor.execute(f"CREATE TABLE {nome_da_tabela} (ID INT AUTO_INCREMENT PRIMARY KEY)")
            while add == "S":
                add = input("Você deseja adicionar uma variável? (S/N) >>> ").upper()
                if add == "S":
                    var += 1
                    variavel_nome = input(f"Digite o nome da {var}ª variável >>> ")
                    variavel_tipo = input(f"Digite o tipo da {var}ª variável (Ex: INT, VARCHAR(255), etc.) >>> ")
                    self.cursor.execute(f"ALTER TABLE {nome_da_tabela} ADD {variavel_nome} {variavel_tipo}")
                elif add == "N":
                    tabela = "0"
                    break


    def inserir_dados(self):
        while True:
            self.mostrar_esquema()
            tabela = input("\nDigite qual a tabela receberá os valores >>> ")
            self.cursor.execute("SHOW TABLES")
            tabelas_existentes = [tabela[0] for tabela in self.cursor.fetchall()]
            if tabela not in tabelas_existentes:
                print(f"A tabela {tabela} não existe.")
                time.sleep(2)
                continue
            self.cursor.execute(f"DESCRIBE {tabela}")
            colunas_info = self.cursor.fetchall()
            colunas = [coluna_info[0] for coluna_info in colunas_info]

            auto_increment_columns = self.obter_chaves_auto_incrementadas()
            valores = []  # Inicializando a lista de valores aqui

            for coluna in colunas:
                if coluna in auto_increment_columns:
                    valores.append(None)  # Adicionando None apenas para colunas auto incrementadas
                else:
                    valor = input(f"Digite o valor para a coluna '{coluna}': ")
                    valores.append(valor)

            placeholders = ", ".join(["%s" if valor is not None else "NULL" for valor in valores])
            query = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
            print("\nConsulta SQL:", query)
            print("\nValores:", valores)

            self.cursor.execute(query, [valor for valor in valores if valor is not None])
            self.conector.commit()
            print("\nDados inseridos com sucesso!\n")
            time.sleep(2)
            continuar = input("Deseja inserir novos dados? (S/N) >>> ").upper()
            if continuar != 'S':
                break

        
    def atualizar_dados(self):
        while True:
            self.mostrar_esquema()
            tabela = input("\nDigite qual tabela será atualizada >>> ")
            primary_keys = self.obter_chaves_primarias()
            self.cursor.execute("SHOW TABLES")
            tabelas_existentes = [tabela[0] for tabela in self.cursor.fetchall()]
            if tabela not in tabelas_existentes:
                print(f"A tabela {tabela} não existe.")
                time.sleep(2)
                continue
            self.cursor.execute(f"DESCRIBE {tabela}")
            colunas_info = self.cursor.fetchall()
            colunas = [coluna_info[0] for coluna_info in colunas_info]

            self.cursor.execute(f"SELECT * FROM {tabela}")
            dados = self.cursor.fetchall()
            colunas_info = self.cursor.description
            colunas = [coluna[0] for coluna in colunas_info]

            print(f"\n{tabela}")
            print("Colunas:", "   |   ".join(colunas))
            print("Valores:")
            for linha in dados:
                valores = [str(valor) for valor in linha]
                print("    " + "   |   ".join(valores))

            coluna_escolhida = input("\nDigite qual coluna deseja atualizar os dados >>> ")
            if coluna_escolhida not in colunas:
                print(f"A coluna {coluna_escolhida} não existe na tabela {tabela}.")
                time.sleep(2)
                continue
            
            identificador_linha = input("Digite o id único da linha que deseja atualizar >>> ")
            novo_valor = input(f"Digite o novo valor para {coluna_escolhida} >>> ")
            query = f"UPDATE {tabela} SET {coluna_escolhida} = %s WHERE {primary_keys[tabela]} = %s"
            self.cursor.execute(query, (novo_valor, identificador_linha))
            self.conector.commit()
            print("Dados atualizados com sucesso!\n")
            time.sleep(2)

            continuar = input("Deseja atualizar outros dados? (S/N) >>> ").upper()
            if continuar != "S":
                break


    def buscar_dados(self):
        while True:
            self.mostrar_esquema()
            tabela = input("\nDigite a tabela que deseja consultar >>> ")
            self.cursor.execute("SHOW TABLES")
            tabelas_existentes = [tabela[0] for tabela in self.cursor.fetchall()]
            if tabela not in tabelas_existentes:
                print(f"A tabela {tabela} não existe.")
                time.sleep(2)
                continue
            self.cursor.execute(f"DESCRIBE {tabela}")
            colunas_info = self.cursor.fetchall()
            colunas = [coluna_info[0] for coluna_info in colunas_info]

            self.cursor.execute(f"SELECT * FROM {tabela}")
            dados = self.cursor.fetchall()
            colunas_info = self.cursor.description
            colunas = [coluna[0] for coluna in colunas_info]

            df = pd.DataFrame(dados, columns=colunas)
            print("\n" + df.to_string(index=False))
            
            time.sleep(2)
            continuar = input("\nDeseja fazer outra consulta? (S/N) >>> ").upper()
            if continuar != "S":
                break


    def deletar_dados(self):
        while True:
            self.mostrar_esquema()
            tabela = input("\nDigite qual a tabela desejada >>> ")
            primary_keys = self.obter_chaves_primarias()
            self.cursor.execute("SHOW TABLES")
            tabelas_existentes = [tabela[0] for tabela in self.cursor.fetchall()]
            if tabela not in tabelas_existentes:
                print(f"A tabela {tabela} não existe.")
                time.sleep(2)
                continue
            self.cursor.execute(f"DESCRIBE {tabela}")
            colunas_info = self.cursor.fetchall()
            colunas = [coluna_info[0] for coluna_info in colunas_info]

            self.cursor.execute(f"SELECT * FROM {tabela}")
            dados = self.cursor.fetchall()
            colunas_info = self.cursor.description
            colunas = [coluna[0] for coluna in colunas_info]

            df = pd.DataFrame(dados, columns=colunas)
            print("\n" + df.to_string(index=False))

            identificador_linha = input("Digite o id único da linha que deseja deletar dados >>> ")
            query = f"DELETE FROM {tabela} WHERE {primary_keys[tabela]} = %s"
            self.cursor.execute(query, (identificador_linha,))  # Passando como uma tupla

            self.conector.commit()
            print("Dados deletados")
            time.sleep(2)
            continuar = input("\nDeseja deletar outros dados? (S/N) >>> ").upper()
            if continuar != "S":
                break

            
    def mostrar_esquema(self):
        self.cursor.execute("SHOW TABLES")
        tabelas = self.cursor.fetchall()
        for tabela in tabelas:
            nome_tabela = tabela[0]
            print(f"Tabela: {nome_tabela}")
            self.cursor.execute(F"DESCRIBE {tabela[0]}")
            colunas = self.cursor.fetchall()
            print("  Colunas:")
            for coluna in colunas:
                print(f"    - {coluna[0]} ({coluna[1]})")
            print()


    def menu(self):
        while True:
            print("\nMenu:\n1. Ver Tabelas\n2. Criar Tabela\n3. Inserir Dados\n4. Atualizar Dados"
                  "\n5. Consultar Dados\n6. Deletar Dados\n0. Sair\n"
                  )

            escolha = input("Escolha uma opção >>> ")
            if escolha == '1':
                self.mostrar_esquema()
                time.sleep(5)
                input("Digite enter para voltar ao menu")
                return self.menu()
            elif escolha == '2':
                self.inserir_tabela()
            elif escolha == '3':
                self.inserir_dados()
            elif escolha == '4':
                self.atualizar_dados()
            elif escolha == '5':
                self.buscar_dados()
            elif escolha == '6':
                self.deletar_dados()
            elif escolha == '0':
                break
            else:
                print("Opção inválida. Digite qual opção deseja.")
                time.sleep(2)


    def obter_chaves_primarias(self):
        primary_keys = {}

        # Obtém o nome de todas as tabelas do banco de dados
        self.cursor.execute("SHOW TABLES")
        tabelas = [tabela[0] for tabela in self.cursor.fetchall()]

        # Para cada tabela, obtém a chave primária e a adiciona ao dicionário
        for tabela in tabelas:
            self.cursor.execute(f"SHOW KEYS FROM {tabela} WHERE Key_name = 'PRIMARY'")
            primary_key = self.cursor.fetchone()
            if primary_key:
                primary_keys[tabela] = primary_key[4]  # O nome da coluna que é a chave primária

        return primary_keys
    
    def obter_chaves_auto_incrementadas(self):
        auto_increment_columns = {}
        self.cursor.execute("SHOW TABLES")
        tabelas = [tabela[0] for tabela in self.cursor.fetchall()]

        # Para cada tabela, verifica se há colunas auto incrementadas e as adiciona ao dicionário
        for tabela in tabelas:
            self.cursor.execute(f"SHOW COLUMNS FROM {tabela} WHERE Extra LIKE '%auto_increment%'")
            colunas_auto_incrementadas = self.cursor.fetchall()
            for coluna in colunas_auto_incrementadas:
                auto_increment_columns[coluna[0]] = tabela

        return auto_increment_columns
    

# Dados de conexão
host = "aaa"
user = "aaa"
password = "aaa"
database = "aaa"

# Criar uma instância da classe SGBD
sgbd = SGBD(host, user, password, database)

# Inserir uma tabela
sgbd.menu()