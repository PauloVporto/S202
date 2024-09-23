from neo4j import GraphDatabase, Driver


def run_query(db, query, **parameters):
    with db.session() as session:
        result = session.run(query, **parameters)
        return list(result)


def engineers(db):
    query = "MATCH (p:Pessoa:Engenheiro) RETURN p.nome AS nome"
    return run_query(db, query)


def father_mother(db, name):
    query = "MATCH (p:Pessoa {nome: $name})-[:PAI_DE|MAE_DE]->(child:Pessoa) RETURN child.nome AS nome"
    return run_query(db, query, name=name)


def partner_since(db, name):
    query = "MATCH (p:Pessoa {nome: $name})-[r:CASADO_COM]->(partner:Pessoa) RETURN partner.nome AS nome, r.ano_de_casamento AS ano_de_casamento"
    return run_query(db, query, name=name)


def create_user(db, name, profession):
    query = "CREATE (p:Pessoa {nome: $name, profissao: $profession})"
    run_query(db, query, name=name, profession=profession)
    
def siblings_of(db, name):
    query = """
    MATCH (a:Pessoa {nome: $name})-[:IRMÃ_DE|IRMÃO_DE]->(sibling:Pessoa)
    WHERE a <> sibling
    RETURN sibling.nome AS nome
    """
    return run_query(db, query, name=name)


def print_menu():
    title = " Exercício avaliativo sobre NEO4J "
    separator = "=" * 10
    top_bottom_separator = "=" * len(title)

    print(top_bottom_separator)
    print(f"{separator}{title}{separator}")
    print(top_bottom_separator)

    print("1. Desde quando está casado?")
    print("2. Listar irmãos de uma pessoa")
    print("3. Quem é engenheiro?")
    print("4. Quem é pai de quem?")
    print("5. Criar novo usuário")
    print("6. Sair")


def main(neo4j: Driver):
    print_menu()

    while True:
        option = input("Escolha uma das opções: ")

        if option == "6":
            break

        if option == "1":
            name = input("Insira o nome da pessoa: ")
            partners = partner_since(neo4j, name)
            for partner in partners:
                print(f"{name} é casado(a) com {partner['nome']} desde {partner['ano_de_casamento']}")
            continue
        
        if option == "2":
            name = input("Insira o nome da pessoa: ")
            siblings = siblings_of(neo4j, name)
            if siblings:
                print(f"Irmãos de {name}:")
                for sibling in siblings:
                    print(sibling['nome'])
            else:
                print(f"{name} não possui irmãos.")
            continue
        
        if option == "3":
            eng = engineers(neo4j)
            for engineer in eng:
                print(f"Engenheiro: {engineer['nome']}")
            continue
        
        if option == "4":
            name = input("Insira o nome da pessoa: ")
            children = father_mother(neo4j, name)
            for child in children:
                print(f"{name} é pai/mãe de: {child['nome']}")
            continue

        if option == "5":
            name = input("Insira o nome do novo usuário: ")
            profession = input("Insira a profissão do novo usuário: ")
            create_user(neo4j, name, profession)
            print("Usuário criado com sucesso!")
            continue

        print("Opção inválida. Por favor, escolha uma opção válida.")


if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = ""

    driver = GraphDatabase.driver(uri, auth=(user, password))

    main(driver)
