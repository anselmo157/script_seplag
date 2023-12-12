from datetime import datetime
import psycopg2


def connect_db():
    connection = psycopg2.connect(host='localhost',
                                  database='cabemce_v01',
                                  user='postgres',
                                  password='34784575')
    return connection


def create_db(sql_create):
    con = connect_db()
    cur = con.cursor()
    cur.execute(sql_create)
    con.commit()
    con.close()


def execute_sql(sql, values):
    con = connect_db()
    cur = con.cursor()

    try:
        cur.execute(sql, values)
        con.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        con.rollback()
        cur.close()
        return 1

    cur.close()


def query_db(sql_query):
    con = connect_db()
    cur = con.cursor()
    cur.execute(sql_query)
    aux_query = cur.fetchall()
    registers = []
    for rec in aux_query:
        registers.append(rec)
    con.close()

    return registers


if __name__ == '__main__':
    sql_associados = """select nome_associado, matricula, codigo_orgao_averbador from associados
join orgaoaverbadores ON orgaoaverbadores.id_orgaoaverbador = associados.orgaoaverbador_id 
order by nome_associado"""

    associados = query_db(sql_associados)

    sql_seplag = """select nome, matricula, orgao, rubrica from adplantesmensalregistros where adplantesmensal_id = 
(select id from adplantesmensal order by id desc limit 1) order by nome"""

    seplag = query_db(sql_seplag)

    exists = []
    toAdd = []

    for i in range(len(seplag)):
        for j in range(len(associados)):
            if seplag[i][1] == associados[j][1] and str(seplag[i][2]) == str(associados[j][2]):
                exists.append(i)

    for i in range(len(seplag)):
        if i not in exists:
            toAdd.append(seplag[i])

    toRemove = []

    for i in range(len(toAdd)):
        for j in range(len(associados)):
            if toAdd[i][0] == associados[j][0] and (toAdd[i][1] == associados[j][1] or toAdd[i][2] == associados[j][2]):
                toRemove.append(i)

    for i in range(len(toAdd), 0, -1):
        if i in toRemove:
            toAdd.pop(i)

    for i in range(len(toAdd)):
        for j in range(len(toAdd)):
            if i != j:
                if toAdd[i][1] == toAdd[j][1] and str(toAdd[i][2]) == str(toAdd[j][2]):
                    print('i' + str(toAdd[i]) + 'j' + str(toAdd[j]))

    sql_drop_table = 'DROP TABLE IF EXISTS public.teste'
    create_db(sql_drop_table)

    sql_create_table = '''CREATE TABLE public.teste
                  ( id_associado             serial primary key, 
                    nome_associado           character varying(250), 
                    data_nascimento          date, 
                    cpf                      varchar, 
                    matricula                character varying(50), 
                    email1                   character varying(250), 
                    email2                   character varying(250),
                    telefone                 varchar,
                    folhapagamento_id        integer, 
                    orgaoaverbador_id        integer,
                    cidade_id                integer, 
                    postograduacao_id        integer,
                    bloqueio_id              integer,
                    created                  timestamp without time zone,
                    modified                 timestamp without time zone,
                    rubrica_id               integer,
                    nome_seplag              character varying,
                    observacao               character varying,
                    import_seplag            boolean,
                    rg                       character varying(15),
                    naturalidade             character varying,
                    estado_civil             integer,
                    imagem_associado         character varying,
                    genero                   character varying(1),
                    data_inicio              date
                  )'''
    create_db(sql_create_table)

    sql_drop_table = 'DROP TABLE IF EXISTS public.teste_enderecos'
    create_db(sql_drop_table)

    sql_create_table = '''CREATE TABLE public.teste_enderecos
                  ( id_endereco              serial primary key, 
                    denominacao              character varying(150), 
                    logradouro               character varying(250),
                    numero                   character varying(40),
                    complemento              character varying(100), 
                    bairro                   character varying(200), 
                    cep                      integer,
                    cidade_id                integer,
                    created                  timestamp without time zone,
                    modified                 timestamp without time zone,
                    associado_id             integer,
                    dependente_id            integer,
                    is_cobranca              boolean
                  )'''
    create_db(sql_create_table)

    sql_drop_table = 'DROP TABLE IF EXISTS public.teste_telefones'
    create_db(sql_drop_table)

    sql_create_table = '''CREATE TABLE public.teste_telefones
                      ( id_telefone              serial primary key, 
                        numero_telefone          text,
                        codigo_area              integer,
                        associado_id             integer,
                        created                  timestamp without time zone,
                        modified                 timestamp without time zone
                      )'''
    create_db(sql_create_table)

    for i in range(len(toAdd)):
        sql_insert_associados = """
            INSERT into public.teste (nome_associado,matricula,orgaoaverbador_id,rubrica_id,created,modified) 
            values(%s, %s, %s, %s, %s, %s);
            """
        sql_insert_associados_values = (toAdd[i][0], toAdd[i][1], toAdd[i][2], toAdd[i][3], datetime.now(),
                                        datetime.now())

        execute_sql(sql_insert_associados, sql_insert_associados_values)

    sql_silveira = """select nome, matricula, orgao, cpf, email, telefone, cep, endereco, 
num, complemento, bairro, municipio  from auxiliares.dados_silveira_v1 order by nome"""

    silveira = query_db(sql_silveira)

    sql_teste = """select nome_associado, matricula, orgaoaverbador_id, id_associado from teste order by 
    nome_associado"""

    teste = query_db(sql_teste)

    for i in range(len(teste)):
        for j in range(len(silveira)):
            if teste[i][0] == silveira[j][0] and (teste[i][1] == silveira[j][1] or teste[i][2] == silveira[j][2]):
                cpf = ''

                if len(silveira[j][3]) < 11:
                    zeros_plus = 11 - len(silveira[j][3])
                    cpf = (zeros_plus * '0') + silveira[j][3]
                    cpf = cpf[:9] + '-' + cpf[9:]
                else:
                    cpf = silveira[j][3][:9] + '-' + silveira[j][3][9:]

                cidade_id = None

                if silveira[j][11] is not None:
                    sql_cidade = """
                                    select id_cidade from cidades where nome_municipio = ('%s')
                                """ % (silveira[j][11].upper())

                    aux = query_db(sql_cidade)
                    if len(aux) > 0:
                        cidade_id = aux[0][0]

                ddd = None
                phone = None

                if silveira[j][5] is not None:
                    if len(silveira[j][5]) < 10:
                        ddd = None
                        if len(silveira[j][5]) < 8:
                            phone = '3' + silveira[j][5]
                        else:
                            phone = silveira[j][5]
                    else:
                        ddd = silveira[j][5][:2]
                        phone = silveira[j][5][2:]

                sql_insert_address = """insert into public.teste_enderecos (cep, logradouro, numero, complemento, 
                bairro, cidade_id, associado_id, created, modified) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                sql_insert_address_values = (silveira[j][6], silveira[j][7], silveira[j][8], silveira[j][9],
                                             silveira[j][10], cidade_id, teste[i][3], datetime.now(), datetime.now())

                sql_update = """update public.teste set cpf = %s, email1 = %s, telefone = %s, cidade_id = %s, 
                modified = %s where id_associado = %s"""
                sql_update_values = (cpf, silveira[j][4], silveira[j][5], cidade_id, datetime.now(), teste[i][3])

                sql_insert_phone = """insert into public.teste_telefones (numero_telefone, codigo_area, associado_id, 
                created, modified) values(%s, %s, %s, %s, %s)"""
                sql_insert_phone_values = (phone, ddd, teste[i][3], datetime.now(), datetime.now())

                execute_sql(sql_update, sql_update_values)
                if silveira[j][7] is not None:
                    execute_sql(sql_insert_address, sql_insert_address_values)
                if silveira[j][5] is not None:
                    execute_sql(sql_insert_phone, sql_insert_phone_values)

    sql_associados = """select id_associado, cpf from public.associados order by id_associado"""

    associados = query_db(sql_associados)

    sql_teste = """select id_associado, cpf from public.teste order by id_associado"""

    teste = query_db(sql_teste)

    for i in range(len(teste)):
        for j in range(len(associados)):
            if teste[i][1] is not None and teste[i][1] == associados[j][1]:
                sql_remove_cpf = """
                             update public.teste set cpf = null where id_associado = %s
                             """
                sql_remove_cpf_values = (teste[i][0],)

                execute_sql(sql_remove_cpf, sql_remove_cpf_values)
