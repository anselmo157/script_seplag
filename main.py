from datetime import datetime
import psycopg2


def connect_db():
    connection = psycopg2.connect(host='localhost',
                                  database='cabemce',
                                  user='martins',
                                  password='34784575')
    return connection


def create_db(sql_create):
    con = connect_db()
    cur = con.cursor()
    cur.execute(sql_create)
    con.commit()
    con.close()


def execute_sql(sql, values, return_id=False):
    con = connect_db()
    cur = con.cursor()

    aux_sql = None

    try:
        cur.execute(sql, values)
        if return_id:
            aux_sql = cur.fetchall()
        con.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        con.rollback()
        cur.close()
        return 1

    cur.close()

    if return_id:
        return aux_sql[0][0]


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
    sql_associados = """select nome_associado, matricula, codigo_orgao_averbador, cpf from associados
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

    added_associados = list()

    for i in range(len(toAdd)):

        rubrica_id = None

        if toAdd[i][3] == 633:
            rubrica_id = 1
        else:
            rubrica_id = 2

        sql_orgaoaverbador_id = """
                                select id_orgaoaverbador from orgaoaverbadores where codigo_orgao_averbador = (%s)
                                """ % (toAdd[i][2])
        orgaoaverbador_id = query_db(sql_orgaoaverbador_id)[0][0]

        sql_insert_associados = """
            insert into public.associados (nome_associado,matricula,orgaoaverbador_id,rubrica_id,data_nascimento,
            created,modified,import_seplag) 
            values(%s, %s, %s, %s, %s, %s, %s, %s) returning id_associado;
            """
        sql_insert_associados_values = (toAdd[i][0], toAdd[i][1], orgaoaverbador_id, rubrica_id, '1900-01-01',
                                        datetime.now(), datetime.now(), True)

        id_associado = execute_sql(sql_insert_associados, sql_insert_associados_values, True)
        added_associados.append([toAdd[i], id_associado])

    sql_silveira = """select nome, matricula, orgao, cpf, email, telefone, cep, endereco, 
    num, complemento, bairro, municipio  from auxiliares.dados_silveira_v1 order by nome"""

    silveira = query_db(sql_silveira)

    for i in range(len(added_associados)):

        for j in range(len(silveira)):
            if added_associados[i][0][0] == silveira[j][0] and (added_associados[i][0][1] == silveira[j][1] or
                                                                added_associados[i][0][2] == silveira[j][2]):
                cpf = None

                if silveira[j][3] is not None:
                    if len(silveira[j][3]) < 11:
                        zeros_plus = 11 - len(silveira[j][3])
                        cpf = (zeros_plus * '0') + silveira[j][3]
                        cpf = cpf[:9] + '-' + cpf[9:]
                    else:
                        cpf = silveira[j][3][:9] + '-' + silveira[j][3][9:]

                    for k in range(len(associados)):
                        if cpf == associados[k][3]:
                            cpf = None
                        if cpf == '034832395-61':
                            cpf = None
                        if cpf == '118989333-91':
                            cpf = None
                        if cpf == '078785294-50':
                            cpf = None

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
                        ddd = 00
                        if len(silveira[j][5]) < 8:
                            phone = '3' + silveira[j][5]
                        else:
                            phone = silveira[j][5]
                    else:
                        ddd = silveira[j][5][:2]
                        phone = silveira[j][5][2:]

                associado_id = added_associados[i][1]

                sql_insert_address = """insert into public.enderecos (cep, logradouro, numero, complemento, 
                bairro, cidade_id, associado_id, denominacao, created, modified) values(%s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s)"""
                sql_insert_address_values = (silveira[j][6], silveira[j][7], silveira[j][8], silveira[j][9],
                                             silveira[j][10], cidade_id, associado_id, 'Endereco Importação',
                                             datetime.now(), datetime.now())

                sql_update = """update public.associados set cpf = %s, email1 = %s, cidade_id = %s, 
                modified = %s where id_associado = %s"""
                sql_update_values = (cpf, silveira[j][4], cidade_id, datetime.now(), associado_id)

                sql_insert_phone = """insert into public.telefone (numero_telefone, codigo_area, telecomempresa_id, 
                associado_id, created, modified) values(%s, %s, %s, %s, %s, %s)"""
                sql_insert_phone_values = (phone, ddd, 5, associado_id, datetime.now(), datetime.now())

                execute_sql(sql_update, sql_update_values)
                if silveira[j][7] is not None:
                    execute_sql(sql_insert_address, sql_insert_address_values)
                if silveira[j][5] is not None:
                    execute_sql(sql_insert_phone, sql_insert_phone_values)
