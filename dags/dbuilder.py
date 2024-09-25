import datetime

from bson import ObjectId


class DBuilder():
    """ По сути `create_table` и `insert_values` - генераторы строки, которая в виде запроса будет подаваться в sql через `cur.execute()` """

    def __init__(self, postgre_connection):
        self.pg_conn = postgre_connection

    def is_table_exists(self, table_name) -> bool:
        """ Check if `table_name` exists in database """

        cur = self.pg_conn.cursor()
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)"
        cur.execute(query, (table_name,))
        table_exists = cur.fetchone()[0]
        self.pg_conn.commit()
        cur.close()

        return table_exists

    def create_table(self, schema_name, table_name, columns, types='text'):
        """ Создаёт таблицу `table_name` по указанным `columns` с указанными типами `types` """

        # Комментарии по логике кода в `insert_values`
        col_substr_list = []
        if types == 'text':
            for col in columns:
                col_substr_list.append(f"{col} text")
        else:
            for col, type in zip(columns, types):
                col_substr_list.append(f"{col} {type}")

        col_substr_list.append(f"inserted_at timestamp")
        col_substr_list.append(f"flag text")
        col_substr = ', '.join(col_substr_list)
        query = f"CREATE TABLE {schema_name}.{table_name}({col_substr})"
        print(f"query: {query} of {table_name}")
        cur = self.pg_conn.cursor()
        cur.execute(query)
        self.pg_conn.commit()
        cur.close()

    def insert_values(self, schema, table, values):
        """ insert into `schema`.`table` `values` """

        val_substr_list = []
        for row in values:
            print('row: ', row)
            str_row = "', '".join(row)
            str_row = "'" + str_row + "', now(), 'False'"
            val_substr_list.append(
                str_row)  # Список строк значений, например ["'1', '2', now(), 'False'", "'3', '4', now(), 'False'"]
        inserting_val = '), ('.join(
            val_substr_list)  # Пример: "'1', '2', now(), 'False'), ('3', '4', now(), 'False'), ('5', '6', now(), 'False'"
        print(f"inserting_val: {inserting_val} of {table}")
        query = f"INSERT INTO {schema}.{table} VALUES ({inserting_val})"
        cur = self.pg_conn.cursor()
        cur.execute(query)
        self.pg_conn.commit()
        cur.close()


class MongoBuilder(DBuilder):

    def __init__(self, mongo_collection_object, postgre_connection, tb_name, is_delta):
        super().__init__(postgre_connection)
        self.mongo_collection = mongo_collection_object
        self.collection_name = tb_name
        self.is_delta = is_delta

    def build_mongo_staging(self):
        """ Вставка данных из монго коллекции в таблицу postgres или в несколько связанных таблиц postgres
                            в зависимости от вложенности документов коллекции """

        documents = list(self.mongo_collection.find())
        doc_ids = list(range(len(documents)))  # Номера документов
        delta_docs = []
        delta_id_docs = []
        if self.is_delta:  # Фильтруем только необходимые документы для загрузки в staging
            cur = self.pg_conn.cursor()
            cur.execute("SELECT mongo_actual_time FROM staging.settings")
            results = cur.fetchall()
            self.pg_conn.commit()
            cur.close()
            if results:
                mongo_actual_time = results[0][0]
                mongo_actual_time_dt = datetime.datetime.strptime(str(mongo_actual_time), '%Y-%m-%d %H:%M:%S')
                print(mongo_actual_time_dt)
                # Находим те `update_time` в документах, которые больше `mongo_actual_time` из `staging.settings`
                for i, doc in enumerate(documents):
                    if '.' in str(doc['update_time']):
                        doc['update_time'] = str(doc['update_time'])[:-7]
                    print(str(doc['update_time']))
                    if datetime.datetime.strptime(str(doc['update_time']), '%Y-%m-%d %H:%M:%S') > mongo_actual_time_dt:
                        delta_docs.append(doc)
                        delta_id_docs.append(i)
                print(delta_docs)
                # Сохраняем те документы, в которых `update_time` > `mongo_actual_time` и идём весело их парсить для загрузки в стэйджинг
                documents = delta_docs
                doc_ids = delta_id_docs
            else:
                raise TypeError(
                    "Delta-load is not available. Init load is required. No mongo timestamp in staging.settings")
        doc_values = []
        for i, doc in zip(doc_ids, documents):
            print(doc)
            doc_list = []
            for key, value in doc.items():
                if isinstance(value, ObjectId) or isinstance(value, datetime.datetime):
                    doc[key] = str(value)
                elif isinstance(value, dict):
                    doc[key] = str(list(value.values())[0])
                elif isinstance(value, list):
                    # Парсим вложенные словарики
                    table_name = f"mongo_{key}"
                    cols = [f"{key}_id"]
                    cols.extend(list(value[0].keys()))  # Составили список полей внутреннего словаря
                    is_additive_table_exists = self.is_table_exists(table_name)
                    print(is_additive_table_exists)
                    # Если таблицы `table_name` нет, то создаём её
                    if not is_additive_table_exists:
                        self.create_table('staging', table_name, cols)
                    # Как писал выше приводим значения к стрингу для инсёрта в стэйдж и собираем их в список для отправки в `insert_values`
                    dict_values = []
                    for data_dict in value:
                        for key1, value1 in data_dict.items():
                            data_dict[key1] = str(value1)
                        print(f"data_dict: {data_dict}")
                        values_with_id = [
                            str(i + 1)]  # первым значением будет номер документа, в продв. случае `_id` (не рассматривалось)
                        values_with_id.extend(list(data_dict.values()))
                        dict_values.append(values_with_id)
                    self.insert_values('staging', table_name, dict_values)
                    # После инсерта вложенной структуры, она больше не нужна. Заменяем её на номер документа. 
                    # В продвинутом случае можно удалить, но нужно быть внимательным с обходом цикла.
                    doc[key] = str(i + 1)
                else:
                    doc[key] = str(value)
                doc_list.append(doc[key])
            doc_values.append(doc_list)
        # Собрали значения в doc_values и если они вообще есть, то вставляем данные в реляционный вид
        if len(doc_values) > 0:
            self.insert_values('staging', f"mongo_{self.collection_name}", doc_values)
