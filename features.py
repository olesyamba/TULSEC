import pandas as pd
import psycopg2
from sshtunnel import SSHTunnelForwarder
import os
import chardet
from dotenv import load_dotenv

'''
# ПОДКЛЮЧЕНИЕ БЕЗ SSH TUNNEL

# Database connection details
conn_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT"))
}

# Connect to the database
connection = psycopg2.connect(**conn_params)
cursor = connection.cursor()
'''

# Определяем кодировку
with open('.env', 'rb') as file:
    encoding = chardet.detect(file.read())['encoding']

load_dotenv(encoding=encoding)

# build table for features
try:
    with SSHTunnelForwarder(
            (os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT"))),
            # ssh_private_key="</path/to/private/ssh/key>",
            ssh_username=os.getenv("SSH_LOGIN"),
            ssh_password=os.getenv("SSH_PASSWORD"),
            remote_bind_address=(os.getenv("DB_HOST"), int(os.getenv("DB_PORT")))) as server:

        server.start()
        print("server connected")
        server.start()

        params = {
            'database': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': 'localhost',
            'port': server.local_bind_port
        }

        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        print("database connected")

        queries = ['DROP TABLE IF EXISTS ml.infrastructure_features', # Запрос на удаление таблицы фичей, если есть
                   'SELECT * INTO ml.infrastructure_features '
                   'FROM ml.polygonal_grid_500_filtered_int',  # Запрос на создание таблицы фичей
                   'ALTER TABLE ml.infrastructure_features '
                   'ADD CONSTRAINT infrastructure_features_pk PRIMARY KEY (id)',  # Запрос на добавление ключа по айди
                   'CREATE INDEX idx_infrastructure_features_coords '
                   'ON ml.infrastructure_features USING GIST(coords)']  # Запрос на создание индекса

        for query in queries:
            cursor.execute(query)
            conn.commit()

        # Close the connection
        cursor.close()
        conn.close()

except:
    print("Connection Failed")

# shelters
try:
    with SSHTunnelForwarder(
            (os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT"))),
            # ssh_private_key="</path/to/private/ssh/key>",
            ssh_username=os.getenv("SSH_LOGIN"),
            ssh_password=os.getenv("SSH_PASSWORD"),
            remote_bind_address=(os.getenv("DB_HOST"), int(os.getenv("DB_PORT")))) as server:

        server.start()
        print("server connected")
        server.start()

        params = {
            'database': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': 'localhost',
            'port': server.local_bind_port
        }

        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        print("database connected")

        cursor.execute("""
            SELECT scheme, table_name, geometry_column, geometry_srid 
            FROM ml.feature_tables_names
            WHERE scheme = 'shelters' 
            """)
        conn.commit()
        # geometry_tables = [row[0] for row in cursor.fetchall()]
        geometry_tables = cursor.fetchall()

        table_with_features = 'infrastructure_features'
        scheme_table_with_features = 'ml'
        raw_srid = []

        try:
            for table in geometry_tables:

                if table[3] == 4326:
                    # count infrastructure objects feature

                    # Add a new column to the base_polygons table for the current geometry table
                    column_name = f"{table[0]}_{table[1]}_cnt"
                    add_column_query = f"""
                    ALTER TABLE {scheme_table_with_features}.{table_with_features}
                    ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0;
                    """
                    cursor.execute(add_column_query)
                    conn.commit()
                    print(f"Added column: {column_name}")

                    # Update the column based on intersection
                    update_query = f"""
                    WITH buildings as (
                        WITH bs as 
                            (SELECT * FROM osm.buildings)
                        SELECT sh.osm_id, bs.{table[2]} 
                        FROM {table[0]}.{table[1]} sh
                        JOIN bs on sh.osm_id = bs.osm_id)
                    UPDATE {scheme_table_with_features}.{table_with_features} b
                    SET {column_name} = (
                        SELECT COUNT(*)
                        FROM buildings o
                        WHERE ST_Intersects(b.coords, o.{table[2]})
                    );
                    """
                    cursor.execute(update_query)
                    conn.commit()
                    print(f"Updated column: {column_name}")

                    # min distance infrastructure objects feature

                    # Add a new column to the base_polygons table for the current geometry table
                    column_name = f"{table[0]}_{table[1]}_min_dist"
                    add_column_query = f"""
                                        ALTER TABLE {scheme_table_with_features}.{table_with_features}
                                        ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0;
                                        """
                    cursor.execute(add_column_query)
                    conn.commit()
                    print(f"Added column: {column_name}")

                    # Update the column based on intersection
                    update_query = f"""
                                        WITH buildings as (
                                            WITH bs as 
                                                (SELECT * FROM osm.buildings)
                                            SELECT sh.osm_id, bs.{table[2]} 
                                            FROM {table[0]}.{table[1]} sh
                                            JOIN bs on sh.osm_id = bs.osm_id)
                                        UPDATE {scheme_table_with_features}.{table_with_features} b
                                        SET {column_name} = (
                                            SELECT MIN(ST_Distance(ST_Transform(b.coords, 3857),
                                                                   ST_Transform(o.{table[2]}, 3857)))
                                            FROM buildings o
                                        );
                                        """

                    cursor.execute(update_query)
                    conn.commit()
                    print(f"Updated column: {column_name}")
                else:
                    raw_srid.append(table)

            # Commit changes
            conn.commit()
            print("All geometry tables processed successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()  # Rollback in case of any error

        finally:
            # Close the connection
            cursor.close()
            conn.close()
            if raw_srid:
                print(f'Columns with different srid: \n {raw_srid}')

except:
    print("Connection Failed")

# opendata
try:
    with SSHTunnelForwarder(
            (os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT"))),
            # ssh_private_key="</path/to/private/ssh/key>",
            ssh_username=os.getenv("SSH_LOGIN"),
            ssh_password=os.getenv("SSH_PASSWORD"),
            remote_bind_address=(os.getenv("DB_HOST"), int(os.getenv("DB_PORT")))) as server:

        server.start()
        print("server connected")
        server.start()

        params = {
            'database': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': 'localhost',
            'port': server.local_bind_port
        }

        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        print("database connected")

        cursor.execute("""
            SELECT scheme, table_name, geometry_column, geometry_srid 
            FROM ml.feature_tables_names
            WHERE scheme = 'opendata' 
            """)
        conn.commit()
        # geometry_tables = [row[0] for row in cursor.fetchall()]
        geometry_tables = cursor.fetchall()

        table_with_features = 'infrastructure_features'
        scheme_table_with_features = 'ml'
        raw_srid = []

        try:
            for table in geometry_tables:

                create_index_query = f"""
                                    CREATE INDEX IF NOT EXISTS idx_{table[1]}_{table[2]}
                                    ON {table[0]}.{table[1]} USING GIST({table[2]})
                                    """
                cursor.execute(create_index_query)
                conn.commit()

                if table[3] == 4326:
                    # count infrastructure objects feature

                    # Add a new column to the base_polygons table for the current geometry table
                    column_name = f"{table[0]}_{table[1]}_cnt"
                    add_column_query = f"""
                    ALTER TABLE {scheme_table_with_features}.{table_with_features}
                    ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0;
                    """
                    cursor.execute(add_column_query)
                    conn.commit()
                    print(f"Added column: {column_name}")

                    # Update the column based on intersection
                    update_query = f"""
                    UPDATE {scheme_table_with_features}.{table_with_features} b
                    SET {column_name} = (
                        SELECT COUNT(*)
                        FROM {table[0]}.{table[1]} o
                        WHERE ST_Intersects(b.coords, o.{table[2]})
                    );
                    """
                    cursor.execute(update_query)
                    conn.commit()
                    print(f"Updated column: {column_name}")

                    # min distance infrastructure objects feature

                    # Add a new column to the base_polygons table for the current geometry table
                    column_name = f"{table[0]}_{table[1]}_min_dist"
                    add_column_query = f"""
                                        ALTER TABLE {scheme_table_with_features}.{table_with_features}
                                        ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0;
                                        """
                    cursor.execute(add_column_query)
                    conn.commit()
                    print(f"Added column: {column_name}")

                    # Update the column based on intersection
                    update_query = f"""
                                        UPDATE {scheme_table_with_features}.{table_with_features} b
                                        SET {column_name} = (
                                            SELECT MIN(ST_Distance(ST_Transform(b.coords, 3857),
                                                                   ST_Transform(o.{table[2]}, 3857)))
                                            FROM {table[0]}.{table[1]} o
                                        );
                                    """
                    cursor.execute(update_query)
                    conn.commit()
                    print(f"Updated column: {column_name}")

                else:
                    raw_srid.append(table)

            # Commit changes
            conn.commit()
            print("All geometry tables processed successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()  # Rollback in case of any error

        finally:
            # Close the connection
            cursor.close()
            conn.close()
            if raw_srid:
                print(f'Columns with different srid: \n {raw_srid}')

except:
    print("Connection Failed")

# rgis
try:
    with SSHTunnelForwarder(
            (os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT"))),
            # ssh_private_key="</path/to/private/ssh/key>",
            ssh_username=os.getenv("SSH_LOGIN"),
            ssh_password=os.getenv("SSH_PASSWORD"),
            remote_bind_address=(os.getenv("DB_HOST"), int(os.getenv("DB_PORT")))) as server:

        server.start()
        print("server connected")
        server.start()

        params = {
            'database': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': 'localhost',
            'port': server.local_bind_port
        }

        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        print("database connected")

        cursor.execute("""
            SELECT scheme, table_name, geometry_column, geometry_srid 
            FROM ml.feature_tables_names
            WHERE scheme = 'rgis' 
            """)
        conn.commit()
        # geometry_tables = [row[0] for row in cursor.fetchall()]
        geometry_tables = cursor.fetchall()

        table_with_features = 'infrastructure_features'
        scheme_table_with_features = 'ml'
        raw_srid = []
        layer_ids = [3, 4, 5, 6, 7, 8, 9, 10,
                     11, 13, 14, 15, 16, 17,
                     18, 20, 21, 22, 23, 27,
                     28, 29, 30, 31, 32, 33,
                     34, 35, 36, 38, 39, 40,
                     41, 43, 44, 45]

        try:
            for table in geometry_tables:

                create_index_query = f"""
                                                    CREATE INDEX IF NOT EXISTS idx_{table[1]}_{table[2]}
                                                    ON {table[0]}.{table[1]} USING GIST({table[2]})
                                                    """
                cursor.execute(create_index_query)
                conn.commit()

                for layer_id in layer_ids:
                    if table[3] == 4326:
                        # count infrastructure objects feature

                        # Add a new column to the base_polygons table for the current geometry table
                        column_name = f"{table[0]}_{table[1]}_layer_id_{layer_id}_cnt"
                        add_column_query = f"""
                        ALTER TABLE {scheme_table_with_features}.{table_with_features}
                        ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0;
                        """
                        cursor.execute(add_column_query)
                        conn.commit()
                        print(f"Added column: {column_name}")

                        # Update the column based on intersection
                        update_query = f"""
                        UPDATE {scheme_table_with_features}.{table_with_features} b
                        SET {column_name} = (
                            SELECT COUNT(*)
                            FROM (
                                SELECT 
                                    * 
                                FROM {table[0]}.{table[1]} 
                                WHERE layer_id = {layer_id}
                                ) o
                            WHERE ST_Intersects(b.coords, o.{table[2]})
                        );
                        """
                        cursor.execute(update_query)
                        conn.commit()
                        print(f"Updated column: {column_name}")

                        # min distance infrastructure objects feature

                        # Add a new column to the base_polygons table for the current geometry table
                        column_name = f"{table[0]}_{table[1]}_layer_id_{layer_id}_min_dist"
                        add_column_query = f"""
                                                ALTER TABLE {scheme_table_with_features}.{table_with_features}
                                                ADD COLUMN IF NOT EXISTS {column_name} INT DEFAULT 0;
                                                """
                        cursor.execute(add_column_query)
                        conn.commit()
                        print(f"Added column: {column_name}")

                        # Update the column based on intersection
                        update_query = f"""
                                                UPDATE {scheme_table_with_features}.{table_with_features} b
                                                SET {column_name} = (
                                                    SELECT MIN(ST_Distance(ST_Transform(b.coords, 3857),
                                                                           ST_Transform(o.{table[2]}, 3857)))
                                                    FROM (
                                                        SELECT 
                                                            * 
                                                        FROM {table[0]}.{table[1]} 
                                                        WHERE layer_id = {layer_id}
                                                        ) o
                                                );
                                                """

                        cursor.execute(update_query)
                        conn.commit()
                        print(f"Updated column: {column_name}")
                    else:
                        raw_srid.append(table)

            # Commit changes
            conn.commit()
            print("All geometry tables processed successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()  # Rollback in case of any error

        finally:
            # Close the connection
            cursor.close()
            conn.close()
            if raw_srid:
                print(f'Columns with different srid: \n {raw_srid}')

except:
    print("Connection Failed")