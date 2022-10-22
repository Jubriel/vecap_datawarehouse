# Upsert function for pandas to_sql with postgres
# https://stackoverflow.com/questions/1109061/insert-on-duplicate-update-in-postgresql/8702291#8702291
# https://www.postgresql.org/docs/devel/sql-insert.html#SQL-ON-CONFLICT
import pandas as pd
import sqlalchemy
import uuid
import os


def upsert_df(df: pd.DataFrame, table_name: str, sch:str, engine: sqlalchemy.engine.Engine, pk:str):
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set primary keys on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name
    Returns: True if successful
    """

    # If the table does not exist, we should just use to_sql to create it
    if not engine.execute(
        f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = '{sch}'
            AND    table_name   = '{table_name}');
            """
    ).first()[0]:
        df.to_sql(table_name, engine, schema= sch)
        return True

    # If it already exists...
    temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
    engine.execute(f"CREATE TABLE {sch}.{temp_table_name} AS (SELECT * FROM {sch}.{table_name}) LIMIT 0;")

    df.to_sql(temp_table_name, engine,  schema= sch, index = True)

    index = pk
    index_sql_txt = ", ".join([f'{i}' for i in index])
    columns = list(df.columns)
    headers = index + columns
    headers_sql_txt = ", ".join(
        [f"{i}" for i in headers]
    )  # index1, index2, ..., column 1, col2, ...

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])

    # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
    query_pk = f"""
    ALTER TABLE {sch}.{table_name} DROP CONSTRAINT IF EXISTS unique_constraint_for_upsert;
    ALTER TABLE {sch}.{table_name} ADD CONSTRAINT unique_constraint_for_upsert UNIQUE ({index_sql_txt});
    """
    try:
        engine.execute(query_pk)
    except Exception as e:
        # relation "unique_constraint_for_upsert" already exists
        if not 'unique_constraint_for_upsert already exists' in e.args[0]:
            raise e

    # Compose and execute upsert query
    query_upsert = f"""
    INSERT INTO {sch}.{table_name} ({headers_sql_txt}) 
    SELECT {headers_sql_txt} FROM {sch}.{temp_table_name}
    ON CONFLICT ({index_sql_txt}) DO UPDATE 
    SET {update_column_stmt};
    """
    engine.execute(query_upsert)
    engine.execute(f'DROP TABLE {sch}.{temp_table_name}')

    return True


# if __name__ == "__main__":
#     # TESTS (create environment variable DB_STR to do it)
#     engine = sqlalchemy.create_engine(os.getenv("DB_STR"))

#     indexes = ["id1", "id2"]