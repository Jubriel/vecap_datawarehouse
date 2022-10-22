from sqlalchemy import create_engine
import os, argparse

def db_source_conn():
    # ## SOURCE CONNECTION
    POSTGRES_ADDRESS_S = 'test-vec-app.cxci97ncnvqs.us-east-1.rds.amazonaws.com'
    POSTGRES_PORT_S = 5432
    POSTGRES_USERNAME_S = 'postgres'
    POSTGRES_PASSWORD_S = '123vec_gh'
    POSTGRES_DBNAME_S = 'member_profile'

    source_postgres_str = ('postgresql+psycop2://{username}:{password}@{ipaddress}:{port}/{dbname}'.
                    format(username=POSTGRES_USERNAME_S,password=POSTGRES_PASSWORD_S,ipaddress=POSTGRES_ADDRESS_S,port=POSTGRES_PORT_S,dbname=POSTGRES_DBNAME_S))
    SourceConn = create_engine(source_postgres_str)
    return SourceConn


def db_target_conn():
    # ## TARGET CONNECTION
    POSTGRES_ADDRESS_T = 'test-vec-app.cxci97ncnvqs.us-east-1.rds.amazonaws.com'
    POSTGRES_PORT_T = 5432
    POSTGRES_USERNAME_T = 'postgres'
    POSTGRES_PASSWORD_T = '123vec_gh'
    POSTGRES_DBNAME_T = 'analytics'

    source_postgres_str = ('postgresql+psycopg2://{username}:{password}@{ipaddress}:{port}/{dbname}'.
                    format(username=POSTGRES_USERNAME_T,password=POSTGRES_PASSWORD_T,ipaddress=POSTGRES_ADDRESS_T,port=POSTGRES_PORT_T,dbname=POSTGRES_DBNAME_T))
    targetConn = create_engine(source_postgres_str)
    return targetConn

# def main(params):
#     schs = params.schs
#     scht = params.scht

#     return schs, scht

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description="Data Migration to Data Warehouse")
#     parser.add_argument('--schs', required=True, help="Schema of source database")
#     parser.add_argument('--scht', required=True, help="Schema of Target database")

#     args = parser.parse_args()
