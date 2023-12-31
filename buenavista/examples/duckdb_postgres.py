import os
import sys
from typing import Tuple

import duckdb
from duckdb.typing import VARCHAR,BIGINT


from ..backends.duckdb import DuckDBConnection
from .. import bv_dialects, postgres, rewrite


class DuckDBPostgresRewriter(rewrite.Rewriter):
    def rewrite(self, sql: str) -> str:
        return sql
        # if sql.lower() == "select pg_catalog.version()":
        #     return "SELECT 'PostgreSQL 9.3' as version"
        # else:
        #     return super().rewrite(sql)


rewriter = DuckDBPostgresRewriter(bv_dialects.BVPostgres(), bv_dialects.BVDuckDB())

def _quote_ident(val: str) -> str:
    return '"%s"' % val.replace('"', '""')

def array_upper(*args):
    """
    todo: multidimensional arrays must have array expressions with matching dimensions
    todo: maybe numpy.array is better than list
    """
    if len(args) < 2:
        return None
    
    arr = args[0]
    dimension = args[1]

    if (not isinstance(arr, list) 
        or not isinstance(dimension, int) 
        or dimension <= 0 
        or dimension > len(arr)):
        return None
    try:
        if dimension==1:
            return len(arr)
        else:
            return len(arr[dimension - 1])
    except:
        return None
    
def create(
    db: duckdb.DuckDBPyConnection, host_addr: Tuple[str, int], auth: dict = None
) -> postgres.BuenaVistaServer:
    server = postgres.BuenaVistaServer(
        host_addr, DuckDBConnection(db), rewriter=rewriter, auth=auth
    )
    return server


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Using in-memory DuckDB database")
        db = duckdb.connect()
    else:
        print("Using DuckDB database at %s" % sys.argv[1])
        db = duckdb.connect(sys.argv[1])

    bv_host = "0.0.0.0"
    bv_port = 5433

    if "BUENAVISTA_HOST" in os.environ:
        bv_host = os.environ["BUENAVISTA_HOST"]

    if "BUENAVISTA_PORT" in os.environ:
        bv_port = int(os.environ["BUENAVISTA_PORT"])

    address = (bv_host, bv_port)
    db.create_function('quote_ident',_quote_ident, [VARCHAR], VARCHAR)
    db.create_function('array_upper',array_upper, None,BIGINT)
    server = create(db, address)
    ip, port = server.server_address
    print(f"Listening on {ip}:{port}")

    try:
        server.serve_forever()
    finally:
        server.shutdown()
        db.close()
