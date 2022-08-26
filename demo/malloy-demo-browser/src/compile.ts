import { DuckDBWasmConnection } from "@malloydata/db-duckdb-wasm/src";
import {
  Connection,
  FixedConnectionMap,
  LookupConnection,
  Malloy,
  Model,
  PreparedResult,
  Result,
  URLReader,
} from "@malloydata/malloy/src";

class DummyFiles implements URLReader {
  async readURL(): Promise<string> {
    return "";
  }
}

export async function initializeConnections(): Promise<
  LookupConnection<Connection>
> {
  const connection = new DuckDBWasmConnection("duckdb-wasm");
  return new FixedConnectionMap(
    new Map([["duckdb-wasm", connection]]),
    "duckdb-wasm"
  );
}

export async function compileModel(
  source: string,
  connections: LookupConnection<Connection>
): Promise<Model> {
  return await Malloy.compile({
    urlReader: new DummyFiles(),
    connections,
    parse: Malloy.parse({ source }),
  });
}

export async function runQuery(
  preparedResult: PreparedResult,
  connections: LookupConnection<Connection>
): Promise<Result> {
  return await Malloy.run({
    connections,
    preparedResult,
  });
}
