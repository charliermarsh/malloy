import {
  Connection,
  LookupConnection,
  Model,
  Result,
} from "@malloydata/malloy";
import { useEffect, useState } from "react";
import { compileModel, initializeConnections, runQuery } from "./compile";

export function useConnections(): LookupConnection<Connection> | undefined {
  const [connections, setConnections] =
    useState<LookupConnection<Connection>>();

  useEffect(() => {
    initializeConnections().then(setConnections);
  }, []);

  return connections;
}

export function useModel(
  source: string,
  connections?: LookupConnection<Connection>
): Model | undefined {
  const [model, setModel] = useState<Model>();

  useEffect(() => {
    if (connections == null) return;
    compileModel(source, connections).then(setModel);
  }, [source, connections]);

  return model;
}

export function useResult(
  model?: Model,
  queryName?: string,
  connections?: LookupConnection<Connection>
): Result | undefined {
  const [result, setResult] = useState<Result>();

  useEffect(() => {
    if (model == null || queryName == null || connections == null) return;
    const preparedResult =
      model.getPreparedQueryByName(queryName).preparedResult;
    runQuery(preparedResult, connections).then(setResult);
  }, [model, queryName, connections]);

  return result;
}
