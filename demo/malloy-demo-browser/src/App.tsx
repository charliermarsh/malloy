import hljs from "highlight.js";
import "highlight.js/styles/github.css";
import { useState } from "react";
import Label from "./Components/Label";
import Selector from "./Components/Selector";
import { GEOGRAPHY_TO_QUERY, MODEL } from "./data";
import { useConnections, useModel } from "./hooks";
import { Geography } from "./types";

export default function App() {
  const connections = useConnections();
  const model = useModel(MODEL, connections);

  const [geography, setGeography] = useState<Geography>("Georgia");

  if (model == null) {
    return (
      <div className={"w-screen h-screen flex justify-center items-center"}>
        Loading...
      </div>
    );
  }

  const { sql } = model.getPreparedQueryByName(
    GEOGRAPHY_TO_QUERY[geography]
  ).preparedResult;

  return (
    <div className={"text-xs flex flex-row w-screen h-screen p-4"}>
      <div className={"w-1/2 mr-2 p-4 rounded border border-slate-300"}>
        <pre className={"w-full h-full overflow-scroll relative"}>
          <div className={"absolute top-0 right-0"}>
            <Label>Malloy</Label>
          </div>
          <code
            dangerouslySetInnerHTML={{
              __html: hljs.highlight("sql", MODEL).value,
            }}
          />
        </pre>
      </div>
      <div className={"w-1/2 ml-2 p-4 rounded border border-slate-300"}>
        <pre className={"w-full h-full overflow-scroll relative hljs"}>
          <div className={"absolute top-0 right-0"}>
            <Label>SQL</Label>
          </div>
          <div className={"absolute top-8 right-0"}>
            <Selector selected={geography} onChange={setGeography} />
          </div>
          <code
            dangerouslySetInnerHTML={{
              __html: hljs.highlight(sql, { language: "sql" }).value,
            }}
          />
        </pre>
      </div>
    </div>
  );
}
