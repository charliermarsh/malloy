import { Geography } from "../types";

const ALL_GEOGRAPHIES: Geography[] = ["Georgia", "Pennsylvania"];

export default function Selector({
  selected,
  onChange,
}: {
  selected: Geography;
  onChange: (selected: Geography) => void;
}) {
  return (
    <div className={"flex flex-row"}>
      {ALL_GEOGRAPHIES.map((geography) => (
        <button
          key={geography}
          className={
            "bg-slate-100 px-2 mr-2 py-1 rounded hover:text-slate-800 " +
            (selected === geography ? "text-slate-800" : "text-slate-300")
          }
          onClick={() => onChange(geography)}
        >
          {geography}
        </button>
      ))}
    </div>
  );
}
