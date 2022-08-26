import { ReactNode } from "react";

export default function Label({ children }: { children: ReactNode }) {
  return (
    <div className={"bg-slate-100 px-2 py-1 rounded text-slate-800"}>
      {children}
    </div>
  );
}
