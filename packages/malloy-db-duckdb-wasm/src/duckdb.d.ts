declare module "duckdb" {
  /**
   * Scalar result value type
   */
  type Scalar = boolean | number | string | null;

  /**
   * All result value types
   */
  type Value = Scalar | Record<string, Value> | Array<Value> | null;

  /**
   * Result row
   */
  type Row = Record<string, Value>;
}
