import { Client } from "pg";
import { execTest } from "./test-util";
import { Value } from "./types";

const eq = (val1: Value, val2: Value): boolean => {
  if (val1 === null) return false;
  if (val2 === null) return false;

  return val1 === val2;
};

const innerJoin = (t1: [Value][], t2: [Value][]): [Value, Value][] => {
  let res: [Value, Value][] = [];

  for (const [val1] of t1) {
    for (const [val2] of t2) {
      if (eq(val1, val2)) {
        res.push([val1, val2]);
      }
    }
  }

  return res;
};

const leftOuterJoin = (t1: [Value][], t2: [Value][]): [Value, Value][] => {
  let res: [Value, Value][] = [];

  for (const [val1] of t1) {
    let selected = false;
    for (const [val2] of t2) {
      if (eq(val1, val2)) {
        res.push([val1, val2]);
        selected = true;
      }
    }

    if (!selected) res.push([val1, null]);
  }

  return res;
};

const fullOuterJoin = (t1: [Value][], t2: [Value][]): [Value, Value][] => {
  let res: [Value, Value][] = [];

  let selectedT1Val = new Set<number>();
  let selectedT2Val = new Set<number>();
  for (const [idx1, [val1]] of t1.entries()) {
    for (const [idx2, [val2]] of t2.entries()) {
      if (eq(val1, val2)) {
        res.push([val1, val2]);
        selectedT1Val.add(idx1);
        selectedT2Val.add(idx2);
      }
    }
  }
  for (const [idx1, [val1]] of t1.entries())
    if (!selectedT1Val.has(idx1)) res.push([val1, null]);
  for (const [idx2, [val2]] of t2.entries())
    if (!selectedT2Val.has(idx2)) res.push([null, val2]);

  return res;
};

// naive
const client = new Client({
  user: "postgres",
  password: "postgres",
  host: "localhost",
  port: 5433,
  database: "postgres",
});

type PGResult = { command: string; rows: { t1_val: Value; t2_val: Value }[] }[];
const extractQueryResult = (pgResult: PGResult): [Value, Value][] => {
  const selectResult = (pgResult as unknown as PGResult).find(
    (x) => x.command === "SELECT"
  );
  if (selectResult === undefined) {
    console.error("query failed", pgResult);
    process.exit(-1);
  }

  return selectResult.rows.map((row) => [row.t1_val, row.t2_val]);
};

const resetTable = (tableName: string, table: [Value][]): string => {
  const serializedVals = table.map(([val]) => `(${val})`).join(",");
  return (
    `truncate table ${tableName};` +
    (table.length !== 0
      ? `insert into ${tableName} (val) values ${serializedVals};`
      : "")
  );
};

const innerJoinNaive = async (
  t1: [Value][],
  t2: [Value][]
): Promise<[Value, Value][]> => {
  const res = await client.query(`
    ${resetTable("t1", t1)}
    ${resetTable("t2", t2)}

    select
      t1.val as t1_val,
      t2.val as t2_val
    from
      t1
      inner join t2 on t1.val = t2.val;
  `);

  return extractQueryResult(res as unknown as PGResult);
};

const leftOuterJoinNaive = async (
  t1: [Value][],
  t2: [Value][]
): Promise<[Value, Value][]> => {
  const res = await client.query(`
    ${resetTable("t1", t1)}
    ${resetTable("t2", t2)}

    select
      t1.val as t1_val,
      t2.val as t2_val
    from
      t1
      left outer join t2 on t1.val = t2.val;
  `);

  return extractQueryResult(res as unknown as PGResult);
};

const fullOuterJoinNaive = async (
  t1: [Value][],
  t2: [Value][]
): Promise<[Value, Value][]> => {
  const res = await client.query(`
    ${resetTable("t1", t1)}
    ${resetTable("t2", t2)}

    select
      t1.val as t1_val,
      t2.val as t2_val
    from
      t1
      full outer join t2 on t1.val = t2.val;
  `);

  return extractQueryResult(res as unknown as PGResult);
};

const main = async () => {
  await client.connect();

  await client.query("create table if not exists t1 (val integer);");
  await client.query("create table if not exists t2 (val integer);");

  const times = 1000;

  console.log("test: inner join");
  await execTest(times, innerJoin, innerJoinNaive);

  console.log("test: left outer join");
  await execTest(times, leftOuterJoin, leftOuterJoinNaive);

  console.log("test: full outer join");
  await execTest(times, fullOuterJoin, fullOuterJoinNaive);

  process.exit();
};
main();
