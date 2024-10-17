import { Client } from "pg";
import { execTest } from "./test-util";
import { Id, Ids, QueryResult } from "./types";

const eq = (id1: Id, id2: Id): boolean => {
  if (id1 === null) return false;
  if (id2 === null) return false;

  return id1 === id2;
};

const innerJoin = (t1: Ids, t2: Ids): QueryResult => {
  let res: QueryResult = [];

  for (const id1 of t1) {
    for (const id2 of t2) {
      if (eq(id1, id2)) {
        res.push({ t1_id: id1, t2_id: id2 });
      }
    }
  }

  return res;
};

const leftOuterJoin = (t1: Ids, t2: Ids): QueryResult => {
  let res: QueryResult = [];

  for (const id1 of t1) {
    let selected = false;
    for (const id2 of t2) {
      if (eq(id1, id2)) {
        res.push({ t1_id: id1, t2_id: id2 });
        selected = true;
      }
    }

    if (!selected) res.push({ t1_id: id1, t2_id: null });
  }

  return res;
};

const fullOuterJoin = (t1: Ids, t2: Ids): QueryResult => {
  let res: QueryResult = [];

  let t1Selected = new Map<number, boolean>();
  let t2Selected = new Map<number, boolean>();
  for (const [idx1, id1] of t1.entries()) {
    for (const [idx2, id2] of t2.entries()) {
      if (eq(id1, id2)) {
        res.push({ t1_id: id1, t2_id: id2 });
        t1Selected.set(idx1, true);
        t2Selected.set(idx2, true);
      }
    }
  }
  for (const [idx1, id1] of t1.entries())
    if (t1Selected.get(idx1) === undefined)
      res.push({ t1_id: id1, t2_id: null });
  for (const [idx2, id2] of t2.entries())
    if (t2Selected.get(idx2) === undefined)
      res.push({ t1_id: null, t2_id: id2 });

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

type PGResult = { command: string; rows: QueryResult }[];
const extractQueryResult = (pgResult: PGResult): QueryResult => {
  const selectResult = (pgResult as unknown as PGResult).find(
    (x) => x.command === "SELECT"
  );
  if (selectResult === undefined) {
    console.error("query failed", pgResult);
    process.exit(-1);
  }

  return selectResult.rows;
};

const resetTable = (table: string, ids: Ids): string => {
  const serializedIds = ids.map((row) => `(${row})`).join(",");
  return (
    `truncate table ${table};` +
    (ids.length !== 0
      ? `insert into ${table} (id) values ${serializedIds};`
      : "")
  );
};

const innerJoinNaive = async (t1: Ids, t2: Ids): Promise<QueryResult> => {
  const res = await client.query(`
    ${resetTable("t1", t1)}
    ${resetTable("t2", t2)}

    select
      t1.id as t1_id,
      t2.id as t2_id
    from
      t1
      inner join t2 on t1.id = t2.id;
  `);

  return extractQueryResult(res as unknown as PGResult);
};

const leftOuterJoinNaive = async (t1: Ids, t2: Ids): Promise<QueryResult> => {
  const res = await client.query(`
    ${resetTable("t1", t1)}
    ${resetTable("t2", t2)}

    select
      t1.id as t1_id,
      t2.id as t2_id
    from
      t1
      left outer join t2 on t1.id = t2.id;
  `);

  return extractQueryResult(res as unknown as PGResult);
};

const fullOuterJoinNaive = async (t1: Ids, t2: Ids): Promise<QueryResult> => {
  const res = await client.query(`
    ${resetTable("t1", t1)}
    ${resetTable("t2", t2)}

    select
      t1.id as t1_id,
      t2.id as t2_id
    from
      t1
      full outer join t2 on t1.id = t2.id;
  `);

  return extractQueryResult(res as unknown as PGResult);
};

const main = async () => {
  await client.connect();

  await client.query("create table if not exists t1 (id integer);");
  await client.query("create table if not exists t2 (id integer);");

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
