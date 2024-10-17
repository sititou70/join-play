import { Ids, QueryResult } from "./types";

const generateTestIds = (): Ids => {
  const values = [1, 2, null];
  const length = Math.floor(Math.random() * 5);

  return [...Array(length).keys()].map(
    () => values[Math.floor(Math.random() * values.length)]
  );
};

const isEqQueryResult = (qr1: QueryResult, qr2: QueryResult): boolean => {
  if (qr1.length !== qr2.length) return false;

  const increment = (map: Map<string, number>, key: string) => {
    const value = map.get(key);
    if (value === undefined) {
      map.set(key, 1);
      return;
    }

    map.set(key, value + 1);
  };

  const qr1Count = new Map<string, number>();
  for (const row of qr1) increment(qr1Count, `${row.t1_id},${row.t2_id}`);

  const qr2Count = new Map<string, number>();
  for (const row of qr1) increment(qr2Count, `${row.t1_id},${row.t2_id}`);

  for (const [key, count] of qr1Count) {
    if (count !== qr2Count.get(key)) return false;
  }

  for (const [key, count] of qr2Count) {
    if (count !== qr1Count.get(key)) return false;
  }

  return true;
};

export const execTest = async (
  times: number,
  fn: (t1: Ids, t2: Ids) => QueryResult,
  fnNaive: (t1: Ids, t2: Ids) => Promise<QueryResult>
) => {
  for (const _ of [...Array(times).keys()]) {
    const t1 = generateTestIds();
    const t2 = generateTestIds();

    const qr = fn(t1, t2);
    const qrNaive = await fnNaive(t1, t2);

    if (!isEqQueryResult(qr, qrNaive)) {
      console.error("failed...", { t1, t2, qr, qrNaive });
      return;
    }
  }
};
