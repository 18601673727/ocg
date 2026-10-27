import type { decodeUsageBuckets } from "../contracts";

type UsageRow = ReturnType<typeof decodeUsageBuckets>["rows"][number];
export type ModelKey = string | null;
export type UsageBar = { bucketUs: number; total: number; totals: Map<ModelKey, number> };
export type RankedUsage = {
  model: ModelKey;
  unknown: boolean;
  other: boolean;
  keys: ModelKey[];
  total: number;
  share: number;
};

export function summarize(
  rows: readonly UsageRow[],
  fromUs: number,
  untilUs: number,
  widthUs: number,
) {
  const bars: UsageBar[] = [];
  for (let bucketUs = fromUs; bucketUs < untilUs; bucketUs += widthUs) {
    bars.push({ bucketUs, total: 0, totals: new Map() });
  }
  const totals = new Map<ModelKey, number>();
  let total = 0;
  for (const row of rows) {
    if (row.bucket_us < fromUs || row.bucket_us >= untilUs) continue;
    const bar = bars[Math.floor((row.bucket_us - fromUs) / widthUs)];
    // Prompt tokens include cache reads; output tokens include reasoning.
    const tokens = row.input_tokens + row.output_tokens;
    bar.totals.set(row.model, (bar.totals.get(row.model) ?? 0) + tokens);
    bar.total += tokens;
    totals.set(row.model, (totals.get(row.model) ?? 0) + tokens);
    total += tokens;
  }
  const sorted = [...totals].sort(([a, aTotal], [b, bTotal]) => {
    if (aTotal !== bTotal) return bTotal - aTotal;
    if (a === b) return 0;
    if (a === null) return 1;
    if (b === null) return -1;
    return a < b ? -1 : 1;
  });
  const ranking: RankedUsage[] = sorted.slice(0, 9).map(([model, value]) => ({
    model,
    unknown: model === null,
    other: false,
    keys: [model],
    total: value,
    share: total ? value / total : 0,
  }));
  if (sorted.length > 9) {
    const rest = sorted.slice(9);
    const value = rest.reduce((sum, [, tokens]) => sum + tokens, 0);
    ranking.push({
      model: null,
      unknown: false,
      other: true,
      keys: rest.map(([key]) => key),
      total: value,
      share: total ? value / total : 0,
    });
  }
  return { bars, ranking, total };
}
