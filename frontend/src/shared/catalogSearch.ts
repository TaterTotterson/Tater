/** Match every query word against display metadata, without searching settings or credentials. */
export function catalogMatches(query: string, ...values: unknown[]): boolean {
  const normalize = (value: unknown) => String(value ?? "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
  const haystack = values.flat().map(normalize).join(" ");
  return normalize(query).trim().split(/\s+/).every((word) => haystack.includes(word));
}
