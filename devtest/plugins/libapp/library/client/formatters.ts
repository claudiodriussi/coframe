/**
 * Formatters for the library plugin.
 * Exported as default Record<string, CellFormatter> — auto-discovered by loader.ts.
 */

// Price range background colours (Tailwind palette):
//   < 10   → green-200  (budget)
//   10–20  → yellow-200 (mid-range)
//   ≥ 20   → red-200    (premium)
const priceColor = (cell: any) => {
  const val = cell.getValue() as number | null;
  const el  = cell.getElement() as HTMLElement;
  if (val == null) return '';
  if      (val < 10)  el.style.backgroundColor = '#bbf7d0';
  else if (val < 20)  el.style.backgroundColor = '#fef08a';
  else                el.style.backgroundColor = '#fecaca';
  return String(val);
};

export default { price_color: priceColor };
