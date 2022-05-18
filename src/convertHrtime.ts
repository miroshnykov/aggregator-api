export const convertHrtime = (diff: bigint): number => {
  const milliseconds = Number(diff) / 1000000;
  return Number(milliseconds.toFixed(0));
};
