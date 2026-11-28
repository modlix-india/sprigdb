/** Runs loads so that only the newest one's outcome is applied: a load that
 * finishes after a newer one started (a reload after a save overtaken by a
 * new filter, say) is dropped, as is any load running when `cancel` is
 * called (the component went away). */
export function latestOnly() {
  let newest = 0;
  return {
    run<T>(load: () => Promise<T>, apply: (v: T) => void, fail: (e: unknown) => void): Promise<void> {
      const mine = ++newest;
      return load().then(
        (v) => {
          if (mine === newest) apply(v);
        },
        (e: unknown) => {
          if (mine === newest) fail(e);
        },
      );
    },
    cancel(): void {
      newest++;
    },
  };
}
