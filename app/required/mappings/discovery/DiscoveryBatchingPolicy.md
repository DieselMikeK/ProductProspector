Batching Policy For Search Family Discovery

- Batch size is an execution convenience, not a taxonomy boundary.
- A vendor batch may contain multiple subfamilies, and the same subfamily may appear across multiple batches.
- After each batch, compare the learned rules against the rest of the parent family before promoting new subgroup labels or vendor-specific overrides.
- Merge identical route shapes, interaction strategies, and search-term transforms across batches whenever runtime validation supports it.
- Only keep vendor-specific rules when cross-batch comparison shows the vendor is a true exception.
- When a batch produces a reusable rule, run at least a light validation pass against vendors outside the batch that appear to share the same pattern.
- Do not let the order of work create artificial family splits. Batch boundaries are for discovery throughput only.
