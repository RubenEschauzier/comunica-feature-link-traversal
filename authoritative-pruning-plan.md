# Authoritative pruning: what's wrong, and the plan

Working note for the composite-resource (CR) pruning in
`actor-derived-resource-select-*` and `actor-rdf-join-inner-multi-stems`.

---

## 1. The one rule everything follows from

For a block of patterns `B` delegated to a CR:

- The **anchors** of `B` are the **subjects** of its patterns.
- The CR can answer `B` only where it is authoritative for *every* anchor.
- A default operator may **prune** a mapping only if that mapping **binds every anchor
  of `B`** and they are all in the CR's space.

Two separate things, and the current code conflates them. Delegation is decided over the
whole block (correct today). Pruning is decided per pattern, using that pattern's own
subject and object (wrong today).

---

## 2. Assumptions that don't hold

### 2.3 "The last pattern can be filtered on its subject"

This is the `[patterns[n-1].subject]` entry. It is wrong even at `n = 2`.

A `tp_2` tuple with `?o_1` in the pod occurs both in the delegated part *and* in the case
where a triple from **outside** the pod connects into `?o_1` (`<bob@B> p1 <alice@A>`).
The CR only produces paths that start inside the pod. Pruning that tuple silently drops
every result where the path enters the pod from outside.

**This is a live incompleteness bug on the current branch.**

### 2.4 "Deduplication catches whatever pruning misses"

`isCoveredByProducedBaseTuples` (in `comunica-adaptive-derived-resources`,
`packages/actor-rdf-join-inner-multi-stems/lib/StemsOperatorStream.ts`)
is a **point-in-time** probe: when a CR result arrives, it checks whether the base
operators have *already* produced the matching parts. Its own comment says it is safe to
emit otherwise "because the authoritative source filter stops it from reading that tuple
later".

That justification is borrowed from pruning. Remove the filter — which correctness
demands for `n >= 3` — and the base operators go on to read those tuples from the pod's
documents and produce the same result again. Duplicates, and not rarely: the CR is the
fast path, so it usually wins the race.

So pruning is not an optimisation you can skip on long chains. Something must evaluate
the test at *some* depth or the decomposition stops being a partition.


### 2.6 (Mine) "The source check in the filter is redundant"

Wrong as stated, and your `/posts/*` question is exactly why. See §3.

---

## 3. The scope problem

Three different spaces are currently one string:

| what | where it comes from | what it means |
|---|---|---|
| `baseUrl` | `derivedResource.split(".meta")[0]` | where the resource was *announced* |
| `selectors` | the resource description | which *documents* it aggregated |
| `S` | **nothing** | the *subject space* it is complete for |

The anchor test needs `S`. The code passes `baseUrl`.

Your example: a pod authoritative over `https://a.example/`, with a CR aggregating only
`/posts/*`. Then `baseUrl = https://a.example/` but `S = https://a.example/posts/`. A
mapping with anchors inside the pod but outside `/posts/` passes the filter, gets pruned,
and is never emitted by the CR either — **lost**. Any pod whose CR is narrower than its
authority hits this.

The existing source-document check doesn't rescue it, because it uses the same
`baseUrl` prefix on both sides: it asks "did this come from anywhere in the pod", not
"from within `/posts/`".

**Fix:** derive the prefixes from `selectors`, not `baseUrl`. A block gets a *set* of
prefixes and the test is "in any of them" — correct, because the CR aggregates their
union as one dataset.

This rests on a convention worth stating: *a subject whose IRI falls under a path prefix
is hosted by a document under that prefix.* That is not a new assumption. It is exactly
the one authoritativeness already makes; narrowing the space doesn't change the kind of
claim, only its extent.

Where it doesn't apply:

- **Selectors that aren't prefix-expressible** (`*.ttl`, an explicit file list). Then `S`
  isn't a subject space at all. Register the block with pruning **disabled** rather than
  approximating, or carry provenance forward (see §5, trap 2 fallback).
- **Blank-node anchors.** The prefix test fails on them, so they never prune —
  conservative in the safe direction (duplicates, not loss), but the write-up currently
  claims they're covered. They aren't.

**Note for the paper:** the definition of `t ∈ CR` as "A is authoritative for triples with
subject t, realised by testing whether t falls under A's domain" is this same conflation.
It should be membership in the resource's declared completeness space, with authority over
that space as a consequence.

---

## 4. The plan, in one idea

Today authoritativeness is **one filter object per operator**, built from **per-pattern**
term lists that each select actor assembles by hand. That's why the shape reasoning was
duplicated across actors and got it wrong in both.

Replace with **one predicate per delegated block, evaluated wherever a mapping's coverage
is known**:

```ts
interface IDelegatedBlock {
  blockMask: number;           // bits of the operations in B
  ownerOperatorIndex: number;  // the CR operator answering B
  anchorVars: string[];        // residual anchors, constants discharged at registration
  domains: IParsedUri[];       // pre-parsed prefixes from selectors
}

shouldPrune(item, doneMask, crMask) =
  blocks.some(b =>
    overlaps(doneMask, b.blockMask) &&                        // mapping touches B
    notVia(crMask, b.ownerOperatorIndex) &&                   // and did not come from the CR
    b.anchorVars.every(v => inAnyDomain(item.get(v), b.domains)))
```

The shape of `B` never appears. Star, 2-chain, `n`-chain, and any future block actor all
reduce to "the subjects of my patterns". The depth at which pruning first fires falls out
of which mappings happen to bind all anchors — no per-shape special cases.

### Two evaluation sites, same predicate

| site | where | what it buys |
|---|---|---|
| ingestion | `StemsOperatorStream.read`, the `joinVars === undefined` branch, before hashing into `tripleMap` | input reduction — the delegated part never enters join state. Fires for stars and `tp_1` of a 2-chain |
| routing | `StemsControllerStream.read`, after reading the metadata, before routing | output reduction at any depth, and the guarantee that makes the point-in-time dedup sound |

Both are needed. Site 1 is where the real saving is for the easy shapes; site 2 is where
long chains and correctness live.

---

## 5. Three traps found while reading the code

**1. The CR's own results would be pruned.** The composite operator's `doneBitMask` *is*
`blockMask`, and its bindings satisfy every anchor by construction — site 2 would delete
exactly the delegated results. `done` can't distinguish "produced via the CR" from
"produced by base operators"; the bits are identical. `lastCrIndex` only records the most
recent one. Add `crMask: number` to `IStemsBindingsMetadata`, merged alongside `done` on
the same two lines that already maintain `lastCrIndex`.

**2. The source-keyed lookup must go.** `shouldFilter` resolves rules by the binding's
source document and **throws** unless there is exactly one source. Intermediates merge
sources from several documents, so site 2 would throw on the first join result. Drop the
source lookup; keep the prefix test. This is only sound once the domain is `S` (§3) — with
`baseUrl` it isn't redundant, just also wrong. *Fallback if selectors aren't
prefix-expressible:* set an in-scope bit per tuple at ingestion, where a tuple has exactly
one source, and merge it through joins like `done`.

**3. Overlap, not subset.** The lemma is stated for `cov(o) ⊆ B`, but the safe rule is
weaker: any mapping overlapping `B` and satisfying all its anchors can go, because its
completion is reachable through `CR ⋈ (rest of its coverage) ⋈ ...`. Subset alone leaves a
duplicate hole for mixed-coverage intermediates, which is the common case at `n >= 3`.
Tests must confirm the CR route is actually reachable for mixed coverage —
`forbiddenBaseMask` and the ascending-CR rule constrain routing.

---

## 6. Performance

Better than today, not worse.

**Ingestion.** The mapping's coverage is the operator's own `doneBitMask` — invariant. So
both guards are decidable **at registration**, not per tuple: an operator caches the
(usually empty) list of blocks that can ever prune it. Consequences:

- an `n >= 3` base operator gets an empty list and pays one length check per tuple, where
  today it runs a full filter test twice per tuple that provably can never fire
- a star or 2-chain `tp_1` operator gets a one-element list with the identical variable
  set it uses today, minus the source lookup

**Routing.** Boundness is a function of coverage, not of the mapping, so "are all anchors
bound" is a table indexed by `done` — the same key `routingTable` already uses. Per item
that's one array index; only genuine pruning candidates reach a domain test. Nice side
effect: the minimum popcount over that table's non-empty entries *is* the pruning depth,
readable at runtime instead of derived per shape.

**The actual hot-path cost, in both designs**, is `matchesPrefix`: it calls `parseUri`
twice per test — once on the term, once on the *fixed* domain — and each call does several
string slices, a `toLowerCase`, and an object literal. Roughly a dozen allocations per
anchor per tuple, half of them re-deriving a constant. Pre-parse the prefix at
registration (halves it, no behaviour change) and burst-cache the term test (traversal
emits many tuples per document with repeating subjects).

**Easy cases are a pure refactor.** Star anchors are `{?x}`; 2-chain `tp_1` anchors are
`{?s, ?o_1}` — exactly the lists the actors build today, applied at the same place. Assert
it: identical result multisets and identical prune counts, before and after.

---

## 7. Steps

1. **Scope helper** in `bus-derived-resource-select`: `scopeOf(resource) -> prefixes[]`
   from `selectors`, falling back to `baseUrl` only when a selector covers the root;
   pruning disabled when not prefix-expressible.
2. **Anchor helper** in the same package: `anchorsOf(patterns)` = deduplicated subjects.
   Both select actors call it; neither reasons about shape.
3. **Rewrite the filter** as a block registry with `registerBlock` /
   `shouldPrune(item, doneMask, crMask)`. Constants discharged at registration; a constant
   anchor outside the scope means the block isn't delegable and the registration is
   *rejected and reported*, not silently dropped.
4. **Move ownership to the controller** — one registry, not one filter per operator. Kills
   both `new AuthoritativeSourceFilter` sites and the `operationToOperatorIndex` fan-out
   loop.
5. **Wire both sites**, with `crMask`.
6. **Actors**: replace `patternToExtractor` with `anchors` + `domains`. Both actors become
   the same two lines. Fix the constant-subject guard and the CR-side stream filter, which
   have the same `baseUrl` bug.
7. **Re-justify the dedup**: `isCoveredByProducedBaseTuples` stays, but its remaining job
   is only the pre-registration window of dynamic discovery. Say so in the comment.
8. **Instrumentation**: count prunes per block by depth. Measures the "one join avoided"
   claim directly, which the evaluation section wants anyway.

Steps 1-3 are behaviour-preserving refactors. 4-5 change results. 6 is where the current
incompleteness is actually fixed. The metadata contract spans both repos, so they land
together.

---

## 8. Tests to write first

- **2-chain, path enters the pod from outside** (`<bob@B> p1 <alice@A>`) — the result the
  current code loses. `tp_1` must prune, `tp_2` must not.
- **3-chain, path leaves the pod at `?o_2`** — no ingestion pruning anywhere; the
  `tp_1 ⋈ tp_2` prefix prunes at site 2.
- **3-chain fully inside the pod** — exactly one copy of each result, from the CR.
- **Constant entry point at `n = 3`** — `tp_2` prunes at ingestion, `tp_1` does not. This
  is the case that falsifies "the first pattern is always prunable".
- **Narrow CR** — pod authoritative over `https://a.example/`, CR over `/posts/*`, query
  touching `/profile/`. The `/profile/` results must come back.
- **Late registration** — CR registered after base operators have emitted overlapping
  tuples. No duplicates, no losses.
- **Two CRs over overlapping blocks** — `crMask` and the ascending-CR rule interact;
  most likely place for a surprise.
