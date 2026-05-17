import pandas as pd

df = pd.read_csv("/Users/marcelgleich/Downloads/koenig-studios-by-bw-raw.csv", sep=";")

# --- Basic stats ---
print("=== Basic Statistics ===")
print(df[["review_count", "rating"]].describe())
print(f"\nTotal businesses: {len(df)}")

# --- Percentiles for review_count ---
print("\n=== Review Count Percentiles ===")
for p in [25, 50, 60, 70, 75, 80, 90, 95]:
    val = df["review_count"].quantile(p / 100)
    kept = (df["review_count"] >= val).sum()
    print(f"  P{p}: {val:.0f} reviews  →  {kept} kept ({kept/len(df)*100:.1f}%)")

# --- Cutoff grid ---
print("\n=== Cutoff Combinations ===")
review_cutoffs = df["review_count"].quantile([0.5, 0.6, 0.7, 0.75, 0.8]).values
rating_cutoffs = [3.0, 3.5, 4.0, 4.5]

print(f"{'reviews >=':<15} {'rating >=':<12} {'kept':<8} {'%':<8}")
print("-" * 43)
for rc in review_cutoffs:
    for rt in rating_cutoffs:
        mask = (df["review_count"] >= rc) & (df["rating"] >= rt)
        n = mask.sum()
        print(f"{rc:<15.0f} {rt:<12.1f} {n:<8} {n/len(df)*100:<8.1f}")