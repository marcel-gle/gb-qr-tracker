#probably one time script

import pandas as pd

# The 40 shared columns, in canonical order
SHARED = [
    "domain", "company_name", "match_score", "score_raw", "score_scale",
    "passed_score_filter", "salutation_1", "first_name_1", "last_name_1",
    "full_address", "street", "house_number", "postcode", "city", "email",
    "phone", "legal_name", "template", "source_file", "source_row",
    "gegenstand", "branchencode", "score_field", "linkedin_profile_url_1",
    "imprint_managing_director_1", "first_name_2", "last_name_2",
    "salutation_2", "linkedin_profile_url_2", "imprint_managing_director_2",
    "first_name_3", "last_name_3", "salutation_3", "linkedin_profile_url_3",
    "imprint_managing_director_3", "domain_analysis_raw", "analysis_result",
    "analysis_makler", "analysis_begruendung", "analysis_score",
    "analysis_veraltung_signale", "analysis_technical_signals_detected",
    "analysis_technical_score", "analysis_visual_age_bonus",
    "analysis_visuelle_signale",
    "analysis_llm_input_meta_classification_original_chars",
    "analysis_llm_input_meta_classification_sent_chars",
    "analysis_llm_input_meta_classification_lines_kept",
    "analysis_llm_input_meta_classification_truncated",
    "analysis_llm_input_meta_classification_call",
    "analysis_llm_input_meta_classification_max_chars_used",
    "analysis_llm_input_meta_visual_age_original_chars",
    "analysis_llm_input_meta_visual_age_sent_chars",
    "analysis_llm_input_meta_visual_age_lines_kept",
    "analysis_llm_input_meta_visual_age_truncated",
    "analysis_llm_input_meta_visual_age_call",
    "analysis_llm_input_meta_visual_age_max_chars_used",
    "analysis_llm_degraded",
    "analysis_llm_input_meta_visual_age_skipped",
    "analysis_llm_input_meta_visual_age_reason",
]

FILE1 = "/Users/marcelgleich/Desktop/Briefversand/009-20260604-koenig-makler/lists/final_listen_sammlung/009-20260604-koenig-makler_scored_ocan.csv"   # the Imprint/contact version
FILE2 = "/Users/marcelgleich/Desktop/Briefversand/009-20260604-koenig-makler/lists/final_listen_sammlung/009-20260604-koenig-makler_scored_nd2.csv"   # the North Data version

def load_and_strip(path):
    df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
    missing = [c for c in SHARED if c not in df.columns]
    if missing:
        raise SystemExit(f"{path} is missing columns: {missing}")
    return df[SHARED]

df1 = load_and_strip(FILE1)
df2 = load_and_strip(FILE2)

df1.to_csv("file1_stripped.csv", sep=";", index=False)
df2.to_csv("file2_stripped.csv", sep=";", index=False)

# merged output (stacked, dedup on domain keeping first)
combined = pd.concat([df1, df2], ignore_index=True)
before = len(combined)
merged = combined.drop_duplicates(subset="domain", keep="first")
after = len(merged)
removed = before - after
merged.to_csv("merged.csv", sep=";", index=False)

print(f"file1_stripped.csv: {len(df1)} rows, {len(df1.columns)} cols")
print(f"file2_stripped.csv: {len(df2)} rows, {len(df2.columns)} cols")
print(f"combined (before dedup): {before} rows")
print(f"duplicates removed:      {removed}")
print(f"merged.csv (remaining):  {after} rows, {len(merged.columns)} cols")

