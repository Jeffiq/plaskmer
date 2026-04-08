from Bio import SeqIO
from itertools import product
import pandas as pd
import sys
import os                                 # NEW: For getting the secret token
from huggingface_hub import HfApi         # NEW: For uploading to cloud

def get_all_kmers(k):
    return [''.join(p) for p in product('ATGC', repeat=k)]

def count_kmers(seq, k):
    seq = str(seq).upper()
    counts = {}
    for i in range(len(seq) - k + 1):
        kmer = seq[i:i+k]
        if all(b in 'ATGC' for b in kmer):
            counts[kmer] = counts.get(kmer, 0) + 1
    return counts

def fasta_to_kmer_df(fasta_file, k=6):
    all_kmers = get_all_kmers(k)
    records = []

    for record in SeqIO.parse(fasta_file, "fasta"):
        counts = count_kmers(record.seq, k)
        total = sum(counts.values())
        row = {'sequence_id': record.id}
        for kmer in all_kmers:
            row[kmer] = counts.get(kmer, 0) / total if total > 0 else 0
        records.append(row)

    df = pd.DataFrame(records).set_index('sequence_id')
    return df

if __name__ == "__main__":
    fasta_file = sys.argv[1] if len(sys.argv) > 1 else "sequences.fasta"
    k = int(sys.argv[2]) if len(sys.argv) > 2 else 6
# --- NEW: Cap K at 6 to prevent memory explosion ---
    if k > 6:
        print(f"⚠️ Warning: Requested k={k} is too large. Capping at k=6 to protect server memory.")
        k = 6

    
    print(f"Processing {fasta_file} with k={k}...")
    df = fasta_to_kmer_df(fasta_file, k)
    out = f"kmer_{k}mer_vectors.csv"
    df.to_csv(out)
    print(f"Done. Shape: {df.shape}")
    print(f"Saved to: {out}")
    print(df.iloc[:, :5].head())

# --- NEW: Push directly to Hugging Face Cloud ---
    print(f"\n🚀 Uploading {out} to Hugging Face...")
    try:
        api = HfApi()
        api.upload_file(
            path_or_fileobj=out,
            path_in_repo=out,
            repo_id="Jeffiq/Plaskmer", # *** UPDATE THIS TO YOUR EXACT HF USERNAME ***
            repo_type="dataset",
            token=os.environ.get("HF_TOKEN")
        )
        print("✅ Successfully pushed CSV to Hugging Face!")
    except Exception as e:
        print(f"❌ Hugging Face upload failed: {e}")
    # ------------------------------------------------
