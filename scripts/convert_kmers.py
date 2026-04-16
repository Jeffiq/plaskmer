import pandas as pd
from pathlib import Path
import os
from huggingface_hub import HfApi, hf_hub_download
import config
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).parent.resolve()

def modernize_remote_kmers():
    hf_token = os.environ.get("HF_TOKEN")
    repo_id = getattr(config, 'HF_REPO_ID', "Jeffiq/Plaskmer")
    
    if not hf_token:
        print("❌ Error: No HF_TOKEN found in your .env file!")
        return

    api = HfApi(token=hf_token)
    
    print(f"🌍 Scanning Hugging Face Repo: {repo_id} for K-mer CSVs...")
    
    # 1. Get list of all files in the repo
    try:
        files = api.list_repo_files(repo_id=repo_id, repo_type="dataset")
    except Exception as e:
        print(f"❌ Failed to connect to HF: {e}")
        return

    # 2. Filter for any K-mer CSV (kmer_Xmer_vectors.csv)
    # This works regardless of where they are (root or data/ folder)
    csv_files = [f for f in files if f.endswith(".csv") and "kmer_" in f]
    
    if not csv_files:
        print("ℹ️ No K-mer CSVs found on the cloud to convert.")
    else:
        print(f"📂 Found {len(csv_files)} CSV files on cloud. Starting conversion...")

    for remote_csv in csv_files:
        filename = os.path.basename(remote_csv)
        pq_name = filename.replace(".csv", ".parquet")
        pq_path = SCRIPT_DIR / pq_name
        
        print(f"\n⏳ Processing {filename}...")
        
        try:
            # A. Download the CSV from cloud to temporary local storage
            print(f"   📥 Downloading {filename}...")
            local_csv_path = hf_hub_download(
                repo_id=repo_id, 
                filename=remote_csv, 
                repo_type="dataset", 
                token=hf_token
            )
            
            # B. Convert to Parquet
            print(f"   📦 Converting to Parquet...")
            df = pd.read_csv(local_csv_path, low_memory=False)
            
            # Standardize the ID column
            if 'sequence_id' in df.columns:
                df['sequence_id'] = df['sequence_id'].astype(str)
            
            df.to_parquet(pq_path, index=False, engine='pyarrow', compression='snappy')
            
            # C. Upload the new Parquet to the 'data/' folder on cloud
            print(f"   📤 Uploading data/{pq_name}...")
            api.upload_file(
                path_or_fileobj=str(pq_path),
                path_in_repo=f"data/{pq_name}",
                repo_id=repo_id,
                repo_type="dataset"
            )
            print(f"✅ Successfully modernized {filename} -> data/{pq_name}")
            
            # Clean up local parquet to save space
            os.remove(pq_path)
            
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")

    print("\n🚀 All existing cloud K-mer files have been converted to Parquet in the /data folder!")

if __name__ == "__main__":
    modernize_remote_kmers()
