import time
import os
import re
import csv
import threading
from queue import Queue
from Bio import Entrez
import config
import constants
from huggingface_hub import HfApi

Entrez.email = config.NCBI_EMAIL

class ParallelOmniSystem:
    # --- CHANGED: Now accepts Streamlit UI inputs as arguments ---
    def __init__(self, target_org, selected_dbs, types, target_goal, push_to_hf=False):
        print("🚀 Initializing Open Source Parallel Pipeline via Streamlit...")
        
        self.target_org = target_org
        self.selected_dbs = selected_dbs
        self.types = types
        self.target_goal = target_goal
        self.push_to_hf = push_to_hf
        self.existing_accs = set()

        # 1. Initialize Local Backup File & Load Existing Accessions
        file_exists = os.path.exists(config.LOCAL_BACKUP)

        if file_exists:
            print(f"📊 Loading existing accessions from {config.LOCAL_BACKUP}...")
            with open(config.LOCAL_BACKUP, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if len(row) > 1:
                        self.existing_accs.add(row[1]) 
        else:
            print(f"🆕 Creating new master database: {config.LOCAL_BACKUP}")
            with open(config.LOCAL_BACKUP, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Unique_ID", "Accession", "Organism", "Type", "Length", "File_Reference", "Source_URL", "Country"])

        # Queues for Parallel Processing
        self.search_to_harvest_q = Queue(maxsize=100)
        self.harvest_to_fasta_q = Queue(maxsize=100)

    def _save_local(self, row):
        """Writes to local CSV immediately after every harvest."""
        with open(config.LOCAL_BACKUP, "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)

    def producer_search(self):
        """Thread 1: Searches NCBI and finds candidates based on UI selections."""
        print(f"\n==============================\nSEARCH PARAMETERS: {self.target_org}\n==============================")

        if not self.selected_dbs:
            print("⚠️ No databases selected. Exiting.")
            self.search_to_harvest_q.put(None)
            return

        # Build the query string from Streamlit inputs
        type_query = " OR ".join([f'"{t}"' for t in self.types]) if self.types else ""

        for country in constants.AFRICA:
            print(f"🔎 [Searcher] Scanning {country}...")
            for db in self.selected_dbs:
                query = f'("{self.target_org}"[Organism]) AND "{country}"'
                if type_query: query += f' AND ({type_query})'

                try:
                    search_handle = Entrez.esearch(db=db, term=query, retmax=100)
                    ids = Entrez.read(search_handle)["IdList"]

                    found_for_country = 0
                    for acc in ids:
                        if acc in self.existing_accs: continue
                        if found_for_country >= self.target_goal: break

                        time.sleep(0.3)
                        v_handle = Entrez.efetch(db=db, id=acc, rettype="gb" if db == "nucleotide" else "xml", retmode="text")
                        raw_data = v_handle.read()

                        if isinstance(raw_data, bytes):
                            raw_data = raw_data.decode('utf-8', errors='ignore')

                        raw_data = raw_data.lower()

                        if country.lower() in raw_data and not re.search(r"travel|imported|returned from", raw_data):
                            data = {
                                "uid": f"{country[:2].upper()}_{acc}",
                                "acc": acc,
                                "org": self.target_org,
                                "country": country.capitalize(),
                                "db": db
                            }
                            self.search_to_harvest_q.put(data)
                            found_for_country += 1
                            self.existing_accs.add(acc)
                            print(f"✅ [Searcher] Queued {acc} ({db}) in {country}")

                except Exception as e:
                    print(f"❌ [Searcher] Error in {db} for {country}: {e}")

        self.search_to_harvest_q.put(None)

# ... (Keep your worker_harvest, consumer_fasta_writer, and run_all_parallel exactly as they are) ...
