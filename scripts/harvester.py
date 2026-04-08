import time
import os
import re
import csv
import threading
from queue import Queue
from Bio import Entrez
from huggingface_hub import HfApi
import config
import constants

# Ensure your NCBI email is set in your config.py
Entrez.email = config.NCBI_EMAIL

class ParallelOmniSystem:
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
                        # Fetch summary to verify country/metadata
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

        # Send poison pill to close workers
        self.search_to_harvest_q.put(None)

    def worker_harvest(self):
        """Thread 2: Consumes queued accessions, fetches FASTA sequences, and logs data."""
        while True:
            item = self.search_to_harvest_q.get()
            
            # Check for poison pill
            if item is None:
                self.search_to_harvest_q.put(None) # Pass it along to other workers
                break

            acc = item['acc']
            db = item['db']
            
            try:
                # Fetch actual FASTA sequence
                time.sleep(0.4) # Respect NCBI rate limits
                handle = Entrez.efetch(db="nucleotide" if db != "sra" else db, id=acc, rettype="fasta", retmode="text")
                fasta_data = handle.read().strip()
                handle.close()
                
                if fasta_data:
                    # Calculate sequence length
                    lines = fasta_data.split('\n')
                    header = lines[0] if len(lines) > 0 else ""
                    sequence = "".join(lines[1:])
                    seq_length = len(sequence)
                    
                    # Guess Type from Header
                    seq_type = "WGS"
                    if "plasmid" in header.lower():
                        seq_type = "Plasmid"
                    elif "gene" in header.lower() or "mrna" in header.lower():
                        seq_type = "mRNA"
                        
                    # Queue FASTA data to writer
                    self.harvest_to_fasta_q.put(fasta_data)
                    
                    # Log to CSV immediately
                    csv_row = [
                        item['uid'], 
                        acc, 
                        item['org'], 
                        seq_type, 
                        seq_length, 
                        "plas_kmer_sequences.fasta", 
                        f"https://www.ncbi.nlm.nih.gov/nuccore/{acc}", 
                        item['country']
                    ]
                    self._save_local(csv_row)
                    print(f"🧬 [Harvester] Downloaded sequence for {acc} ({seq_length} bp)")
                    
            except Exception as e:
                print(f"❌ [Harvester] Failed to fetch sequence for {acc}: {e}")
                
            self.search_to_harvest_q.task_done()

    def consumer_fasta_writer(self):
        """Thread 3: Listens for sequence text and safely writes to the unified FASTA file."""
        fasta_path = os.path.join(os.path.dirname(config.LOCAL_BACKUP), "plas_kmer_sequences.fasta")
        
        while True:
            fasta_data = self.harvest_to_fasta_q.get()
            if fasta_data is None:
                break
                
            try:
                with open(fasta_path, "a", encoding="utf-8") as f:
                    f.write(fasta_data + "\n\n")
            except Exception as e:
                print(f"❌ [Writer] Error writing to FASTA file: {e}")
                
            self.harvest_to_fasta_q.task_done()

        # Push to Hugging Face only if toggled ON in Streamlit
        if self.push_to_hf:
            self._upload_to_huggingface(fasta_path)

    def _upload_to_huggingface(self, fasta_path):
        """Uploads the updated CSV and FASTA to Hugging Face (GitHub Mode)."""
        print("☁️ [Cloud] Initiating Hugging Face Upload...")
        try:
            api = HfApi()
            repo_id = getattr(config, 'HF_REPO_ID', None)
            
            if not repo_id:
                print("⚠️ [Cloud] HF_REPO_ID not found in config.py. Skipping upload.")
                return

            if os.path.exists(config.LOCAL_BACKUP):
                api.upload_file(
                    path_or_fileobj=config.LOCAL_BACKUP,
                    path_in_repo="data/local_harvest_log.csv",
                    repo_id=repo_id,
                    repo_type="dataset"
                )
                
            if os.path.exists(fasta_path):
                api.upload_file(
                    path_or_fileobj=fasta_path,
                    path_in_repo="data/plas_kmer_sequences.fasta",
                    repo_id=repo_id,
                    repo_type="dataset"
                )
            print("✅ [Cloud] Files successfully synced to Hugging Face!")
        except Exception as e:
            print(f"❌ [Cloud] Hugging Face sync failed: {e}")

    def run_all_parallel(self):
        """Orchestrates all threads."""
        print("🚦 Starting Parallel Harvest...")
        
        # 1. Start the Writer Thread
        t_writer = threading.Thread(target=self.consumer_fasta_writer)
        t_writer.start()
        
        # 2. Start Worker Threads
        num_workers = 3
        workers = []
        for _ in range(num_workers):
            t = threading.Thread(target=self.worker_harvest)
            t.start()
            workers.append(t)
            
        # 3. Run the Searcher on the main thread
        self.producer_search()
        
        # 4. Wait for all workers to finish processing queue
        for t in workers:
            t.join()
            
        # 5. Stop Writer
        self.harvest_to_fasta_q.put(None)
        t_writer.join()
        
        print("\n🎉 All parallel tasks completed successfully.")
