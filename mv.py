import os
import time
import re
from botasaurus.browser import browser, Driver
from botasaurus.soupify import soupify
from supabase import create_client, Client

# --- KONFIGURASI SUPABASE ---
# Mengambil URL dan KEY dari GitHub Secrets / Environment Variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[ERROR] SUPABASE_URL atau SUPABASE_KEY tidak ditemukan di environment!")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@browser(
    headless=True,            # WAJIB True untuk GitHub Actions
    reuse_driver=True,
    block_images=True,        # Mempercepat loading
    window_size=(1280, 720)
)
def run_latest_scraper_supabase(driver: Driver, data=None):
    # Format link sesuai permintaan: https://tv10.lk21official.cc/latest/page/1
    base_url_pattern = "https://tv10.lk21official.cc/latest/page/"
    END_PAGE = 10  
    
    print(f"[*] MEMULAI SCRAPING - TARGET: {END_PAGE} HALAMAN")

    for page in range(1, END_PAGE + 1):
        target_url = f"{base_url_pattern}{page}"
        print(f"\n--- SCANNING HALAMAN {page}: {target_url} ---")
        
        try:
            driver.get(target_url)
            # Waktu tunggu agar Cloudflare selesai melakukan pengecekan browser
            time.sleep(10) 
            
            page_soup = soupify(driver)
            articles = page_soup.select('article')
            
            movie_list = []
            for art in articles:
                a_tag = art.select_one('figure a')
                year_tag = art.select_one('span.year')
                
                if a_tag and year_tag:
                    href = a_tag.get('href')
                    # Pastikan URL lengkap
                    full_url = href if href.startswith('http') else "https://tv10.lk21official.cc" + href
                    
                    # Filter: Pastikan ini link film, bukan link kategori/tahun
                    if '/year/' not in full_url and '/genre/' not in full_url:
                        slug = full_url.strip('/').split('/')[-1]
                        year_text = year_tag.get_text(strip=True)
                        year_val = re.sub(r'\D', '', year_text) # Ambil hanya angka
                        
                        movie_list.append({
                            'url': full_url, 
                            'slug': slug, 
                            'year': int(year_val) if year_val else None
                        })

            print(f"    [+] Terdeteksi {len(movie_list)} item film.")

            # --- PROSES DETAIL TIAP FILM ---
            for movie in movie_list:
                slug = movie['slug']
                try:
                    print(f"        [*] Scraping Detail: {slug}")
                    driver.get(movie['url'])
                    time.sleep(8) # Tunggu iframe/opsi player muncul
                    
                    detail_soup = soupify(driver)
                    options = detail_soup.select('select#player-select option')
                    
                    found_providers = {}
                    for opt in options:
                        val = opt.get('value', '').strip()
                        server = opt.get('data-server', '').lower()
                        if val:
                            # Normalisasi URL iframe
                            clean_link = 'https:' + val if val.startswith('//') else \
                                         'https://playeriframe.sbs' + val if val.startswith('/') else val
                            found_providers[server] = clean_link

                    # --- UPSERT KE SUPABASE ---
                    if found_providers:
                        payload = {
                            "slug": slug,
                            "year": movie['year'],
                            "link_cast": found_providers.get('cast', ''),
                            "link_turbo": found_providers.get('turbovip', '')
                        }

                        # Upsert berdasarkan constraint unique 'slug'
                        supabase.table("movies").upsert(
                            payload, 
                            on_conflict="slug"
                        ).execute()
                        print(f"            [OK] Sync Supabase Berhasil.")
                    else:
                        print(f"            [SKIP] Link player tidak ditemukan.")

                except Exception as e:
                    print(f"            [ERROR] Gagal pada {slug}: {e}")

        except Exception as e:
            print(f"[!] Gagal memuat halaman {page}: {e}")
            continue

    print(f"\n[DONE] Seluruh proses selesai.")

if __name__ == "__main__":
    run_latest_scraper_supabase()
